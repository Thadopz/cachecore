package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	groupcache "goCache"
	"goCache/bloomfilter"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func seedRedisNumericKeys() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("[Redis] skip seed, ping failed: %v", err)
		return
	}

	pipe := rdb.Pipeline()
	for i := 0; i <= 100; i++ {
		v := strconv.Itoa(i)
		pipe.Set(ctx, "key"+v, v, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[Redis] seed failed: %v", err)
		return
	}

	log.Printf("[Redis] seeded key0~key100")
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func defaultWarmupKeys() []string {
	keys := make([]string, 0, len(db)+101)
	for key := range db {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for i := 0; i <= 100; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
	}
	return keys
}

func createGroup(strategy string, shards uint, autoPolicy groupcache.AutoSwitchPolicy, switchInterval time.Duration, fallbackTTL time.Duration, switchCooldown time.Duration, enableFilter bool, filterSize int, filterHashes int) *groupcache.Group {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Printf("[Redis] ping failed, fallback to db only: %v", err)
	}

	opts := []groupcache.Option{
		groupcache.WithOnEvicted(func(key string, value groupcache.ByteView) {
			log.Printf("[Cache] evicted key=%s", key)
		}),
	}
	if enableFilter {
		opts = append(opts, groupcache.WithFilter(bloomfilter.New(filterSize, filterHashes)))
	}

	if fallbackTTL >= 0 {
		opts = append(opts, groupcache.WithFallbackTTL(fallbackTTL))
	}
	if switchCooldown >= 0 {
		opts = append(opts, groupcache.WithSwitchCooldown(switchCooldown))
	}

	switch strings.ToLower(strategy) {
	case "sharded":
		opts = append(opts, groupcache.WithShardedCache(uint32(shards)))
	case "dynamic":
		opts = append(opts, groupcache.WithAutoSwitchByMissRate(autoPolicy))
	default:
		// unsharded is default; no extra option required.
	}

	g := groupcache.NewGroup("scores", 2<<10, groupcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			redisCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			if value, err := rdb.Get(redisCtx, key).Result(); err == nil {
				return []byte(value), nil
			} else if !errors.Is(err, redis.Nil) {
				log.Printf("[Redis] GET failed for key=%s: %v", key, err)
			}

			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if err := rdb.Set(redisCtx, key, v, 0).Err(); err != nil {
					log.Printf("[Redis] SET failed for key=%s: %v", key, err)
				}
				return []byte(v), nil
			}
			return nil, groupcache.ErrNotFound
		}),
		opts...,
	)

	if strings.EqualFold(strategy, "dynamic") {
		g.StartAutoSwitchController(switchInterval, func() float64 {
			s := groupcache.Stats.Snapshot()
			if s.GroupGets == 0 {
				return 0
			}
			return float64(s.CacheMisses) / float64(s.GroupGets)
		})
	}

	return g
}

func startCacheServer(addr string, addrs []string, gcache *groupcache.Group) {
	peers := groupcache.NewHTTPPool(addr)
	peers.Set(addrs...)
	gcache.RegisterPeers(peers)
	log.Println("cache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}

func newAPIHandler(gcache *groupcache.Group) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			groupcache.Stats.RecordAPILatency(time.Since(start))
		}()

		groupcache.Stats.IncAPIRequests()
		key := r.URL.Query().Get("key")
		view, err := gcache.Get(r.Context(), key)
		if err != nil {
			if errors.Is(err, groupcache.ErrNotFound) || errors.Is(err, groupcache.ErrFilterNotFound) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			groupcache.Stats.IncAPIErrors()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(view.ByteSlice())
	})
}

func startAPIServer(apiAddr string, gcache *groupcache.Group) {
	http.Handle("/api", newAPIHandler(gcache))
	http.Handle("/debug/stats", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			snapshot := groupcache.Stats.Snapshot()

			hitRate := 0.0
			if snapshot.GroupGets > 0 {
				hitRate = float64(snapshot.CacheHits) / float64(snapshot.GroupGets) * 100
			}

			errorRate := 0.0
			if snapshot.APIRequests > 0 {
				errorRate = float64(snapshot.APIErrors) / float64(snapshot.APIRequests) * 100
			}

			payload := map[string]any{
				"timestamp": time.Now().Format(time.RFC3339),
				"totals": map[string]uint64{
					"api_requests":       snapshot.APIRequests,
					"api_errors":         snapshot.APIErrors,
					"group_gets":         snapshot.GroupGets,
					"cache_hits":         snapshot.CacheHits,
					"cache_misses":       snapshot.CacheMisses,
					"peer_loads":         snapshot.PeerLoads,
					"local_loads":        snapshot.LocalLoads,
					"peer_http_requests": snapshot.PeerHTTPRequests,
					"filter_misses":      snapshot.FilterMisses,
				},
				"rates": map[string]float64{
					"hit_rate_percent":   hitRate,
					"error_rate_percent": errorRate,
					"api_avg_ms":         float64(snapshot.APILatencyTotalMicros) / float64(max(snapshot.APILatencyCount, 1)) / 1000.0,
					"api_p95_ms":         float64(snapshot.APILastP95Micros) / 1000.0,
					"api_p99_ms":         float64(snapshot.APILastP99Micros) / 1000.0,
					"group_get_avg_ms":   float64(snapshot.GroupGetTotalMicros) / float64(max(snapshot.GroupGetLatencyCount, 1)) / 1000.0,
					"group_get_p95_ms":   float64(snapshot.GroupGetLastP95Micros) / 1000.0,
					"group_get_p99_ms":   float64(snapshot.GroupGetLastP99Micros) / 1000.0,
					"cache_get_avg_ms":   float64(snapshot.CacheGetTotalMicros) / float64(max(snapshot.CacheGetLatencyCount, 1)) / 1000.0,
					"cache_get_p95_ms":   float64(snapshot.CacheGetLastP95Micros) / 1000.0,
					"cache_get_p99_ms":   float64(snapshot.CacheGetLastP99Micros) / 1000.0,
				},
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(payload); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}))
	log.Println("frontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))

}

func main() {
	var port int
	var api bool
	var strategy string
	var shards uint
	var switchInterval time.Duration
	var missHigh float64
	var missLow float64
	var highConsecutive int
	var lowConsecutive int
	var fallbackTTL time.Duration
	var switchCooldown time.Duration
	var mutexProfileFraction int
	var blockProfileRate int
	var enableFilter bool
	var filterSize int
	var filterHashes int
	var warmupKeysCSV string
	flag.IntVar(&port, "port", 8001, "Cache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.StringVar(&strategy, "strategy", "unsharded", "Cache strategy: unsharded | sharded | dynamic")
	flag.UintVar(&shards, "shards", 256, "Shard count when using sharded or dynamic strategy")
	flag.DurationVar(&switchInterval, "switch-interval", 2*time.Second, "Auto switch evaluation interval in dynamic mode")
	flag.Float64Var(&missHigh, "miss-high", 0.8, "High miss-rate threshold to switch to sharded in dynamic mode")
	flag.Float64Var(&missLow, "miss-low", 0.55, "Low miss-rate threshold to switch to unsharded in dynamic mode")
	flag.IntVar(&highConsecutive, "high-consecutive", 6, "Consecutive high miss-rate windows needed before switching to sharded")
	flag.IntVar(&lowConsecutive, "low-consecutive", 8, "Consecutive low miss-rate windows needed before switching to unsharded")
	flag.DurationVar(&fallbackTTL, "fallback-ttl", 90*time.Second, "Fallback cache TTL after mode switch")
	flag.DurationVar(&switchCooldown, "switch-cooldown", 45*time.Second, "Minimum interval between two mode switches")
	flag.IntVar(&mutexProfileFraction, "mutex-profile-fraction", 0, "runtime.SetMutexProfileFraction value; >0 enables mutex contention sampling")
	flag.IntVar(&blockProfileRate, "block-profile-rate", 0, "runtime.SetBlockProfileRate value; >0 enables blocking event sampling")
	flag.BoolVar(&enableFilter, "filter", false, "Enable bloom filter pre-check before calling the getter")
	flag.IntVar(&filterSize, "filter-size", 1000, "Bloom filter bitset size when -filter is enabled")
	flag.IntVar(&filterHashes, "filter-hashes", 6, "Bloom filter hash count when -filter is enabled")
	flag.StringVar(&warmupKeysCSV, "warmup-keys", "", "Comma-separated keys used to warm up the filter when -filter is enabled")
	flag.Parse()

	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}
	if blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
	}

	apiAddr := "http://0.0.0.0:9999"
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	seedRedisNumericKeys()

	autoPolicy := groupcache.AutoSwitchPolicy{
		Enable:          strings.EqualFold(strategy, "dynamic"),
		MissRateHigh:    missHigh,
		MissRateLow:     missLow,
		HighConsecutive: highConsecutive,
		LowConsecutive:  lowConsecutive,
	}
	requestGroup := createGroup(strategy, shards, autoPolicy, switchInterval, fallbackTTL, switchCooldown, enableFilter, filterSize, filterHashes)
	warmupKeys := splitCSV(warmupKeysCSV)
	if enableFilter && len(warmupKeys) == 0 {
		warmupKeys = defaultWarmupKeys()
	}
	log.Printf("[Config] strategy=%s shards=%d dynamic=%v filter=%v filter_size=%d filter_hashes=%d warmup_keys=%d miss_high=%.3f miss_low=%.3f interval=%s mutex_profile_fraction=%d block_profile_rate=%d", strategy, shards, autoPolicy.Enable, enableFilter, filterSize, filterHashes, len(warmupKeys), missHigh, missLow, switchInterval, mutexProfileFraction, blockProfileRate)
	if enableFilter {
		requestGroup.Warmup(warmupKeys)
	}
	if api {
		go startAPIServer(apiAddr, requestGroup)
	}
	go groupcache.Stats.StartLogger(time.Second * 5)
	startCacheServer(addrMap[port], []string(addrs), requestGroup)
}
