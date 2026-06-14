package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	groupcache "github.com/Thadopz/cachecore"
	"github.com/Thadopz/cachecore/bloomfilter"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
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

func seedRedisNumericKeys(redisAddr string) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
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

func syntheticValue(key string) ([]byte, bool) {
	if v, ok := db[key]; ok {
		return []byte(v), true
	}
	if strings.HasPrefix(key, "key") && len(key) > len("key") {
		raw := strings.TrimPrefix(key, "key")
		if _, err := strconv.Atoi(raw); err == nil {
			return []byte(raw), true
		}
	}
	return nil, false
}

func createGroup(strategy string, shards uint, enableFilter bool, filterSize int, filterHashes int, filterRefresh time.Duration, redisAddr string, backend string, cacheBytes int64, evictionLog bool) *groupcache.Group {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		backend = "demo"
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if backend != "synthetic" {
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Printf("[Redis] ping failed, fallback to db only: %v", err)
		}
	}

	opts := []groupcache.Option{}
	if evictionLog {
		opts = append(opts, groupcache.WithOnEvicted(func(key string, value groupcache.ByteView) {
			log.Printf("[Cache] evicted key=%s", key)
		}))
	}
	if backend != "synthetic" {
		opts = append(opts, groupcache.WithIncrementer(groupcache.IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			redisCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			return rdb.IncrBy(redisCtx, key, delta).Result()
		})))
	}
	if enableFilter {
		opts = append(opts, groupcache.WithFilter(bloomfilter.New(filterSize, filterHashes)))
		if filterRefresh > 0 {
			opts = append(opts, groupcache.WithFilterRefresh(filterRefresh))
		}
	}

	switch strings.ToLower(strategy) {
	case "sharded":
		opts = append(opts, groupcache.WithShardedCache(uint32(shards)))
	case "unsharded":
		// unsharded requires no extra option.
	case "slru":
		opts = append(opts, groupcache.WithSLRU(0.8))
	case "sharded-slru":
		opts = append(opts, groupcache.WithShardedCache(uint32(shards)), groupcache.WithSLRU(0.8))
	default:
		log.Fatalf("invalid cache strategy %q: use sharded, unsharded, slru, or sharded-slru", strategy)
	}

	g := groupcache.NewGroup("scores", cacheBytes, groupcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			if backend == "synthetic" {
				if value, ok := syntheticValue(key); ok {
					return value, nil
				}
				return nil, groupcache.ErrNotFound
			}
			if backend != "demo" {
				return nil, groupcache.ErrNotFound
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

	return g
}

func startCacheServer(addr string, addrs []string, gcache *groupcache.Group) {
	peers := groupcache.NewHTTPPool(addr)
	peers.Set(addrs...)
	gcache.RegisterPeers(peers)
	log.Println("cache is running at", addr)
	log.Fatal(http.ListenAndServe(listenAddrFromHTTPAddr(addr), peers))
}

func normalizeHTTPAddr(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return "http://" + strings.TrimPrefix(strings.TrimPrefix(raw, "http://"), "https://")
	}
	return "http://" + raw
}

func parsePeerAddrs(csv string) []string {
	parts := splitCSV(csv)
	if len(parts) == 0 {
		return nil
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		normalized := normalizeHTTPAddr(part)
		if normalized != "" {
			out = append(out, normalized)
		}
	}
	return out
}

func listenAddrFromHTTPAddr(httpAddr string) string {
	u, err := url.Parse(httpAddr)
	if err != nil || u.Host == "" {
		return httpAddr
	}
	return u.Host
}

func newAPIHandler(gcache *groupcache.Group) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			groupcache.Stats.RecordAPILatency(time.Since(start))
		}()

		groupcache.Stats.IncAPIRequests()
		if r.URL.Path == "/api/increment" {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			key := r.URL.Query().Get("key")
			if key == "" {
				http.Error(w, "key is required", http.StatusBadRequest)
				return
			}
			delta := int64(1)
			if raw := r.URL.Query().Get("delta"); raw != "" {
				n, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					http.Error(w, "delta must be an integer", http.StatusBadRequest)
					return
				}
				delta = n
			}
			next, err := gcache.Increment(r.Context(), key, delta)
			if err != nil {
				if errors.Is(err, groupcache.ErrNonNumericValue) {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				groupcache.Stats.IncAPIErrors()
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"key": key, "value": next}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

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
	apiHandler := newAPIHandler(gcache)
	http.Handle("/ping", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	http.Handle("/api", apiHandler)
	http.Handle("/api/increment", apiHandler)
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
	log.Fatal(http.ListenAndServe(listenAddrFromHTTPAddr(apiAddr), nil))

}

func main() {
	var port int
	var api bool
	var strategy string
	var shards uint
	var mutexProfileFraction int
	var blockProfileRate int
	var latencySampleRate float64
	var enableFilter bool
	var filterSize int
	var filterHashes int
	var filterRefresh time.Duration
	var warmupKeysCSV string
	var selfAddr string
	var peersCSV string
	var redisAddr string
	var apiAddr string
	var backend string
	var cacheBytes int64
	var evictionLog bool
	flag.IntVar(&port, "port", 8001, "Cache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.StringVar(&apiAddr, "api-addr", "0.0.0.0:9999", "API server listen address")
	flag.StringVar(&selfAddr, "self-addr", "", "Current node HTTP address for peer routing, e.g. http://cache-0.cache:8001")
	flag.StringVar(&peersCSV, "peers", "", "Comma-separated peer HTTP addresses; defaults to local demo addresses when empty")
	flag.StringVar(&redisAddr, "redis-addr", "127.0.0.1:6379", "Redis address host:port")
	flag.StringVar(&strategy, "strategy", "sharded", "Cache strategy: sharded | unsharded | slru | sharded-slru")
	flag.UintVar(&shards, "shards", 256, "Shard count when using sharded strategy")
	flag.Int64Var(&cacheBytes, "cache-bytes", 2<<10, "Cache capacity in bytes")
	flag.StringVar(&backend, "backend", "demo", "Backend mode: demo | synthetic")
	flag.BoolVar(&evictionLog, "eviction-log", true, "Log cache eviction callbacks")
	flag.IntVar(&mutexProfileFraction, "mutex-profile-fraction", 0, "runtime.SetMutexProfileFraction value; >0 enables mutex contention sampling")
	flag.IntVar(&blockProfileRate, "block-profile-rate", 0, "runtime.SetBlockProfileRate value; >0 enables blocking event sampling")
	flag.Float64Var(&latencySampleRate, "latency-sample-rate", 1.0, "Latency percentile sampling rate: 1=all, 0.01=1%, 0=disable percentile sampling")
	flag.BoolVar(&enableFilter, "filter", false, "Enable bloom filter pre-check before calling the getter")
	flag.IntVar(&filterSize, "filter-size", 1000, "Bloom filter bitset size when -filter is enabled")
	flag.IntVar(&filterHashes, "filter-hashes", 6, "Bloom filter hash count when -filter is enabled")
	flag.DurationVar(&filterRefresh, "filter-refresh", 5*time.Minute, "Bloom filter refresh interval from warmup keys when -filter is enabled; <=0 disables refresh")
	flag.StringVar(&warmupKeysCSV, "warmup-keys", "", "Comma-separated keys used to warm up the filter when -filter is enabled")
	flag.Parse()

	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}
	if blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
	}
	groupcache.Stats.SetLatencySampleRate(latencySampleRate)

	if selfAddr == "" {
		selfAddr = "http://localhost:" + strconv.Itoa(port)
	} else {
		selfAddr = normalizeHTTPAddr(selfAddr)
	}

	if apiAddr == "" {
		apiAddr = "0.0.0.0:9999"
	}

	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	addrs := parsePeerAddrs(peersCSV)
	if len(addrs) == 0 {
		for _, v := range addrMap {
			addrs = append(addrs, v)
		}
	}

	if !slices.Contains(addrs, selfAddr) {
		addrs = append(addrs, selfAddr)
	}

	if strings.ToLower(strings.TrimSpace(backend)) != "synthetic" {
		seedRedisNumericKeys(redisAddr)
	}

	requestGroup := createGroup(strategy, shards, enableFilter, filterSize, filterHashes, filterRefresh, redisAddr, backend, cacheBytes, evictionLog)
	warmupKeys := splitCSV(warmupKeysCSV)
	if enableFilter && len(warmupKeys) == 0 {
		warmupKeys = defaultWarmupKeys()
	}
	log.Printf("[Config] strategy=%s shards=%d backend=%s cache_bytes=%d eviction_log=%v filter=%v filter_size=%d filter_hashes=%d filter_refresh=%s warmup_keys=%d mutex_profile_fraction=%d block_profile_rate=%d latency_sample_rate=%.4f", strategy, shards, backend, cacheBytes, evictionLog, enableFilter, filterSize, filterHashes, filterRefresh, len(warmupKeys), mutexProfileFraction, blockProfileRate, latencySampleRate)
	if enableFilter {
		requestGroup.Warmup(warmupKeys)
	}
	if api {
		go startAPIServer("http://"+apiAddr, requestGroup)
	}
	go groupcache.Stats.StartLogger(time.Second * 5)
	startCacheServer(selfAddr, addrs, requestGroup)
}
