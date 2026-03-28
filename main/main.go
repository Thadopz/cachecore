package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	groupcache "goCache"
	"goCache/bloomfilter"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
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

func createGroup() *groupcache.Group {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Printf("[Redis] ping failed, fallback to db only: %v", err)
	}

	return groupcache.NewGroup("scores", 2<<10, groupcache.GetterFunc(
		func(key string) ([]byte, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			if value, err := rdb.Get(ctx, key).Result(); err == nil {
				return []byte(value), nil
			} else if !errors.Is(err, redis.Nil) {
				log.Printf("[Redis] GET failed for key=%s: %v", key, err)
			}

			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if err := rdb.Set(ctx, key, v, 0).Err(); err != nil {
					log.Printf("[Redis] SET failed for key=%s: %v", key, err)
				}
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}),
		groupcache.WithFilter(bloomfilter.New(1000, 6)),
		//groupcache.WithJanitor(time.Minute),
		groupcache.WithOnEvicted(func(key string, value groupcache.ByteView) {
			log.Printf("[Cache] evicted key=%s", key)
		}),
		//groupcache.WithShardedCache(10),
	)
}

func startCacheServer(addr string, addrs []string, gcache *groupcache.Group) {
	peers := groupcache.NewHTTPPool(addr)
	peers.Set(addrs...)
	gcache.RegisterPeers(peers)
	log.Println("cache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}

func startAPIServer(apiAddr string, gcache *groupcache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := gcache.Get(key)
			groupcache.Stats.IncAPIRequests()
			if err != nil {
				groupcache.Stats.IncAPIErrors()
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())

		}))
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
	flag.IntVar(&port, "port", 8001, "Cache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

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

	requestGroup := createGroup()
	requestGroup.Warmup([]string{"Tom"})
	if api {
		go startAPIServer(apiAddr, requestGroup)
	}
	go groupcache.Stats.StartLogger(time.Second * 5)
	startCacheServer(addrMap[port], []string(addrs), requestGroup)
}
