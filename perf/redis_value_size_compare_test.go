//go:build redis

package perf

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	groupcache "github.com/Thadopz/cachecore"

	"github.com/redis/go-redis/v9"
)

type compareResult struct {
	valueBytes   int
	requests     uint64
	qps          float64
	hitRate      float64
	cacheHits    uint64
	cacheMisses  uint64
	groupGets    uint64
	redisSeeded  int
	cacheBytes   int64
	workingSet   int
	concurrency  int
	duration     time.Duration
	dist         string
	redisAddress string
}

func TestRedisValueSizeComparison(t *testing.T) {
	t.Helper()

	const (
		redisAddr   = "127.0.0.1:6379"
		cacheBytes  = 32 << 20
		workingSet  = 1000
		concurrency = 64
		duration    = 8 * time.Second
	)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed at %s: %v", redisAddr, err)
	}

	sizes := []int{1 << 10, 100 << 10}
	results := make([]compareResult, 0, len(sizes))
	for _, size := range sizes {
		result := runValueSizeScenario(t, rdb, redisAddr, cacheBytes, workingSet, concurrency, duration, size)
		results = append(results, result)
		t.Logf(
			"value=%dKB qps=%.2f hit_rate=%.2f%% hits=%d misses=%d gets=%d cache=%dMB working_set=%d concurrency=%d duration=%s dist=%s",
			result.valueBytes>>10,
			result.qps,
			result.hitRate,
			result.cacheHits,
			result.cacheMisses,
			result.groupGets,
			result.cacheBytes>>20,
			result.workingSet,
			result.concurrency,
			result.duration,
			result.dist,
		)
	}

	if len(results) != 2 {
		t.Fatalf("unexpected result count: %d", len(results))
	}

	base := results[0]
	large := results[1]
	qpsDrop := 0.0
	if base.qps > 0 {
		qpsDrop = (1 - large.qps/base.qps) * 100
	}
	hitRateDrop := base.hitRate - large.hitRate
	t.Logf(
		"summary redis=%s cache=%dMB value_1kb_qps=%.2f value_100kb_qps=%.2f qps_drop=%.2f%% hit_rate_drop=%.2fpp",
		redisAddr,
		cacheBytes>>20,
		base.qps,
		large.qps,
		qpsDrop,
		hitRateDrop,
	)
}

func TestRedisCacheBytesValueSizeMatrix(t *testing.T) {
	t.Helper()

	const (
		redisAddr   = "127.0.0.1:6379"
		workingSet  = 768
		concurrency = 48
		duration    = 4 * time.Second
	)

	cacheSizes := []int64{
		128 << 20,
		512 << 20,
		1 << 30,
	}
	valueSizes := []int{
		1 << 10,
		1 << 20,
		2 << 20,
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed at %s: %v", redisAddr, err)
	}

	t.Logf("matrix redis=%s working_set=%d concurrency=%d duration=%s dist=%s", redisAddr, workingSet, concurrency, duration, "zipf(s=1.15,v=3)")
	for _, valueBytes := range valueSizes {
		prefix := fmt.Sprintf("bench:matrix:sz:%d:", valueBytes)
		cleanupRedisPrefix(t, rdb, prefix)
		seedRedisValues(t, rdb, prefix, workingSet, valueBytes)

		for _, cacheBytes := range cacheSizes {
			result := runValueSizeScenarioWithPrefix(t, rdb, redisAddr, cacheBytes, workingSet, concurrency, duration, valueBytes, prefix)
			t.Logf(
				"cache=%dMB value=%s qps=%.2f hit_rate=%.2f%% hits=%d misses=%d gets=%d",
				result.cacheBytes>>20,
				formatBytesIEC(int64(result.valueBytes)),
				result.qps,
				result.hitRate,
				result.cacheHits,
				result.cacheMisses,
				result.groupGets,
			)
		}

		cleanupRedisPrefix(t, rdb, prefix)
	}
}

func runValueSizeScenario(t *testing.T, rdb *redis.Client, redisAddr string, cacheBytes int64, workingSet int, concurrency int, duration time.Duration, valueBytes int) compareResult {
	t.Helper()

	prefix := fmt.Sprintf("bench:sz:%d:", valueBytes)
	seedRedisValues(t, rdb, prefix, workingSet, valueBytes)
	return runValueSizeScenarioWithPrefix(t, rdb, redisAddr, cacheBytes, workingSet, concurrency, duration, valueBytes, prefix)
}

func runValueSizeScenarioWithPrefix(t *testing.T, rdb *redis.Client, redisAddr string, cacheBytes int64, workingSet int, concurrency int, duration time.Duration, valueBytes int, prefix string) compareResult {
	t.Helper()

	groupcache.Stats = &groupcache.Metrics{}
	g := groupcache.NewGroup(
		fmt.Sprintf("value-size-%d", valueBytes),
		cacheBytes,
		groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			if ctx == nil {
				ctx = context.Background()
			}

			value, err := rdb.Get(ctx, key).Bytes()
			if err == nil {
				return value, nil
			}
			if errors.Is(err, redis.Nil) {
				return nil, groupcache.ErrNotFound
			}
			return nil, err
		}),
	)

	keys := make([]string, 0, workingSet)
	for i := 0; i < workingSet; i++ {
		keys = append(keys, prefix+strconv.Itoa(i))
	}

	// Preload once so the run reflects steady-state cache behavior rather than cold-start misses.
	for _, key := range keys {
		if _, err := g.Get(context.Background(), key); err != nil {
			t.Fatalf("warmup get failed for key %s: %v", key, err)
		}
	}

	groupcache.Stats = &groupcache.Metrics{}

	var requests uint64
	var wg sync.WaitGroup
	stop := time.Now().Add(duration)

	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + id)))
			zipf := rand.NewZipf(rng, 1.15, 3, uint64(workingSet-1))
			for time.Now().Before(stop) {
				key := keys[int(zipf.Uint64())]
				if _, err := g.Get(context.Background(), key); err != nil {
					t.Errorf("get failed for key %s: %v", key, err)
					return
				}
				atomic.AddUint64(&requests, 1)
			}
		}(workerID)
	}
	wg.Wait()

	snapshot := groupcache.Stats.Snapshot()
	hitRate := 0.0
	if snapshot.GroupGets > 0 {
		hitRate = float64(snapshot.CacheHits) / float64(snapshot.GroupGets) * 100
	}

	totalRequests := atomic.LoadUint64(&requests)
	return compareResult{
		valueBytes:   valueBytes,
		requests:     totalRequests,
		qps:          float64(totalRequests) / duration.Seconds(),
		hitRate:      hitRate,
		cacheHits:    snapshot.CacheHits,
		cacheMisses:  snapshot.CacheMisses,
		groupGets:    snapshot.GroupGets,
		redisSeeded:  workingSet,
		cacheBytes:   cacheBytes,
		workingSet:   workingSet,
		concurrency:  concurrency,
		duration:     duration,
		dist:         "zipf(s=1.15,v=3)",
		redisAddress: redisAddr,
	}
}

func seedRedisValues(t *testing.T, rdb *redis.Client, prefix string, count int, valueBytes int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	payload := buildValue(prefix, valueBytes)
	pipe := rdb.Pipeline()
	for i := 0; i < count; i++ {
		key := prefix + strconv.Itoa(i)
		pipe.Set(ctx, key, payload, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed redis failed for prefix %s: %v", prefix, err)
	}
}

func cleanupRedisPrefix(t *testing.T, rdb *redis.Client, prefix string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 256).Result()
		if err != nil {
			t.Fatalf("scan redis prefix %s failed: %v", prefix, err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("delete redis keys for prefix %s failed: %v", prefix, err)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}

func buildValue(prefix string, n int) string {
	if n <= len(prefix) {
		return prefix[:n]
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(prefix)
	pad := "0123456789abcdefghijklmnopqrstuvwxyz"
	for b.Len() < n {
		remaining := n - b.Len()
		if remaining >= len(pad) {
			b.WriteString(pad)
			continue
		}
		b.WriteString(pad[:remaining])
	}
	return b.String()
}

func formatBytesIEC(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%dGB", n>>30)
	case n >= 1<<20:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
