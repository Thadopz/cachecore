package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	groupcache "github.com/Thadopz/cachecore"
)

func disableLatencySamplingForMainBench(b *testing.B) {
	groupcache.Stats.SetLatencySamplingEnabled(false)
	b.Cleanup(func() {
		groupcache.Stats.SetLatencySamplingEnabled(true)
	})
}

func newStrategyBenchGroup(strategy string) *groupcache.Group {
	name := fmt.Sprintf("bench-%s-%d", strategy, time.Now().UnixNano())
	opts := []groupcache.Option{}

	switch strategy {
	case "sharded":
		opts = append(opts, groupcache.WithShardedCache(256))
	}

	return groupcache.NewGroup(name, 8<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), opts...)
}

func BenchmarkStrategyComparisonNoPopup(b *testing.B) {
	disableLatencySamplingForMainBench(b)

	cases := []struct {
		name     string
		strategy string
	}{
		{name: "static-sharded", strategy: "sharded"},
		{name: "static-unsharded", strategy: "unsharded"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			g := newStrategyBenchGroup(tc.strategy)
			if _, err := g.Get(context.Background(), "hot-key"); err != nil {
				b.Fatalf("warmup get failed: %v", err)
			}

			var seq uint64
			var phase uint32
			stopPhase := make(chan struct{})
			go func() {
				phaseInterval := 40 * time.Millisecond
				ticker := time.NewTicker(phaseInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						atomic.AddUint32(&phase, 1)
					case <-stopPhase:
						return
					}
				}
			}()
			b.Cleanup(func() {
				close(stopPhase)
			})

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddUint64(&seq, 1)
					curPhase := atomic.LoadUint32(&phase) % 4
					warmColdModulo := uint64(10)
					coldBurstPercent := uint64(8)

					key := "hot-key"
					switch curPhase {
					case 0, 1:
						if n%warmColdModulo == 0 {
							key = "warm-cold-" + strconv.FormatUint(n%64, 10)
						}
					default:
						if n%10 < coldBurstPercent {
							key = "cold-" + strconv.FormatUint(n, 10)
						}
					}

					if _, err := g.Get(context.Background(), key); err != nil {
						b.Fatalf("get failed: %v", err)
					}
				}
			})
		})
	}
}
