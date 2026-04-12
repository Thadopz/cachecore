package cache

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func newBenchGroup() *Group {
	return NewGroup("bench-group", 8<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}))
}

func newBenchGroupWithOptions(opts ...Option) *Group {
	return NewGroup("bench-group-opt", 8<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), opts...)
}

func BenchmarkGroupGetHotKey(b *testing.B) {
	g := newBenchGroup()
	if _, err := g.Get("hot-key"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Get("hot-key"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGroupGetColdKey(b *testing.B) {
	g := newBenchGroup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "cold-" + strconv.Itoa(i)
		if _, err := g.Get(key); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGroupGetParallelHotKey(b *testing.B) {
	g := newBenchGroup()
	if _, err := g.Get("parallel-hot-key"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.Get("parallel-hot-key"); err != nil {
				b.Fatalf("get failed: %v", err)
			}
		}
	})
}

func BenchmarkGroupGetParallelColdKey(b *testing.B) {
	g := newBenchGroup()
	var seq uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			key := "parallel-cold-" + strconv.FormatUint(n, 10)
			if _, err := g.Get(key); err != nil {
				b.Fatalf("get failed: %v", err)
			}
		}
	})
}

func BenchmarkGroupGetStrategyComparisonParallel(b *testing.B) {
	cases := []struct {
		name    string
		new     func() *Group
		dynamic bool
	}{
		{
			name: "dynamic-auto-switch",
			new: func() *Group {
				return newBenchGroupWithOptions(
					WithSwitchCooldown(0),
					WithFallbackTTL(2*time.Second),
					WithAutoSwitchByMissRate(AutoSwitchPolicy{
						Enable:          true,
						MissRateHigh:    0.6,
						MissRateLow:     0.2,
						HighConsecutive: 2,
						LowConsecutive:  2,
					}),
				)
			},
			dynamic: true,
		},
		{
			name: "static-sharded",
			new: func() *Group {
				return newBenchGroupWithOptions(WithShardedCache(256))
			},
			dynamic: false,
		},
		{
			name: "static-unsharded",
			new: func() *Group {
				return newBenchGroupWithOptions()
			},
			dynamic: false,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			g := tc.new()
			if _, err := g.Get("hot-key"); err != nil {
				b.Fatalf("warmup get failed: %v", err)
			}

			var seq uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddUint64(&seq, 1)

					var key string
					if n%10 < 7 {
						key = "hot-key"
					} else {
						key = "cold-" + strconv.FormatUint(n%4096, 10)
					}

					if _, err := g.Get(key); err != nil {
						b.Fatalf("get failed: %v", err)
					}

					if tc.dynamic && n%512 == 0 {
						phase := (n / 512) % 8
						if phase < 4 {
							_ = g.EvaluateAutoSwitchOnce(0.8)
						} else {
							_ = g.EvaluateAutoSwitchOnce(0.1)
						}
					}
				}
			})
		})
	}
}
