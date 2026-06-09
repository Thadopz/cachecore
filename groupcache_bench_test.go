package cache

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

func disableLatencySamplingForBench(b *testing.B) {
	Stats.SetLatencySamplingEnabled(false)
	b.Cleanup(func() {
		Stats.SetLatencySamplingEnabled(true)
	})
}

func newBenchGroup() *Group {
	return NewGroup("bench-group", 8<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}))
}

func newBenchGroupWithOptions(opts ...Option) *Group {
	return NewGroup("bench-group-opt", 8<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), opts...)
}

func BenchmarkGroupGetHotKey(b *testing.B) {
	disableLatencySamplingForBench(b)
	g := newBenchGroup()
	if _, err := g.Get(context.Background(), "hot-key"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Get(context.Background(), "hot-key"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGroupGetColdKey(b *testing.B) {
	disableLatencySamplingForBench(b)
	g := newBenchGroup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "cold-" + strconv.Itoa(i)
		if _, err := g.Get(context.Background(), key); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGroupGetParallelHotKey(b *testing.B) {
	disableLatencySamplingForBench(b)
	g := newBenchGroup()
	if _, err := g.Get(context.Background(), "parallel-hot-key"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.Get(context.Background(), "parallel-hot-key"); err != nil {
				b.Fatalf("get failed: %v", err)
			}
		}
	})
}

func BenchmarkGroupGetParallelColdKey(b *testing.B) {
	disableLatencySamplingForBench(b)
	g := newBenchGroup()
	var seq uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			key := "parallel-cold-" + strconv.FormatUint(n, 10)
			if _, err := g.Get(context.Background(), key); err != nil {
				b.Fatalf("get failed: %v", err)
			}
		}
	})
}

func BenchmarkGroupGetStrategyComparisonParallel(b *testing.B) {
	disableLatencySamplingForBench(b)
	cases := []struct {
		name string
		new  func() *Group
	}{
		{
			name: "static-sharded",
			new: func() *Group {
				return newBenchGroupWithOptions(WithShardedCache(256))
			},
		},
		{
			name: "static-unsharded",
			new: func() *Group {
				return newBenchGroupWithOptions()
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			g := tc.new()
			if _, err := g.Get(context.Background(), "hot-key"); err != nil {
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

					if _, err := g.Get(context.Background(), key); err != nil {
						b.Fatalf("get failed: %v", err)
					}

				}
			})
		})
	}
}

func BenchmarkStaticModeByWorkload(b *testing.B) {
	disableLatencySamplingForBench(b)

	type workloadCase struct {
		name   string
		warmup func(g *Group) error
		keyFor func(n uint64) string
	}

	workloads := []workloadCase{
		{
			name: "parallel-hot-hit",
			warmup: func(g *Group) error {
				_, err := g.Get(context.Background(), "hot-key")
				return err
			},
			keyFor: func(_ uint64) string {
				return "hot-key"
			},
		},
		{
			name: "parallel-warmset-64",
			warmup: func(g *Group) error {
				for i := 0; i < 64; i++ {
					if _, err := g.Get(context.Background(), "warm-"+strconv.Itoa(i)); err != nil {
						return err
					}
				}
				return nil
			},
			keyFor: func(n uint64) string {
				return "warm-" + strconv.FormatUint(n%64, 10)
			},
		},
		{
			name: "parallel-mixed-churn",
			warmup: func(g *Group) error {
				if _, err := g.Get(context.Background(), "hot-key"); err != nil {
					return err
				}
				for i := 0; i < 256; i++ {
					if _, err := g.Get(context.Background(), "warm-mixed-"+strconv.Itoa(i)); err != nil {
						return err
					}
				}
				return nil
			},
			keyFor: func(n uint64) string {
				if n%10 < 7 {
					return "hot-key"
				}
				return "warm-mixed-" + strconv.FormatUint(n%4096, 10)
			},
		},
		{
			name:   "parallel-cold-unique",
			warmup: func(_ *Group) error { return nil },
			keyFor: func(n uint64) string {
				return "cold-unique-" + strconv.FormatUint(n, 10)
			},
		},
	}

	modes := []struct {
		name string
		new  func() *Group
	}{
		{
			name: "static-sharded",
			new: func() *Group {
				return newBenchGroupWithOptions(WithShardedCache(256))
			},
		},
		{
			name: "static-unsharded",
			new: func() *Group {
				return newBenchGroupWithOptions()
			},
		},
	}

	for _, wl := range workloads {
		b.Run(wl.name, func(b *testing.B) {
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					g := mode.new()
					if wl.warmup != nil {
						if err := wl.warmup(g); err != nil {
							b.Fatalf("warmup failed: %v", err)
						}
					}

					var seq uint64
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							n := atomic.AddUint64(&seq, 1)
							if _, err := g.Get(context.Background(), wl.keyFor(n)); err != nil {
								b.Fatalf("get failed: %v", err)
							}
						}
					})
				})
			}
		})
	}
}
