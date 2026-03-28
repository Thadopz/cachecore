package cache

import (
	"strconv"
	"sync/atomic"
	"testing"
)

func newBenchGroup() *Group {
	return NewGroup("bench-group", 8<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}))
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
