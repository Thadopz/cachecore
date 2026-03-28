package singleflight

import (
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkGroupDoUniqueKey(b *testing.B) {
	var g Group

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i)
		_, err := g.Do(key, func() (interface{}, error) {
			return 1, nil
		})
		if err != nil {
			b.Fatalf("Do failed: %v", err)
		}
	}
}

func BenchmarkGroupDoParallelSameKey(b *testing.B) {
	var g Group
	var calls uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := g.Do("same-key", func() (interface{}, error) {
				atomic.AddUint64(&calls, 1)
				return 1, nil
			})
			if err != nil {
				b.Fatalf("Do failed: %v", err)
			}
		}
	})
	_ = calls
}
