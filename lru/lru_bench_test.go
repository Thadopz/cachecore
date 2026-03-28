package lru

import (
	"strconv"
	"testing"
)

type benchValue string

func (v benchValue) Len() int {
	return len(v)
}

func BenchmarkCacheAdd(b *testing.B) {
	c := New(0, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i)
		c.Add(key, benchValue("value"))
	}
}

func BenchmarkCacheGetHit(b *testing.B) {
	c := New(0, nil)
	for i := 0; i < 10000; i++ {
		key := "key-" + strconv.Itoa(i)
		c.Add(key, benchValue("value"))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i%10000)
		if _, ok := c.Get(key); !ok {
			b.Fatalf("expected cache hit for key %s", key)
		}
	}
}

func BenchmarkCacheGetMiss(b *testing.B) {
	c := New(0, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "missing-" + strconv.Itoa(i)
		if _, ok := c.Get(key); ok {
			b.Fatalf("expected cache miss for key %s", key)
		}
	}
}

func BenchmarkCacheMixedReadWriteParallel(b *testing.B) {
	c := New(0, nil)

	for i := 0; i < 10000; i++ {
		key := "seed-" + strconv.Itoa(i)
		c.Add(key, benchValue("value"))
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				key := "write-" + strconv.Itoa(i)
				c.Add(key, benchValue("value"))
			} else {
				key := "seed-" + strconv.Itoa(i%10000)
				_, _ = c.Get(key)
			}
			i++
		}
	})
}
