package bloomfilter

import (
	"strconv"
	"testing"
)

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := New(1<<20, 6)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add("key-" + strconv.Itoa(i))
	}
}

func BenchmarkBloomFilterContainsHit(b *testing.B) {
	bf := New(1<<20, 6)
	for i := 0; i < 100000; i++ {
		bf.Add("key-" + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !bf.Contains("key-" + strconv.Itoa(i%100000)) {
			b.Fatalf("expected contains hit")
		}
	}
}

func BenchmarkBloomFilterContainsMiss(b *testing.B) {
	bf := New(1<<20, 6)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.Contains("missing-" + strconv.Itoa(i))
	}
}
