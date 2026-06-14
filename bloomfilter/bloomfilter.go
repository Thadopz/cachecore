package bloomfilter

import (
	"hash/crc32"
	"hash/fnv"
	"sync"
)

const (
	defaultBitsetSize = 1000
	defaultHashCount  = 6
)

// BloomFilter is a thread-safe Bloom filter.
type BloomFilter struct {
	arr []uint32
	k   uint32
	mu  sync.RWMutex
}

// New creates a Bloom filter with m bits and k hash functions.
func New(m, k int) *BloomFilter {
	if m <= 0 {
		m = defaultBitsetSize
	}
	if k <= 0 {
		k = defaultHashCount
	}
	return &BloomFilter{
		arr: make([]uint32, m),
		k:   uint32(k),
	}
}

// NewDefault creates a Bloom filter with default sizing.
func NewDefault() *BloomFilter {
	return New(defaultBitsetSize, defaultHashCount)
}

// Add inserts item into the filter.
func (bf *BloomFilter) Add(item string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if len(bf.arr) == 0 {
		return
	}
	for _, idx := range bf.indexes(item) {
		bf.arr[idx] = 1
	}
}

// Contains reports whether item may exist in the filter.
func (bf *BloomFilter) Contains(item string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	if len(bf.arr) == 0 {
		return false
	}
	for _, idx := range bf.indexes(item) {
		if bf.arr[idx] == 0 {
			return false
		}
	}
	return true
}

// Reset clears all filter bits.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.arr {
		bf.arr[i] = 0
	}
}

func (bf *BloomFilter) indexes(item string) []int {
	m := uint32(len(bf.arr))
	h1 := hash1(item)
	h2 := hash2(item)
	if h2 == 0 {
		h2 = 1
	}

	indexes := make([]int, 0, int(bf.k))
	for i := uint32(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % m
		indexes = append(indexes, int(idx))
	}
	return indexes
}

func hash1(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func hash2(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
