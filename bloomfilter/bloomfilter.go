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

type BloomFilter struct {
	arr []uint32
	k   uint32
	mu  sync.RWMutex
}

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

func NewDefault() *BloomFilter {
	return New(defaultBitsetSize, defaultHashCount)
}

func (bf *BloomFilter) Add(item string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if len(bf.arr) == 0 {
		return
	}
	for _, idx := range bf.indexes(item) {
		bf.arr[idx]++
	}
}

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

func (bf *BloomFilter) Remove(item string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if len(bf.arr) == 0 {
		return
	}
	for _, idx := range bf.indexes(item) {
		if bf.arr[idx] > 0 {
			bf.arr[idx]--
		}
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
