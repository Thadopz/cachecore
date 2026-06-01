package cache

import "time"

type ShardedCache struct {
	seed uint32
	m    uint32
	cs   []*cache
}

func djb33(seed uint32, k string) uint32 {
	d := uint32(5381) + seed + uint32(len(k))
	for i := 0; i < len(k); i++ {
		d = (d * 33) ^ uint32(k[i])
	}
	return d ^ (d >> 16)
}

func newShardedCache(seed uint32, m uint32, cacheBytes int64, onEvicted onEvictedFunc) *ShardedCache {
	cs := make([]*cache, m)
	for i := range cs {
		cs[i] = &cache{cacheBytes: cacheBytes, onEvicted: onEvicted}
	}
	return &ShardedCache{
		seed: seed,
		m:    m,
		cs:   cs,
	}
}

func (sc *ShardedCache) bucket(key string) *cache {
	idx := djb33(sc.seed, key) % sc.m
	return sc.cs[idx]
}

func (sc *ShardedCache) add(key string, value ByteView) {
	sc.bucket(key).add(key, value)
}

func (sc *ShardedCache) addWithTTL(key string, value ByteView, ttl time.Duration) {
	sc.bucket(key).addWithTTL(key, value, ttl)
}

func (sc *ShardedCache) get(key string) (value ByteView, ok bool) {
	return sc.bucket(key).get(key)
}

func (sc *ShardedCache) increment(key string, delta int64, ttl time.Duration, makeValue func([]byte) ByteView) (int64, error) {
	return sc.bucket(key).increment(key, delta, ttl, makeValue)
}

func (sc *ShardedCache) remove(key string) {
	sc.bucket(key).remove(key)
}

func (sc *ShardedCache) clearupExpired() {
	for _, c := range sc.cs {
		c.clearupExpired()
	}
}
