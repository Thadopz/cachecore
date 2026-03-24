package cache

import "time"

type ShardedCache struct {
	seed uint32
	m    uint32
	cs   []*cache
}

func djb33(seed uint32, k string) uint32 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)
	if l >= 4 {
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])
			d = (d * 33) ^ uint32(k[i+1])
			d = (d * 33) ^ uint32(k[i+2])
			d = (d * 33) ^ uint32(k[i+3])
			i += 4
		}
	}
	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i])
	case 3:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
	case 4:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
		d = (d * 33) ^ uint32(k[i+2])
	}
	return d ^ (d >> 16)
}

func defaultShardedCache(cacheBytes int64, onEvicted onEvictedFunc) *ShardedCache {
	return newShardedCache(0, 256, cacheBytes, onEvicted)
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

func (sc *ShardedCache) clearupExpired() {
	for _, c := range sc.cs {
		c.clearupExpired()
	}
}
