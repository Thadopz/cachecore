package cache

import (
	"sync"
	"time"

	"goCache/lru"
)

type Cache interface {
	add(key string, value ByteView)
	addWithTTL(key string, value ByteView, ttl time.Duration)
	get(key string) (value ByteView, ok bool)
	clearupExpired()
}

type onEvictedFunc func(key string, value ByteView)

type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
	onEvicted  onEvictedFunc
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, func(k string, v lru.Value) {
			if c.onEvicted != nil {
				c.onEvicted(k, v.(ByteView))
			}
		})
	}
	c.lru.Add(key, value)
}

func (c *cache) addWithTTL(key string, value ByteView, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, func(k string, v lru.Value) {
			if c.onEvicted != nil {
				c.onEvicted(k, v.(ByteView))
			}
		})
	}
	c.lru.AddWithTTL(key, value, ttl)
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), true
	}
	return
}

func (c *cache) clearupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.RemoveExpired()
}
