package cache

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Thadopz/cachecore/internal/lru"
)

type Cache interface {
	add(key string, value ByteView)
	addWithTTL(key string, value ByteView, ttl time.Duration)
	get(key string) (value ByteView, ok bool)
	increment(key string, delta int64, ttl time.Duration, makeValue func([]byte) ByteView) (int64, error)
	remove(key string)
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
	start := time.Now()
	defer func() {
		Stats.RecordCacheGetLatency(time.Since(start))
	}()

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

func (c *cache) increment(key string, delta int64, ttl time.Duration, makeValue func([]byte) ByteView) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, func(k string, v lru.Value) {
			if c.onEvicted != nil {
				c.onEvicted(k, v.(ByteView))
			}
		})
	}

	current := int64(0)
	if v, ok := c.lru.Get(key); ok {
		view := v.(ByteView)
		if !view.isNotFound() && len(view.b) > 0 {
			n, err := parseNumericValue(view.b)
			if err != nil {
				return 0, err
			}
			current = n
		}
	}

	next := current + delta
	raw := []byte(strconv.FormatInt(next, 10))
	value := ByteView{b: raw}
	if makeValue != nil {
		value = makeValue(raw)
	}
	c.lru.AddWithTTL(key, value, ttl)
	return next, nil
}

func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

func parseNumericValue(raw []byte) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, ErrNonNumericValue
	}
	return n, nil
}

func (c *cache) clearupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.RemoveExpired()
}
