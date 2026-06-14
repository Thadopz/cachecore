package cache

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Thadopz/cachecore/internal/lru"
)

// Cache defines the local storage operations used by a Group.
type Cache interface {
	add(key string, value ByteView)
	addWithTTL(key string, value ByteView, ttl time.Duration)
	get(key string) (value ByteView, ok bool)
	increment(key string, delta int64, ttl time.Duration, makeValue func([]byte) ByteView) (int64, error)
	remove(key string)
	clearupExpired()
}

type onEvictedFunc func(key string, value ByteView)

type localCache interface {
	Add(key string, value lru.Value)
	AddWithTTL(key string, value lru.Value, ttl time.Duration)
	Get(key string) (value lru.Value, ok bool)
	Remove(key string)
	RemoveExpired()
}

type cache struct {
	mu                 sync.Mutex
	store              localCache
	cacheBytes         int64
	onEvicted          onEvictedFunc
	useSLRU            bool
	slruProtectedRatio float64
}

func (c *cache) ensureStore() {
	if c.store != nil {
		return
	}
	if c.useSLRU {
		c.store = lru.NewSLRU(c.cacheBytes, c.slruProtectedRatio, func(k string, v lru.Value) {
			if c.onEvicted != nil {
				c.onEvicted(k, v.(ByteView))
			}
		})
		return
	}
	c.store = lru.New(c.cacheBytes, func(k string, v lru.Value) {
		if c.onEvicted != nil {
			c.onEvicted(k, v.(ByteView))
		}
	})
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStore()
	c.store.Add(key, value)
}

func (c *cache) addWithTTL(key string, value ByteView, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStore()
	c.store.AddWithTTL(key, value, ttl)
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	start := time.Now()
	defer func() {
		Stats.RecordCacheGetLatency(time.Since(start))
	}()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store == nil {
		return
	}
	if v, ok := c.store.Get(key); ok {
		return v.(ByteView), true
	}
	return
}

func (c *cache) increment(key string, delta int64, ttl time.Duration, makeValue func([]byte) ByteView) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStore()

	current := int64(0)
	if v, ok := c.store.Get(key); ok {
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
	c.store.AddWithTTL(key, value, ttl)
	return next, nil
}

func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store == nil {
		return
	}
	c.store.Remove(key)
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
	if c.store == nil {
		return
	}
	c.store.RemoveExpired()
}
