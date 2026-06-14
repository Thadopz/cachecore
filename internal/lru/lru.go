package lru

import (
	"container/list"
	"time"
)

// Cache is a size-limited LRU cache.
type Cache struct {
	maxBytes  int64
	curbytes  int64
	ll        *list.List
	cache     map[string]*list.Element
	onEvicted func(key string, value Value)
}

// Value is a cache value with a byte size.
type Value interface {
	Len() int
}

type entry struct {
	key      string
	value    Value
	expireAt time.Time
}

// New creates an LRU cache.
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		onEvicted: onEvicted,
	}
}

// Get returns the cached value for key.
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		if !kv.expireAt.IsZero() && time.Now().After(kv.expireAt) {
			c.Remove(key)
			return nil, false
		}
		if ele != c.ll.Front() {
			c.ll.MoveToFront(ele)
		}
		return kv.value, true
	}
	return
}

// RemoveOldest removes the least recently used item.
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.curbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.onEvicted != nil {
			c.onEvicted(kv.key, kv.value)
		}
	}
}

// Add stores value without expiration.
func (c *Cache) Add(key string, value Value) {
	c.AddWithTTL(key, value, 0)
}

// AddWithTTL stores value with an optional TTL.
func (c *Cache) AddWithTTL(key string, value Value, ttl time.Duration) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.curbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
		if ttl > 0 {
			kv.expireAt = time.Now().Add(ttl)
		} else {
			kv.expireAt = time.Time{}
		}
	} else {
		expireAt := time.Time{}
		if ttl > 0 {
			expireAt = time.Now().Add(ttl)
		}
		ele := c.ll.PushFront(&entry{key: key, value: value, expireAt: expireAt})
		c.cache[key] = ele
		c.curbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.curbytes > c.maxBytes {
		c.RemoveOldest()
	}
}

// Remove deletes key from the cache.
func (c *Cache) Remove(key string) {
	if ele, ok := c.cache[key]; ok {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, key)
		c.curbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.onEvicted != nil {
			c.onEvicted(kv.key, kv.value)
		}
	}
}

// RemoveExpired removes expired entries.
func (c *Cache) RemoveExpired() {
	for ele := c.ll.Back(); ele != nil; {
		prev := ele.Prev()
		kv := ele.Value.(*entry)
		if !kv.expireAt.IsZero() && time.Now().After(kv.expireAt) {
			c.Remove(kv.key)
		}
		ele = prev
	}
}

// Len returns the number of cache entries.
func (c *Cache) Len() int {
	return c.ll.Len()
}
