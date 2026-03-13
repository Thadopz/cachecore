package lru

import (
	"container/list"
	"time"
)

type Cache struct {
	maxBytes  int64
	curbytes  int64
	ll        *list.List
	cache     map[string]*list.Element
	onEvicted func(key string, value Value)
}

type Value interface {
	Len() int
}

type entry struct {
	key      string
	value    Value
	expireAt time.Time
}

func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		onEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		if !kv.expireAt.IsZero() && time.Now().After(kv.expireAt) {
			c.Remove(key)
			return nil, false
		}
		c.ll.MoveToFront(ele)

		return kv.value, true
	}
	return
}

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

func (c *Cache) Add(key string, value Value) {
	c.AddWithTTL(key, value, 0)
}

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
		ele := c.ll.PushFront(&entry{key, value, time.Now().Add(ttl)})
		c.cache[key] = ele
		c.curbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.curbytes > c.maxBytes {
		c.RemoveOldest()
	}
}

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

func (c *Cache) Len() int {
	return c.ll.Len()
}
