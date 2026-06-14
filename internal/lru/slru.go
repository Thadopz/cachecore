package lru

import (
	"container/list"
	"time"
)

const defaultProtectedRatio = 0.8

type segment uint8

const (
	segProbation segment = iota
	segProtected
)

type slruEntry struct {
	key      string
	value    Value
	expireAt time.Time
	seg      segment
}

// SLRUCache is a segmented LRU cache.
type SLRUCache struct {
	maxBytes       int64
	curbytes       int64
	probationBytes int64
	protectedBytes int64
	protectedRatio float64
	probation      *list.List
	protected      *list.List
	cache          map[string]*list.Element
	onEvicted      func(key string, value Value)
}

// NewSLRU creates a segmented LRU cache.
func NewSLRU(maxBytes int64, protectedRatio float64, onEvicted func(string, Value)) *SLRUCache {
	if protectedRatio <= 0 || protectedRatio >= 1 {
		protectedRatio = defaultProtectedRatio
	}
	return &SLRUCache{
		maxBytes:       maxBytes,
		protectedRatio: protectedRatio,
		probation:      list.New(),
		protected:      list.New(),
		cache:          make(map[string]*list.Element),
		onEvicted:      onEvicted,
	}
}

// Get returns the cached value for key.
func (c *SLRUCache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*slruEntry)
		if !kv.expireAt.IsZero() && time.Now().After(kv.expireAt) {
			c.removeElement(ele, true)
			return nil, false
		}
		if kv.seg == segProtected {
			if ele != c.protected.Front() {
				c.protected.MoveToFront(ele)
			}
			return kv.value, true
		}
		c.promote(ele)
		return kv.value, true
	}
	return nil, false
}

// RemoveOldest removes the oldest entry from the probation or protected segment.
func (c *SLRUCache) RemoveOldest() {
	if ele := c.probation.Back(); ele != nil {
		c.removeElement(ele, true)
		return
	}
	if ele := c.protected.Back(); ele != nil {
		c.removeElement(ele, true)
	}
}

// Add stores value without expiration.
func (c *SLRUCache) Add(key string, value Value) {
	c.AddWithTTL(key, value, 0)
}

// AddWithTTL stores value with an optional TTL.
func (c *SLRUCache) AddWithTTL(key string, value Value, ttl time.Duration) {
	expireAt := time.Time{}
	if ttl > 0 {
		expireAt = time.Now().Add(ttl)
	}

	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*slruEntry)
		oldCost := c.cost(kv)
		kv.value = value
		kv.expireAt = expireAt
		newCost := c.cost(kv)
		delta := newCost - oldCost
		c.curbytes += delta
		if kv.seg == segProtected {
			c.protectedBytes += delta
			if ele != c.protected.Front() {
				c.protected.MoveToFront(ele)
			}
		} else {
			c.probationBytes += delta
			if ele != c.probation.Front() {
				c.probation.MoveToFront(ele)
			}
		}
		c.enforceProtectedLimit()
		c.enforceTotalLimit()
		return
	}

	kv := &slruEntry{key: key, value: value, expireAt: expireAt, seg: segProbation}
	ele := c.probation.PushFront(kv)
	c.cache[key] = ele
	cost := c.cost(kv)
	c.curbytes += cost
	c.probationBytes += cost
	c.enforceTotalLimit()
}

// Remove deletes key from the cache.
func (c *SLRUCache) Remove(key string) {
	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele, true)
	}
}

// RemoveExpired removes expired entries.
func (c *SLRUCache) RemoveExpired() {
	now := time.Now()
	for ele := c.probation.Back(); ele != nil; {
		prev := ele.Prev()
		kv := ele.Value.(*slruEntry)
		if !kv.expireAt.IsZero() && now.After(kv.expireAt) {
			c.removeElement(ele, true)
		}
		ele = prev
	}
	for ele := c.protected.Back(); ele != nil; {
		prev := ele.Prev()
		kv := ele.Value.(*slruEntry)
		if !kv.expireAt.IsZero() && now.After(kv.expireAt) {
			c.removeElement(ele, true)
		}
		ele = prev
	}
}

// Len returns the number of cache entries.
func (c *SLRUCache) Len() int {
	return len(c.cache)
}

func (c *SLRUCache) promote(ele *list.Element) {
	kv := ele.Value.(*slruEntry)
	cost := c.cost(kv)
	c.probation.Remove(ele)
	c.probationBytes -= cost
	kv.seg = segProtected
	newEle := c.protected.PushFront(kv)
	c.cache[kv.key] = newEle
	c.protectedBytes += cost
	c.enforceProtectedLimit()
	c.enforceTotalLimit()
}

func (c *SLRUCache) enforceProtectedLimit() {
	if c.maxBytes <= 0 {
		return
	}
	protectedMax := c.protectedMaxBytes()
	for c.protectedBytes > protectedMax && c.protected.Len() > 0 {
		ele := c.protected.Back()
		kv := ele.Value.(*slruEntry)
		cost := c.cost(kv)
		c.protected.Remove(ele)
		c.protectedBytes -= cost
		kv.seg = segProbation
		newEle := c.probation.PushFront(kv)
		c.cache[kv.key] = newEle
		c.probationBytes += cost
	}
}

func (c *SLRUCache) enforceTotalLimit() {
	for c.maxBytes != 0 && c.curbytes > c.maxBytes {
		if ele := c.probation.Back(); ele != nil {
			c.removeElement(ele, true)
			continue
		}
		if ele := c.protected.Back(); ele != nil {
			c.removeElement(ele, true)
			continue
		}
		return
	}
}

func (c *SLRUCache) protectedMaxBytes() int64 {
	protectedMax := int64(float64(c.maxBytes) * c.protectedRatio)
	if protectedMax <= 0 {
		return 1
	}
	return protectedMax
}

func (c *SLRUCache) removeElement(ele *list.Element, evict bool) {
	kv := ele.Value.(*slruEntry)
	cost := c.cost(kv)
	if kv.seg == segProtected {
		c.protected.Remove(ele)
		c.protectedBytes -= cost
	} else {
		c.probation.Remove(ele)
		c.probationBytes -= cost
	}
	delete(c.cache, kv.key)
	c.curbytes -= cost
	if evict && c.onEvicted != nil {
		c.onEvicted(kv.key, kv.value)
	}
}

func (c *SLRUCache) cost(kv *slruEntry) int64 {
	return int64(len(kv.key)) + int64(kv.value.Len())
}
