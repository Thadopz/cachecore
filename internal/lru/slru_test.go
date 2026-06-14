package lru

import (
	"testing"
	"time"
)

type slruTestValue string

func (v slruTestValue) Len() int {
	return len(v)
}

func TestSLRUNewKeyStartsInProbation(t *testing.T) {
	c := NewSLRU(100, 0.8, nil)
	c.Add("hot", slruTestValue("v"))

	if c.probation.Len() != 1 || c.protected.Len() != 0 {
		t.Fatalf("new key should start in probation, probation=%d protected=%d", c.probation.Len(), c.protected.Len())
	}
}

func TestSLRUProbationHitPromotesToProtected(t *testing.T) {
	c := NewSLRU(100, 0.8, nil)
	c.Add("hot", slruTestValue("v"))

	if _, ok := c.Get("hot"); !ok {
		t.Fatalf("expected hot key hit")
	}
	if c.probation.Len() != 0 || c.protected.Len() != 1 {
		t.Fatalf("probation hit should promote to protected, probation=%d protected=%d", c.probation.Len(), c.protected.Len())
	}
}

func TestSLRUProtectedLimitDemotesWithoutEvictionCallback(t *testing.T) {
	var evicted int
	c := NewSLRU(10, 0.5, func(string, Value) {
		evicted++
	})

	c.Add("a", slruTestValue("1"))
	c.Add("b", slruTestValue("1"))
	c.Add("c", slruTestValue("1"))
	_, _ = c.Get("a")
	_, _ = c.Get("b")
	_, _ = c.Get("c")

	if evicted != 0 {
		t.Fatalf("protected demotion should not evict, callbacks=%d", evicted)
	}
	if c.protectedBytes > c.protectedMaxBytes() {
		t.Fatalf("protected segment over limit: got %d max %d", c.protectedBytes, c.protectedMaxBytes())
	}
	if c.probation.Len() == 0 {
		t.Fatalf("expected protected overflow to demote an entry into probation")
	}
}

func TestSLRUScanEvictsProbationBeforeProtectedHotKey(t *testing.T) {
	c := NewSLRU(20, 0.8, nil)
	c.Add("hot", slruTestValue("v"))
	if _, ok := c.Get("hot"); !ok {
		t.Fatalf("expected hot promotion hit")
	}

	for i := 0; i < 10; i++ {
		c.Add(string(rune('a'+i)), slruTestValue("cold"))
	}

	if v, ok := c.Get("hot"); !ok || v.(slruTestValue) != "v" {
		t.Fatalf("protected hot key should survive probation scan, got value=%v ok=%v", v, ok)
	}
}

func TestSLRURemoveExpiredChecksBothSegments(t *testing.T) {
	c := NewSLRU(100, 0.8, nil)
	c.AddWithTTL("protected", slruTestValue("v"), 10*time.Millisecond)
	if _, ok := c.Get("protected"); !ok {
		t.Fatalf("expected protected key hit")
	}
	c.AddWithTTL("probation", slruTestValue("v"), 10*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	c.RemoveExpired()

	if c.Len() != 0 {
		t.Fatalf("expected both expired entries to be removed, len=%d", c.Len())
	}
}

func TestSLRURemoveAndCapacityCallbacks(t *testing.T) {
	keys := make([]string, 0)
	c := NewSLRU(8, 0.8, func(key string, value Value) {
		keys = append(keys, key)
	})

	c.Add("a", slruTestValue("1"))
	c.Remove("a")
	c.Add("long", slruTestValue("value"))

	if len(keys) != 2 {
		t.Fatalf("expected remove and capacity eviction callbacks, got %#v", keys)
	}
	if keys[0] != "a" || keys[1] != "long" {
		t.Fatalf("unexpected eviction callback order: %#v", keys)
	}
}
