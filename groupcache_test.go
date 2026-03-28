package cache

import (
	"errors"
	"fmt"
	"sync"
	atomic "sync/atomic"
	"testing"
	"time"
)

type testFilter struct {
	mu   sync.RWMutex
	data map[string]struct{}
}

func newTestFilter() *testFilter {
	return &testFilter{data: make(map[string]struct{})}
}

func (f *testFilter) Add(item string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[item] = struct{}{}
}

func (f *testFilter) Contains(item string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.data[item]
	return ok
}

func (f *testFilter) Remove(item string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, item)
}

func TestGroupGetCachesHotKey(t *testing.T) {
	var calls int32
	g := NewGroup("test-cache-hot-key", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		if key != "Tom" {
			return nil, fmt.Errorf("unexpected key: %s", key)
		}
		return []byte("630"), nil
	}))

	v1, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := v1.String(); got != "630" {
		t.Fatalf("unexpected first value: got %s, want 630", got)
	}

	v2, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := v2.String(); got != "630" {
		t.Fatalf("unexpected second value: got %s, want 630", got)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should be called once for hot key: got %d", got)
	}
}

func TestGroupGetUsesSingleflightForConcurrentRequests(t *testing.T) {
	var calls int32
	g := NewGroup("test-singleflight-hot-key", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(40 * time.Millisecond)
		return []byte("589"), nil
	}))

	const workers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Get("Jack")
			if err != nil {
				errCh <- err
				return
			}
			if v.String() != "589" {
				errCh <- fmt.Errorf("unexpected value: %s", v.String())
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent get failed: %v", err)
		}
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should be called once with singleflight: got %d", got)
	}
}

func TestGroupGetBlockedByFilterWhenReady(t *testing.T) {
	var calls int32
	filter := newTestFilter()
	g := NewGroupWithFilter("test-filter-block", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}), filter)
	g.filterReady.Store(true)

	_, err := g.Get("missing-key")
	if !errors.Is(err, ErrFilterNotFound) {
		t.Fatalf("expected ErrFilterNotFound, got: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("getter should not be called when filter blocks request: got %d", got)
	}
}

func TestWarmupSetsReadyOnSuccess(t *testing.T) {
	filter := newTestFilter()
	g := NewGroupWithFilter("test-warmup-success", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("ok"), nil
	}), filter)

	g.Warmup([]string{"Tom", "Jack"})

	if !g.filterReady.Load() {
		t.Fatalf("filter should be ready after successful warmup")
	}
}

func TestWarmupKeepsNotReadyOnFailure(t *testing.T) {
	filter := newTestFilter()
	g := NewGroupWithFilter("test-warmup-failure", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		if key == "missing" {
			return nil, fmt.Errorf("not found")
		}
		return []byte("ok"), nil
	}), filter)

	g.Warmup([]string{"Tom", "missing"})

	if g.filterReady.Load() {
		t.Fatalf("filter should remain not ready when warmup has failures")
	}
}

func TestGroupWithRandomTTLExpiresEntry(t *testing.T) {
	var calls int32
	g := NewGroup("test-random-ttl-expire", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithRandomTTL(40*time.Millisecond, 0))

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter call count after first get: got %d, want 1", got)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after ttl expires: got %d, want 2", got)
	}
}

func TestGroupWithRandomTTLSamplesWithinRange(t *testing.T) {
	g := NewGroup("test-random-ttl-range", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("v"), nil
	}), WithRandomTTL(100*time.Millisecond, 30*time.Millisecond))

	minTTL := 70 * time.Millisecond
	maxTTL := 130 * time.Millisecond

	for i := 0; i < 300; i++ {
		ttl := g.nextTTL()
		if ttl < minTTL || ttl > maxTTL {
			t.Fatalf("ttl out of expected range: got %s, want [%s, %s]", ttl, minTTL, maxTTL)
		}
	}
}

func TestInvalidateRemovesMainCacheEntry(t *testing.T) {
	var calls int32
	g := NewGroup("test-invalidate-main-cache", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}))

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter call count after first get: got %d, want 1", got)
	}

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("second get should hit cache: got %d, want 1", got)
	}

	g.Invalidate("Tom")

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("third get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after invalidate: got %d, want 2", got)
	}
}
