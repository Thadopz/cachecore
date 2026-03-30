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

func TestSwitchToShardedReadsFromFallbackAndRefillsActive(t *testing.T) {
	var calls int32
	g := NewGroup("test-switch-fallback-refill", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v-" + key), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0))

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("unexpected getter calls before switch: got %d, want 1", got)
	}

	if err := g.SwitchToSharded(8); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeSharded {
		t.Fatalf("unexpected mode after switch: got %v, want sharded", mode)
	}

	v, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("get after switch failed: %v", err)
	}
	if got := v.String(); got != "v-Tom" {
		t.Fatalf("unexpected value after switch: got %s, want v-Tom", got)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fallback read should not call getter again: got %d, want 1", got)
	}

	g.routeMu.RLock()
	active := g.activeCache
	g.routeMu.RUnlock()
	if active == nil {
		t.Fatalf("active cache should exist")
	}
	if _, ok := active.get("Tom"); !ok {
		t.Fatalf("active cache should be refilled after fallback hit")
	}
}

func TestFallbackExpiresAndCleanupStopsServingOldCache(t *testing.T) {
	var calls int32
	g := NewGroup("test-fallback-expire", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithFallbackTTL(30*time.Millisecond), WithSwitchCooldown(0))

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(4); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	g.CleanupFallback()

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("get after fallback cleanup failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after fallback expiry: got %d, want 2", got)
	}
}

func TestFallbackTTLZeroNeverServesFallback(t *testing.T) {
	var calls int32
	g := NewGroup("test-fallback-ttl-zero", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithFallbackTTL(0), WithSwitchCooldown(0))
	if g.fallbackTTL != 0 {
		t.Fatalf("unexpected fallbackTTL: got %v, want 0", g.fallbackTTL)
	}
	if mode := g.Mode(); mode != CacheModeUnsharded {
		t.Fatalf("unexpected initial mode: got %v, want unsharded", mode)
	}

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(4); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeSharded {
		t.Fatalf("unexpected mode after switch: got %v, want sharded", mode)
	}

	active, fallback := g.currentCaches()
	if fallback != nil {
		t.Fatalf("fallback should be disabled when fallbackTTL is 0")
	}
	if active == nil {
		t.Fatalf("active cache should exist")
	}
	if _, ok := active.get("Tom"); ok {
		t.Fatalf("new active cache should be cold right after switch")
	}

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("get after switch failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again when fallbackTTL is 0: got %d, want 2", got)
	}
}

func TestInvalidateRemovesFromActiveAndFallback(t *testing.T) {
	var calls int32
	g := NewGroup("test-invalidate-active-fallback", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("x"), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0))

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(4); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	g.Invalidate("Tom")

	if _, err := g.Get("Tom"); err != nil {
		t.Fatalf("get after invalidate failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after invalidate: got %d, want 2", got)
	}
}

func TestSwitchRespectsCooldown(t *testing.T) {
	g := NewGroup("test-switch-cooldown", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("ok"), nil
	}), WithSwitchCooldown(150*time.Millisecond), WithFallbackTTL(time.Second))

	if err := g.SwitchToSharded(8); err != nil {
		t.Fatalf("first switch to sharded failed: %v", err)
	}
	if err := g.SwitchToUnsharded(); !errors.Is(err, ErrSwitchCooldown) {
		t.Fatalf("expected ErrSwitchCooldown, got: %v", err)
	}

	time.Sleep(170 * time.Millisecond)
	if err := g.SwitchToUnsharded(); err != nil {
		t.Fatalf("switch to unsharded after cooldown failed: %v", err)
	}
}

func TestAutoSwitchByMissRateHysteresis(t *testing.T) {
	g := NewGroup("test-auto-switch-hysteresis", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("ok"), nil
	}),
		WithSwitchCooldown(0),
		WithAutoSwitchByMissRate(AutoSwitchPolicy{
			Enable:          true,
			MissRateHigh:    0.7,
			MissRateLow:     0.3,
			HighConsecutive: 2,
			LowConsecutive:  2,
		}),
	)

	if mode := g.Mode(); mode != CacheModeUnsharded {
		t.Fatalf("unexpected initial mode: got %v, want unsharded", mode)
	}

	if err := g.EvaluateAutoSwitchOnce(0.8); err != nil {
		t.Fatalf("first high miss evaluation failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeUnsharded {
		t.Fatalf("mode should stay unsharded before high threshold streak: got %v", mode)
	}

	if err := g.EvaluateAutoSwitchOnce(0.85); err != nil {
		t.Fatalf("second high miss evaluation failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeSharded {
		t.Fatalf("mode should switch to sharded after high streak: got %v", mode)
	}

	if err := g.EvaluateAutoSwitchOnce(0.5); err != nil {
		t.Fatalf("mid miss evaluation failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeSharded {
		t.Fatalf("mode should stay sharded in hysteresis band: got %v", mode)
	}

	if err := g.EvaluateAutoSwitchOnce(0.2); err != nil {
		t.Fatalf("first low miss evaluation failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeSharded {
		t.Fatalf("mode should stay sharded before low threshold streak: got %v", mode)
	}

	if err := g.EvaluateAutoSwitchOnce(0.1); err != nil {
		t.Fatalf("second low miss evaluation failed: %v", err)
	}
	if mode := g.Mode(); mode != CacheModeUnsharded {
		t.Fatalf("mode should switch back to unsharded after low streak: got %v", mode)
	}
}

func TestConcurrentGetDuringSwitch(t *testing.T) {
	var calls int32
	g := NewGroup("test-concurrent-get-switch", 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v-" + key), nil
	}), WithFallbackTTL(time.Second), WithSwitchCooldown(0))

	keys := []string{"k1", "k2", "k3", "k4", "k5"}

	stop := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			idx := worker
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := keys[idx%len(keys)]
				idx++
				v, err := g.Get(key)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if v.String() == "" {
					select {
					case errCh <- fmt.Errorf("empty value for key=%s", key):
					default:
					}
					return
				}
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			_ = g.SwitchToSharded(8)
		} else {
			_ = g.SwitchToUnsharded()
		}
	}

	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent get during switch failed: %v", err)
		}
	}
}
