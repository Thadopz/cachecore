package cache

import (
	"context"
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

func (f *testFilter) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = make(map[string]struct{})
}

func TestGroupGetCachesHotKey(t *testing.T) {
	var calls int32
	g := NewGroup("test-cache-hot-key", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		if key != "Tom" {
			return nil, fmt.Errorf("unexpected key: %s", key)
		}
		return []byte("630"), nil
	}))

	v1, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := v1.String(); got != "630" {
		t.Fatalf("unexpected first value: got %s, want 630", got)
	}

	v2, err := g.Get(context.Background(), "Tom")
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

func TestGroupIncrementWithIncrementerUpdatesCache(t *testing.T) {
	var counter int64 = 40
	g := NewGroup("test-increment-backend", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter should not be called after increment fills cache")
	}), WithIncrementer(IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		if key != "counter" {
			return 0, fmt.Errorf("unexpected key: %s", key)
		}
		return atomic.AddInt64(&counter, delta), nil
	})))

	next, err := g.Increment(context.Background(), "counter", 2)
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if got, want := next, int64(42); got != want {
		t.Fatalf("unexpected increment result: got %d, want %d", got, want)
	}

	view, err := g.Get(context.Background(), "counter")
	if err != nil {
		t.Fatalf("get after increment failed: %v", err)
	}
	if got, want := view.String(), "42"; got != want {
		t.Fatalf("unexpected cached value: got %q, want %q", got, want)
	}
}

func TestGroupIncrementCacheOnlyConcurrent(t *testing.T) {
	g := NewGroup("test-increment-cache-only-concurrent", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, ErrNotFound
	}))

	const workers = 100
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := g.Increment(context.Background(), "counter", 1)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("increment failed: %v", err)
		}
	}

	view, err := g.Get(context.Background(), "counter")
	if err != nil {
		t.Fatalf("get after concurrent increment failed: %v", err)
	}
	if got, want := view.String(), "100"; got != want {
		t.Fatalf("unexpected final value: got %q, want %q", got, want)
	}
}

func TestGroupIncrementWithIncrementerSerializesSameKey(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	var calls int32

	g := NewGroup("test-increment-backend-same-key-serial", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter should not be called")
	}), WithIncrementer(IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			close(firstEntered)
			<-releaseFirst
			return 1, nil
		}
		close(secondEntered)
		return 2, nil
	})))

	firstDone := make(chan error, 1)
	go func() {
		_, err := g.Increment(context.Background(), "counter", 1)
		firstDone <- err
	}()

	<-firstEntered
	secondDone := make(chan error, 1)
	go func() {
		_, err := g.Increment(context.Background(), "counter", 1)
		secondDone <- err
	}()

	select {
	case <-secondEntered:
		t.Fatalf("same-key incrementer call should wait for the first call")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first increment failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second increment failed: %v", err)
	}

	view, err := g.Get(context.Background(), "counter")
	if err != nil {
		t.Fatalf("get after increments failed: %v", err)
	}
	if got, want := view.String(), "2"; got != want {
		t.Fatalf("cache should keep newest increment value: got %q, want %q", got, want)
	}
}

func TestGroupIncrementDifferentStripesRunInParallel(t *testing.T) {
	blockedKey, fastKey := differentIncrementStripeKeys(t)
	blockedEntered := make(chan struct{})
	releaseBlocked := make(chan struct{})
	fastEntered := make(chan struct{})

	g := NewGroup("test-increment-backend-different-stripes", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter should not be called")
	}), WithIncrementer(IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		switch key {
		case blockedKey:
			close(blockedEntered)
			<-releaseBlocked
			return 1, nil
		case fastKey:
			close(fastEntered)
			return 1, nil
		default:
			return 0, fmt.Errorf("unexpected key: %s", key)
		}
	})))

	blockedDone := make(chan error, 1)
	go func() {
		_, err := g.Increment(context.Background(), blockedKey, 1)
		blockedDone <- err
	}()
	<-blockedEntered

	fastDone := make(chan error, 1)
	go func() {
		_, err := g.Increment(context.Background(), fastKey, 1)
		fastDone <- err
	}()

	select {
	case <-fastEntered:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("different-stripe increment should not wait for blocked key")
	}
	if err := <-fastDone; err != nil {
		t.Fatalf("fast increment failed: %v", err)
	}

	close(releaseBlocked)
	if err := <-blockedDone; err != nil {
		t.Fatalf("blocked increment failed: %v", err)
	}
}

func differentIncrementStripeKeys(t *testing.T) (string, string) {
	t.Helper()
	first := "counter-0"
	firstStripe := djb33(0, first) % defaultIncrementLockStripes
	for i := 1; i < 10000; i++ {
		candidate := fmt.Sprintf("counter-%d", i)
		if djb33(0, candidate)%defaultIncrementLockStripes != firstStripe {
			return first, candidate
		}
	}
	t.Fatalf("failed to find keys mapped to different increment lock stripes")
	return "", ""
}

func TestGroupIncrementRejectsNonNumericCachedValue(t *testing.T) {
	g := NewGroup("test-increment-non-numeric", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("abc"), nil
	}))
	if _, err := g.Get(context.Background(), "counter"); err != nil {
		t.Fatalf("get failed: %v", err)
	}

	_, err := g.Increment(context.Background(), "counter", 1)
	if !errors.Is(err, ErrNonNumericValue) {
		t.Fatalf("expected ErrNonNumericValue, got %v", err)
	}
}

func TestGroupGetUsesSingleflightForConcurrentRequests(t *testing.T) {
	var calls int32
	g := NewGroup("test-singleflight-hot-key", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
			v, err := g.Get(context.Background(), "Jack")
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

func TestGroupGetSingleflightWaitTTLTimeout(t *testing.T) {
	var calls int32
	started := make(chan struct{})
	var once sync.Once

	g := NewGroup("test-singleflight-wait-timeout", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		once.Do(func() { close(started) })
		atomic.AddInt32(&calls, 1)
		time.Sleep(80 * time.Millisecond)
		return []byte("slow"), nil
	}), WithSingleflightWaitTTL(15*time.Millisecond))

	firstDone := make(chan error, 1)
	go func() {
		_, err := g.Get(context.Background(), "Tom")
		firstDone <- err
	}()

	<-started
	v, err := g.Get(context.Background(), "Tom")
	if !errors.Is(err, ErrSingleflightWaitTimeout) {
		t.Fatalf("expected ErrSingleflightWaitTimeout, got value=%q err=%v", v.String(), err)
	}

	select {
	case err := <-firstDone:
		if !errors.Is(err, ErrSingleflightWaitTimeout) {
			t.Fatalf("first load should also timeout under strict waitTTL policy: %v", err)
		}
	default:
		// still running means the second request really exited before first load finished.
	}

	err = <-firstDone
	if !errors.Is(err, ErrSingleflightWaitTimeout) {
		t.Fatalf("first load should timeout: %v", err)
	}

	time.Sleep(90 * time.Millisecond)
	v, err = g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("value should be available after inflight load completes: %v", err)
	}
	if got := v.String(); got != "slow" {
		t.Fatalf("unexpected cached value after inflight completion: got %s, want slow", got)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("timeout path should not amplify loader calls: got %d", got)
	}
}

func TestLoadSingleflightWaitTTLServesFallback(t *testing.T) {
	var calls int32
	var slowMode int32

	g := NewGroup("test-singleflight-wait-fallback", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		if atomic.LoadInt32(&slowMode) == 1 {
			time.Sleep(80 * time.Millisecond)
			return []byte("v2"), nil
		}
		return []byte("v1"), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0), WithSingleflightWaitTTL(15*time.Millisecond))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(8); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	atomic.StoreInt32(&slowMode, 1)
	firstDone := make(chan error, 1)
	go func() {
		_, err := g.load(context.Background(), "Tom")
		firstDone <- err
	}()

	time.Sleep(5 * time.Millisecond)
	v, err := g.load(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("second load should degrade to fallback, got err=%v", err)
	}
	if got := v.String(); got != "v1" {
		t.Fatalf("expected fallback value v1, got %s", got)
	}

	err = <-firstDone
	if err != nil {
		t.Fatalf("first load failed: %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 total getter calls (warm + one inflight), got %d", got)
	}
}

func TestGroupGetBlockedByFilterWhenReady(t *testing.T) {
	var calls int32
	filter := newTestFilter()
	g := NewGroupWithFilter("test-filter-block", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}), filter)
	g.filter.ready.Store(true)

	_, err := g.Get(context.Background(), "missing-key")
	if !errors.Is(err, ErrFilterNotFound) {
		t.Fatalf("expected ErrFilterNotFound, got: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("getter should not be called when filter blocks request: got %d", got)
	}
}

func TestWarmupSetsReadyOnSuccess(t *testing.T) {
	filter := newTestFilter()
	g := NewGroupWithFilter("test-warmup-success", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("ok"), nil
	}), filter)

	g.Warmup([]string{"Tom", "Jack"})

	if !g.filter.ready.Load() {
		t.Fatalf("filter should be ready after successful warmup")
	}
}

func TestWarmupKeepsNotReadyOnFailure(t *testing.T) {
	filter := newTestFilter()
	g := NewGroupWithFilter("test-warmup-failure", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		if key == "missing" {
			return nil, fmt.Errorf("not found")
		}
		return []byte("ok"), nil
	}), filter)

	g.Warmup([]string{"Tom", "missing"})

	if g.filter.ready.Load() {
		t.Fatalf("filter should remain not ready when warmup has failures")
	}
}

func TestGroupWithRandomTTLExpiresEntry(t *testing.T) {
	var calls int32
	g := NewGroup("test-random-ttl-expire", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithRandomTTL(40*time.Millisecond, 0))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter call count after first get: got %d, want 1", got)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after ttl expires: got %d, want 2", got)
	}
}

func TestGroupWithRandomTTLSamplesWithinRange(t *testing.T) {
	g := NewGroup("test-random-ttl-range", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
	g := NewGroup("test-invalidate-main-cache", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter call count after first get: got %d, want 1", got)
	}

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("second get should hit cache: got %d, want 1", got)
	}

	g.Invalidate("Tom")

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("third get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after invalidate: got %d, want 2", got)
	}
}

func TestInvalidateKeepsFilterEntry(t *testing.T) {
	filter := newTestFilter()
	g := NewGroupWithFilter("test-invalidate-filter", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value"), nil
	}), filter)

	g.Warmup([]string{"Tom"})
	g.Invalidate("Tom")

	if !filter.Contains("Tom") {
		t.Fatalf("invalidation should not delete add-only filter entries")
	}
}

func TestFilterRefreshRebuildsFromWarmupKeys(t *testing.T) {
	filter := newTestFilter()
	var calls int32
	g := NewGroup("test-refresh-filter", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value-" + key), nil
	}), WithFilter(filter), WithFilterRefresh(25*time.Millisecond))
	defer g.StopFilterRefresh()

	g.Warmup([]string{"Tom"})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 2 && g.filter.ready.Load() && filter.Contains("Tom") {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("filter refresh should rerun warmup getters, got %d calls", got)
	}
	t.Fatalf("filter refresh did not publish a ready rebuild for warmup keys")
}

func TestSwitchToShardedReadsFromFallbackAndRefillsActive(t *testing.T) {
	var calls int32
	g := NewGroup("test-switch-fallback-refill", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v-" + key), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
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

	v, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("get after switch failed: %v", err)
	}
	if got := v.String(); got != "v-Tom" {
		t.Fatalf("unexpected value after switch: got %s, want v-Tom", got)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fallback read should not call getter again: got %d, want 1", got)
	}

	g.router.mu.RLock()
	active := g.router.active
	g.router.mu.RUnlock()
	if active == nil {
		t.Fatalf("active cache should exist")
	}
	if _, ok := active.get("Tom"); !ok {
		t.Fatalf("active cache should be refilled after fallback hit")
	}
}

func TestFallbackRefillSkippedWhenVersionOutdated(t *testing.T) {
	var calls int32
	g := NewGroup("test-fallback-version-compare", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v-" + key), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(8); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	// Simulate a newer cache generation so the fallback entry epoch becomes stale.
	g.router.mu.Lock()
	g.router.fallbackEpoch++
	g.router.mu.Unlock()

	v, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("get after version advance failed: %v", err)
	}
	if got := v.String(); got != "v-Tom" {
		t.Fatalf("unexpected value from fallback: got %s, want v-Tom", got)
	}

	g.router.mu.RLock()
	active := g.router.active
	g.router.mu.RUnlock()
	if active == nil {
		t.Fatalf("active cache should exist")
	}
	if _, ok := active.get("Tom"); ok {
		t.Fatalf("stale fallback should not refill active cache")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should not be called for fallback hit: got %d, want 1", got)
	}
}

func TestFallbackExpiresAndCleanupStopsServingOldCache(t *testing.T) {
	var calls int32
	g := NewGroup("test-fallback-expire", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithFallbackTTL(30*time.Millisecond), WithSwitchCooldown(0))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(4); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	g.CleanupFallback()

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("get after fallback cleanup failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after fallback expiry: got %d, want 2", got)
	}
}

func TestFallbackTTLZeroNeverServesFallback(t *testing.T) {
	var calls int32
	g := NewGroup("test-fallback-ttl-zero", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("v"), nil
	}), WithFallbackTTL(0), WithSwitchCooldown(0))
	if g.router.fallbackTTL != 0 {
		t.Fatalf("unexpected fallbackTTL: got %v, want 0", g.router.fallbackTTL)
	}
	if mode := g.Mode(); mode != CacheModeUnsharded {
		t.Fatalf("unexpected initial mode: got %v, want unsharded", mode)
	}

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
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

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("get after switch failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again when fallbackTTL is 0: got %d, want 2", got)
	}
}

func TestInvalidateRemovesFromActiveAndFallback(t *testing.T) {
	var calls int32
	g := NewGroup("test-invalidate-active-fallback", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("x"), nil
	}), WithFallbackTTL(2*time.Second), WithSwitchCooldown(0))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if err := g.SwitchToSharded(4); err != nil {
		t.Fatalf("switch to sharded failed: %v", err)
	}

	g.Invalidate("Tom")

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("get after invalidate failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after invalidate: got %d, want 2", got)
	}
}

func TestSwitchRespectsCooldown(t *testing.T) {
	g := NewGroup("test-switch-cooldown", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
	g := NewGroup("test-auto-switch-hysteresis", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
	g := NewGroup("test-concurrent-get-switch", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
				v, err := g.Get(context.Background(), key)
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

func TestNegativeCacheCachesNotFound(t *testing.T) {
	var calls int32
	g := NewGroup("test-negative-cache-hit", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, ErrNotFound
	}), WithNegativeCache(200*time.Millisecond))

	_, err := g.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("first get should return ErrNotFound: %v", err)
	}

	_, err = g.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("second get should return ErrNotFound: %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should be called once within negative ttl: got %d", got)
	}
}

func TestNegativeCacheExpires(t *testing.T) {
	var calls int32
	g := NewGroup("test-negative-cache-expire", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, ErrNotFound
	}), WithNegativeCache(30*time.Millisecond))

	_, err := g.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("first get should return ErrNotFound: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	_, err = g.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("second get should return ErrNotFound: %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("getter should be called again after negative ttl expires: got %d", got)
	}
}

func TestNegativeCacheSingleflightConcurrentRequests(t *testing.T) {
	var calls int32
	g := NewGroup("test-negative-cache-singleflight", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(40 * time.Millisecond)
		return nil, ErrNotFound
	}), WithNegativeCache(200*time.Millisecond))

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := g.Get(context.Background(), "missing")
			if !errors.Is(err, ErrNotFound) {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("unexpected concurrent get error: %v", err)
		}
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should be called once for concurrent not found requests: got %d", got)
	}
}

func TestGroupGetHonorsCanceledContextBeforeLoad(t *testing.T) {
	var calls int32
	g := NewGroup("test-context-canceled-before-load", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Get(ctx, "Tom")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("getter should not be called after context cancel: got %d", got)
	}
}

func TestGroupGetDoesNotShareCallerCancellationAcrossSingleflight(t *testing.T) {
	var calls int32
	g := NewGroup("test-context-isolated-singleflight", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(40 * time.Millisecond)
		return []byte("value"), nil
	}), WithSingleflightWaitTTL(200*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	firstErrCh := make(chan error, 1)
	go func() {
		_, err := g.Get(ctx, "Tom")
		firstErrCh <- err
	}()

	time.Sleep(5 * time.Millisecond)
	v, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("second get should succeed after first caller cancels: %v", err)
	}
	if got := v.String(); got != "value" {
		t.Fatalf("unexpected value after shared load: got %q, want value", got)
	}

	if err := <-firstErrCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first get should see context deadline exceeded, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("getter should be called once for shared load: got %d", got)
	}
}
