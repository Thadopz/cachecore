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

func TestGroupWithSLRUCachesHotKey(t *testing.T) {
	var calls int32
	g := NewGroup("test-cache-hot-key-slru", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value:" + key), nil
	}), WithSLRU(0.8))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("SLRU group should cache hot key: got getter calls %d, want 1", got)
	}
}

func TestGroupWithShardedSLRUExpiresAndInvalidates(t *testing.T) {
	var calls int32
	g := NewGroup("test-cache-sharded-slru", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		n := atomic.AddInt32(&calls, 1)
		return []byte(fmt.Sprintf("value-%d", n)), nil
	}), WithShardedCache(4), WithSLRU(0.8), WithRandomTTL(20*time.Millisecond, 0))

	v, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := v.String(); got != "value-1" {
		t.Fatalf("first value = %q, want value-1", got)
	}

	time.Sleep(35 * time.Millisecond)
	v, err = g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("get after ttl failed: %v", err)
	}
	if got := v.String(); got != "value-2" {
		t.Fatalf("value after ttl = %q, want value-2", got)
	}

	g.Invalidate("Tom")
	v, err = g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("get after invalidate failed: %v", err)
	}
	if got := v.String(); got != "value-3" {
		t.Fatalf("value after invalidate = %q, want value-3", got)
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

	firstReturned := false
	select {
	case err := <-firstDone:
		firstReturned = true
		if !errors.Is(err, ErrSingleflightWaitTimeout) {
			t.Fatalf("first load should also timeout under strict waitTTL policy: %v", err)
		}
	default:
		// still running means the second request really exited before first load finished.
	}

	if !firstReturned {
		err = <-firstDone
		if !errors.Is(err, ErrSingleflightWaitTimeout) {
			t.Fatalf("first load should timeout: %v", err)
		}
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

func TestGroupGetSingleflightWaitTTLServesStaleWhileRevalidating(t *testing.T) {
	var calls int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var refreshOnce sync.Once

	g := NewGroup("test-singleflight-swr-stale", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return []byte("old"), nil
		}
		refreshOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
		return []byte("new"), nil
	}), WithRandomTTL(20*time.Millisecond, 0), WithSingleflightWaitTTL(10*time.Millisecond), WithStaleWhileRevalidate(200*time.Millisecond))

	v, err := g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got := v.String(); got != "old" {
		t.Fatalf("unexpected first value: got %s, want old", got)
	}

	time.Sleep(35 * time.Millisecond)
	refreshDone := make(chan error, 1)
	go func() {
		_, err := g.Get(context.Background(), "Tom")
		refreshDone <- err
	}()
	<-refreshStarted

	v, err = g.Get(context.Background(), "Tom")
	if err != nil {
		t.Fatalf("timeout during refresh should serve stale value: %v", err)
	}
	if got := v.String(); got != "old" {
		t.Fatalf("unexpected stale value: got %s, want old", got)
	}

	close(releaseRefresh)
	if err := <-refreshDone; err != nil {
		t.Fatalf("refresh trigger should receive stale value instead of failing: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		v, err = g.Get(context.Background(), "Tom")
		if err == nil && v.String() == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for refreshed value, last value=%q err=%v", v.String(), err)
		}
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("SWR should not amplify loader calls: got %d", got)
	}
}

func TestGroupGetSingleflightWaitTTLDoesNotServeExpiredStale(t *testing.T) {
	var calls int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var refreshOnce sync.Once

	g := NewGroup("test-singleflight-swr-expired", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return []byte("old"), nil
		}
		refreshOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
		return []byte("new"), nil
	}), WithRandomTTL(20*time.Millisecond, 0), WithSingleflightWaitTTL(10*time.Millisecond), WithStaleWhileRevalidate(25*time.Millisecond))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}

	time.Sleep(70 * time.Millisecond)
	refreshDone := make(chan error, 1)
	go func() {
		_, err := g.Get(context.Background(), "Tom")
		refreshDone <- err
	}()
	<-refreshStarted

	v, err := g.Get(context.Background(), "Tom")
	if !errors.Is(err, ErrSingleflightWaitTimeout) {
		t.Fatalf("expected stale window to expire, got value=%q err=%v", v.String(), err)
	}

	close(releaseRefresh)
	<-refreshDone

	deadline := time.Now().Add(time.Second)
	for {
		v, err = g.Get(context.Background(), "Tom")
		if err == nil && v.String() == "new" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for refreshed value, last value=%q err=%v", v.String(), err)
		}
		time.Sleep(time.Millisecond)
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
