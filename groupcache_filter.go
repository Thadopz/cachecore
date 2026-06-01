package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type filterGate struct {
	filter Filter

	ready atomic.Bool
	mu    sync.RWMutex

	warmupMu    sync.Mutex
	warmupKeys  []string
	refreshStop chan struct{}
}

func newFilterGate(filter Filter) *filterGate {
	if filter == nil {
		return nil
	}
	return &filterGate{filter: filter}
}

func (f *filterGate) add(key string) {
	if f == nil || f.filter == nil {
		return
	}
	f.mu.RLock()
	f.filter.Add(key)
	f.mu.RUnlock()
}

func (f *filterGate) rejects(key string) bool {
	if f == nil || f.filter == nil {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ready.Load() && !f.filter.Contains(key)
}

func (f *filterGate) warmup(keys []string, load func(context.Context, string) error) bool {
	if f == nil || f.filter == nil || load == nil {
		return false
	}

	keys = cloneStrings(keys)
	ready := true
	for _, key := range keys {
		if err := load(context.Background(), key); err != nil {
			ready = false
		}
	}
	if !ready {
		return false
	}

	f.warmupMu.Lock()
	f.warmupKeys = keys
	f.warmupMu.Unlock()
	f.ready.Store(true)
	return true
}

func (f *filterGate) startRefresh(interval time.Duration, warmup func([]string)) {
	if f == nil || f.filter == nil || interval <= 0 || warmup == nil {
		return
	}
	resettable, ok := f.filter.(ResettableFilter)
	if !ok {
		return
	}

	f.warmupMu.Lock()
	if f.refreshStop != nil {
		f.warmupMu.Unlock()
		return
	}
	stop := make(chan struct{})
	f.refreshStop = stop
	f.warmupMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				keys := f.keysForRefresh()
				if len(keys) == 0 {
					continue
				}
				f.reset(resettable)
				warmup(keys)
			case <-stop:
				return
			}
		}
	}()
}

func (f *filterGate) stopRefresh() {
	if f == nil {
		return
	}
	f.warmupMu.Lock()
	stop := f.refreshStop
	f.refreshStop = nil
	f.warmupMu.Unlock()
	if stop != nil {
		close(stop)
	}
}

func (f *filterGate) keysForRefresh() []string {
	f.warmupMu.Lock()
	defer f.warmupMu.Unlock()
	return cloneStrings(f.warmupKeys)
}

func (f *filterGate) reset(filter ResettableFilter) {
	f.mu.Lock()
	f.ready.Store(false)
	filter.Reset()
	f.mu.Unlock()
}
