package cache

import "sync"

const defaultIncrementLockStripes = 256

type stripedLocks struct {
	locks []sync.Mutex
}

func newStripedLocks(stripes int) *stripedLocks {
	if stripes <= 0 {
		stripes = defaultIncrementLockStripes
	}
	return &stripedLocks{locks: make([]sync.Mutex, stripes)}
}

func (l *stripedLocks) lock(key string) func() {
	if l == nil || len(l.locks) == 0 {
		return func() {}
	}
	idx := djb33(0, key) % uint32(len(l.locks))
	mu := &l.locks[idx]
	mu.Lock()
	return mu.Unlock
}
