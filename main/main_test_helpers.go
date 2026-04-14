package main

import "sync"

type noopFilter struct {
	mu   sync.RWMutex
	data map[string]struct{}
}

func newNoopFilter() *noopFilter {
	return &noopFilter{data: make(map[string]struct{})}
}

func (f *noopFilter) Add(item string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[item] = struct{}{}
}

func (f *noopFilter) Contains(item string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.data[item]
	return ok
}

func (f *noopFilter) Remove(item string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, item)
}
