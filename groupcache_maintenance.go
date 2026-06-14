package cache

import "time"

// Janitor periodically removes expired entries from a group.
type Janitor struct {
	interval time.Duration
	stop     chan struct{}
}

// Run starts the cleanup loop for g.
func (j *Janitor) Run(g *Group) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			active := g.mainCache
			if active != nil {
				active.clearupExpired()
			}
			stale := g.staleCache
			if stale != nil {
				stale.clearupExpired()
			}
		case <-j.stop:
			return
		}
	}
}

// NewJanitor creates a janitor with interval.
func NewJanitor(interval time.Duration) *Janitor {
	if interval <= 0 {
		interval = time.Minute
	}
	return &Janitor{
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Stop stops the cleanup loop.
func (j *Janitor) Stop() {
	if j == nil || j.stop == nil {
		return
	}
	select {
	case <-j.stop:
		return
	default:
		close(j.stop)
	}
}

// StartFilterRefresh starts periodic filter warmup for the group.
func (g *Group) StartFilterRefresh(interval time.Duration) {
	if g.filter == nil {
		return
	}
	g.filter.startRefresh(interval, g.Warmup)
}

// StopFilterRefresh stops periodic filter refresh for the group.
func (g *Group) StopFilterRefresh() {
	g.filter.stopRefresh()
}
