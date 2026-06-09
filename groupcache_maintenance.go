package cache

import "time"

type Janitor struct {
	interval time.Duration
	stop     chan struct{}
}

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

func NewJanitor(interval time.Duration) *Janitor {
	if interval <= 0 {
		interval = time.Minute
	}
	return &Janitor{
		interval: interval,
		stop:     make(chan struct{}),
	}
}

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

func (g *Group) StartFilterRefresh(interval time.Duration) {
	if g.filter == nil {
		return
	}
	g.filter.startRefresh(interval, g.Warmup)
}

func (g *Group) StopFilterRefresh() {
	g.filter.stopRefresh()
}
