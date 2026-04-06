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
			active, fallback := g.currentCaches()
			if active != nil {
				active.clearupExpired()
			}
			if fallback != nil {
				fallback.clearupExpired()
			}
			g.CleanupFallback()
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
