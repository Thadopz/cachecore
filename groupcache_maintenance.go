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

func (g *Group) StartFilterRefresh(interval time.Duration) {
	resettable, ok := g.filter.(ResettableFilter)
	if !ok || interval <= 0 {
		return
	}

	g.filterWarmupMu.Lock()
	if g.filterRefreshStop != nil {
		g.filterWarmupMu.Unlock()
		return
	}
	stop := make(chan struct{})
	g.filterRefreshStop = stop
	g.filterWarmupMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				keys := g.filterKeysForRefresh()
				if len(keys) == 0 {
					continue
				}
				g.filterMu.Lock()
				g.filterReady.Store(false)
				resettable.Reset()
				g.filterMu.Unlock()
				g.Warmup(keys)
			case <-stop:
				return
			}
		}
	}()
}

func (g *Group) StopFilterRefresh() {
	g.filterWarmupMu.Lock()
	stop := g.filterRefreshStop
	g.filterRefreshStop = nil
	g.filterWarmupMu.Unlock()
	if stop != nil {
		close(stop)
	}
}

func (g *Group) filterKeysForRefresh() []string {
	g.filterWarmupMu.Lock()
	defer g.filterWarmupMu.Unlock()
	return cloneStrings(g.filterWarmupKeys)
}
