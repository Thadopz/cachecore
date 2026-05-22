package cache

func (g *Group) stampActiveValue(b []byte) ByteView {
	g.router.mu.RLock()
	epoch := g.router.activeEpoch
	g.router.mu.RUnlock()
	return ByteView{b: b, epoch: epoch}
}

func (g *Group) shouldRefillFromFallback(value ByteView) bool {
	g.router.mu.RLock()
	defer g.router.mu.RUnlock()
	return g.router.fallback != nil && value.epoch != 0 && value.epoch == g.router.fallbackEpoch
}

func (g *Group) tryRefillActiveFromFallback(key string, value ByteView) {
	g.router.mu.RLock()
	active := g.router.active
	activeEpoch := g.router.activeEpoch
	g.router.mu.RUnlock()
	if active == nil {
		return
	}
	refill := value.withEpoch(activeEpoch)
	active.add(key, refill)
}
