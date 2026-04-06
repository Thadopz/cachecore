package cache

func (g *Group) stampActiveValue(b []byte) ByteView {
	g.routeMu.RLock()
	epoch := g.activeEpoch
	g.routeMu.RUnlock()
	return ByteView{b: b, epoch: epoch}
}

func (g *Group) shouldRefillFromFallback(value ByteView) bool {
	g.routeMu.RLock()
	defer g.routeMu.RUnlock()
	return g.fallbackCache != nil && value.epoch != 0 && value.epoch == g.fallbackEpoch
}

func (g *Group) tryRefillActiveFromFallback(key string, value ByteView) {
	g.routeMu.RLock()
	active := g.activeCache
	activeEpoch := g.activeEpoch
	g.routeMu.RUnlock()
	if active == nil {
		return
	}
	refill := value.withEpoch(activeEpoch)
	active.add(key, refill)
}

func (g *Group) markInvalidated(key string) {
	_ = key
}
