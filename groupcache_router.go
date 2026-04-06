package cache

import (
	"sync/atomic"
	"time"
)

func (g *Group) Mode() CacheMode {
	g.routeMu.RLock()
	defer g.routeMu.RUnlock()
	return g.mode
}

func (g *Group) SwitchToSharded(shards uint32) error {
	if g.Mode() == CacheModeSharded {
		return nil
	}
	if shards == 0 {
		shards = atomic.LoadUint32(&g.shardCount)
		if shards == 0 {
			shards = 256
		}
	}
	atomic.StoreUint32(&g.shardCount, shards)
	perShardBytes := g.cacheBytes / int64(shards)
	if perShardBytes <= 0 {
		perShardBytes = 1
	}
	newCache := newShardedCache(0, shards, perShardBytes, g.onEvicted)
	return g.switchTo(newCache, CacheModeSharded)
}

func (g *Group) SwitchToUnsharded() error {
	if g.Mode() == CacheModeUnsharded {
		return nil
	}
	newCache := &cache{cacheBytes: g.cacheBytes, onEvicted: g.onEvicted}
	return g.switchTo(newCache, CacheModeUnsharded)
}

func (g *Group) switchTo(newCache Cache, mode CacheMode) error {
	g.routeMu.Lock()
	defer g.routeMu.Unlock()

	if g.mode == mode {
		return nil
	}
	if g.switchCooldown > 0 && !g.switchedAt.IsZero() && time.Since(g.switchedAt) < g.switchCooldown {
		Stats.IncSwitchSkippedCooldown()
		return ErrSwitchCooldown
	}

	g.applySwitchLocked(newCache, mode)
	if mode == CacheModeSharded {
		Stats.IncSwitchToSharded()
	} else {
		Stats.IncSwitchToUnsharded()
	}
	return nil
}

func (g *Group) EvaluateAutoSwitchOnce(missRate float64) error {
	g.routeMu.Lock()
	defer g.routeMu.Unlock()

	policy := g.autoSwitchPolicy
	if !policy.Enable {
		return nil
	}
	if policy.HighConsecutive <= 0 {
		policy.HighConsecutive = 1
	}
	if policy.LowConsecutive <= 0 {
		policy.LowConsecutive = 1
	}
	if policy.MissRateLow > policy.MissRateHigh {
		policy.MissRateLow = policy.MissRateHigh
	}

	if g.mode == CacheModeUnsharded {
		if missRate >= policy.MissRateHigh {
			g.highStreak++
		} else {
			g.highStreak = 0
		}
		g.lowStreak = 0
		if g.highStreak < policy.HighConsecutive {
			return nil
		}
		if g.switchCooldown > 0 && !g.switchedAt.IsZero() && time.Since(g.switchedAt) < g.switchCooldown {
			Stats.IncSwitchSkippedCooldown()
			return ErrSwitchCooldown
		}

		shards := atomic.LoadUint32(&g.shardCount)
		if shards == 0 {
			shards = 256
		}
		perShardBytes := g.cacheBytes / int64(shards)
		if perShardBytes <= 0 {
			perShardBytes = 1
		}
		g.applySwitchLocked(newShardedCache(0, shards, perShardBytes, g.onEvicted), CacheModeSharded)
		Stats.IncSwitchToSharded()
		return nil
	}

	if missRate <= policy.MissRateLow {
		g.lowStreak++
	} else {
		g.lowStreak = 0
	}
	g.highStreak = 0
	if g.lowStreak < policy.LowConsecutive {
		return nil
	}
	if g.switchCooldown > 0 && !g.switchedAt.IsZero() && time.Since(g.switchedAt) < g.switchCooldown {
		Stats.IncSwitchSkippedCooldown()
		return ErrSwitchCooldown
	}

	g.applySwitchLocked(&cache{cacheBytes: g.cacheBytes, onEvicted: g.onEvicted}, CacheModeUnsharded)
	Stats.IncSwitchToUnsharded()
	return nil
}

func (g *Group) applySwitchLocked(newCache Cache, mode CacheMode) {
	oldActive := g.activeCache
	g.activeCache = newCache
	g.fallbackCache = oldActive
	g.fallbackEpoch = g.activeEpoch
	g.activeEpoch++
	if g.activeEpoch == 0 {
		g.activeEpoch = 1
	}
	g.mode = mode
	g.switchedAt = time.Now()
	g.highStreak = 0
	g.lowStreak = 0
}

func (g *Group) StartAutoSwitchController(interval time.Duration, sampleMissRate func() float64) {
	if interval <= 0 || sampleMissRate == nil {
		return
	}
	g.routeMu.Lock()
	if g.controllerStop != nil {
		g.routeMu.Unlock()
		return
	}
	stop := make(chan struct{})
	g.controllerStop = stop
	g.routeMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = g.EvaluateAutoSwitchOnce(sampleMissRate())
				g.CleanupFallback()
			case <-stop:
				return
			}
		}
	}()
}

func (g *Group) StopAutoSwitchController() {
	g.routeMu.Lock()
	stop := g.controllerStop
	g.controllerStop = nil
	g.routeMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
}

func (g *Group) CleanupFallback() {
	g.routeMu.Lock()
	defer g.routeMu.Unlock()
	if g.fallbackCache == nil {
		return
	}
	if g.fallbackTTL <= 0 {
		g.fallbackCache = nil
		return
	}
	if !g.switchedAt.IsZero() && time.Since(g.switchedAt) > g.fallbackTTL {
		g.fallbackCache = nil
	}
}
