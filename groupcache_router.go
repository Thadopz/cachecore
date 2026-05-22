package cache

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type cacheRouter struct {
	mu sync.RWMutex

	active   Cache
	fallback Cache
	mode     CacheMode

	switchedAt     time.Time
	fallbackTTL    time.Duration
	switchCooldown time.Duration

	cacheBytes int64
	onEvicted  onEvictedFunc
	shardCount uint32

	autoSwitchPolicy AutoSwitchPolicy
	highStreak       int
	lowStreak        int

	activeEpoch   uint64
	fallbackEpoch uint64

	controllerStop chan struct{}
}

func newCacheRouter(active Cache, mode CacheMode, fallbackTTL, switchCooldown time.Duration, cacheBytes int64, onEvicted onEvictedFunc, shardCount uint32, autoPolicy AutoSwitchPolicy) *cacheRouter {
	if shardCount == 0 {
		shardCount = 256
	}
	return &cacheRouter{
		active:           active,
		mode:             mode,
		fallbackTTL:      fallbackTTL,
		switchCooldown:   switchCooldown,
		cacheBytes:       cacheBytes,
		onEvicted:        onEvicted,
		shardCount:       shardCount,
		autoSwitchPolicy: autoPolicy,
		activeEpoch:      1,
	}
}

func (r *cacheRouter) currentCaches() (active Cache, fallback Cache) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	active = r.active
	if r.fallback == nil {
		return active, nil
	}
	if r.fallbackTTL <= 0 {
		return active, nil
	}
	if !r.switchedAt.IsZero() && time.Since(r.switchedAt) > r.fallbackTTL {
		return active, nil
	}
	return active, r.fallback
}

func (r *cacheRouter) modeSnapshot() CacheMode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mode
}

func (r *cacheRouter) switchToSharded(shards uint32) error {
	if r.modeSnapshot() == CacheModeSharded {
		return nil
	}
	if shards == 0 {
		shards = atomic.LoadUint32(&r.shardCount)
		if shards == 0 {
			shards = 256
		}
	}
	atomic.StoreUint32(&r.shardCount, shards)
	newCache := newShardedCache(0, shards, r.perShardBytes(shards), r.onEvicted)
	return r.switchTo(newCache, CacheModeSharded)
}

func (r *cacheRouter) switchToUnsharded() error {
	if r.modeSnapshot() == CacheModeUnsharded {
		return nil
	}
	newCache := &cache{cacheBytes: r.cacheBytes, onEvicted: r.onEvicted}
	return r.switchTo(newCache, CacheModeUnsharded)
}

func (r *cacheRouter) switchTo(newCache Cache, mode CacheMode) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mode == mode {
		return nil
	}
	if r.inCooldownLocked() {
		Stats.IncSwitchSkippedCooldown()
		return ErrSwitchCooldown
	}

	r.applySwitchLocked(newCache, mode)
	if mode == CacheModeSharded {
		Stats.IncSwitchToSharded()
	} else {
		Stats.IncSwitchToUnsharded()
	}
	return nil
}

func (r *cacheRouter) evaluateAutoSwitchOnce(missRate float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if missRate < 0 || math.IsNaN(missRate) || math.IsInf(missRate, 0) {
		return nil
	}

	policy := r.autoSwitchPolicy
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

	if r.mode == CacheModeUnsharded {
		if missRate >= policy.MissRateHigh {
			r.highStreak++
		} else {
			r.highStreak = 0
		}
		r.lowStreak = 0
		if r.highStreak < policy.HighConsecutive {
			return nil
		}
		if r.inCooldownLocked() {
			Stats.IncSwitchSkippedCooldown()
			return ErrSwitchCooldown
		}

		shards := atomic.LoadUint32(&r.shardCount)
		if shards == 0 {
			shards = 256
		}
		r.applySwitchLocked(newShardedCache(0, shards, r.perShardBytes(shards), r.onEvicted), CacheModeSharded)
		Stats.IncSwitchToSharded()
		return nil
	}

	if missRate <= policy.MissRateLow {
		r.lowStreak++
	} else {
		r.lowStreak = 0
	}
	r.highStreak = 0
	if r.lowStreak < policy.LowConsecutive {
		return nil
	}
	if r.inCooldownLocked() {
		Stats.IncSwitchSkippedCooldown()
		return ErrSwitchCooldown
	}

	r.applySwitchLocked(&cache{cacheBytes: r.cacheBytes, onEvicted: r.onEvicted}, CacheModeUnsharded)
	Stats.IncSwitchToUnsharded()
	return nil
}

func (r *cacheRouter) perShardBytes(shards uint32) int64 {
	perShardBytes := r.cacheBytes / int64(shards)
	if perShardBytes <= 0 {
		return 1
	}
	return perShardBytes
}

func (r *cacheRouter) inCooldownLocked() bool {
	return r.switchCooldown > 0 && !r.switchedAt.IsZero() && time.Since(r.switchedAt) < r.switchCooldown
}

func (r *cacheRouter) applySwitchLocked(newCache Cache, mode CacheMode) {
	oldActive := r.active
	r.active = newCache
	r.fallback = oldActive
	r.fallbackEpoch = r.activeEpoch
	r.activeEpoch++
	if r.activeEpoch == 0 {
		r.activeEpoch = 1
	}
	r.mode = mode
	r.switchedAt = time.Now()
	r.highStreak = 0
	r.lowStreak = 0
}

func (r *cacheRouter) startAutoSwitchController(interval time.Duration, sampleMissRate func() float64) {
	if interval <= 0 || sampleMissRate == nil {
		return
	}
	r.mu.Lock()
	if r.controllerStop != nil {
		r.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	r.controllerStop = stop
	r.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = r.evaluateAutoSwitchOnce(sampleMissRate())
				r.cleanupFallback()
			case <-stop:
				return
			}
		}
	}()
}

func (r *cacheRouter) stopAutoSwitchController() {
	r.mu.Lock()
	stop := r.controllerStop
	r.controllerStop = nil
	r.mu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
}

func (r *cacheRouter) cleanupFallback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fallback == nil {
		return
	}
	if r.fallbackTTL <= 0 {
		r.fallback = nil
		return
	}
	if !r.switchedAt.IsZero() && time.Since(r.switchedAt) > r.fallbackTTL {
		r.fallback = nil
	}
}

func (g *Group) Mode() CacheMode {
	return g.router.modeSnapshot()
}

func (g *Group) SwitchToSharded(shards uint32) error {
	return g.router.switchToSharded(shards)
}

func (g *Group) SwitchToUnsharded() error {
	return g.router.switchToUnsharded()
}

func (g *Group) EvaluateAutoSwitchOnce(missRate float64) error {
	return g.router.evaluateAutoSwitchOnce(missRate)
}

func (g *Group) StartAutoSwitchController(interval time.Duration, sampleMissRate func() float64) {
	g.router.startAutoSwitchController(interval, sampleMissRate)
}

func (g *Group) StopAutoSwitchController() {
	g.router.stopAutoSwitchController()
}

func (g *Group) CleanupFallback() {
	g.router.cleanupFallback()
}
