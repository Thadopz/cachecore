package cache

import (
	"fmt"
	"goCache/singleflight"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "goCache/groupcachepb"
)

type Group struct {
	// name is the name of this group, must be unique and non-empty
	name string

	// getter is called when a key is not found in the cache.
	// It should return the data corresponding to the key.
	getter Getter

	// activeCache serves all new reads/writes.
	activeCache Cache

	// fallbackCache is the previous cache kept for a short grace window after switch.
	fallbackCache Cache

	// routeMu protects active/fallback cache swapping and mode metadata.
	routeMu sync.RWMutex

	// mode records current cache mode.
	mode CacheMode

	// switchedAt tracks when the last cache mode switch happened.
	switchedAt time.Time

	// fallbackTTL controls how long fallbackCache can serve misses.
	fallbackTTL time.Duration

	// switchCooldown prevents rapid mode flapping.
	switchCooldown time.Duration

	// cacheBytes is the target capacity for newly created cache instance.
	cacheBytes int64

	// onEvicted callback is reused when creating new cache instances.
	onEvicted func(key string, value ByteView)

	// shardCount is used when switching to sharded mode.
	shardCount uint32

	// autoSwitchPolicy controls threshold-based dynamic switching.
	autoSwitchPolicy AutoSwitchPolicy

	// highStreak/lowStreak count consecutive intervals that satisfy thresholds.
	highStreak int
	lowStreak  int

	// activeEpoch labels the current cache generation.
	activeEpoch uint64

	// fallbackEpoch labels the previous cache generation during a grace window.
	fallbackEpoch uint64

	// controllerStop stops the background auto switch controller.
	controllerStop chan struct{}

	// peers is used to pick a peer to get the value for a key.
	// It is set by RegisterPeers and should not be nil after that.
	peers PeerPicker

	// loader is used to ensure that each key is only fetched once
	// even if there are concurrent requests for the same key.
	loader *singleflight.Group

	// singleflightWaitTTL bounds how long a caller waits for an inflight load.
	// <= 0 keeps the original behavior (wait until completion).
	singleflightWaitTTL time.Duration

	// filter is used to track which keys are present in the cache,
	// to avoid unnecessary calls to the getter.
	// it is optional and can be nil if not needed.
	filter      Filter
	filterReady atomic.Bool

	// janitor is used to periodically clean up expired items from the cache.
	// it is optional and can be nil if not needed.
	janitor *Janitor

	// cacheTTL is the base TTL for cached values. 0 means no expiration.
	cacheTTL time.Duration

	// cacheTTLJitter adds random jitter in range [-cacheTTLJitter, +cacheTTLJitter]
	// to avoid synchronized expirations.
	cacheTTLJitter time.Duration
}

func (g *Group) Log(format string, v ...interface{}) {
	log.Printf("[Group %s] %s", g.name, fmt.Sprintf(format, v...))
}

func (g *Group) Get(key string) (ByteView, error) {
	start := time.Now()
	defer func() {
		Stats.RecordGroupGetLatency(time.Since(start))
	}()

	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	Stats.IncGroupGets()
	active, fallback := g.currentCaches()
	if active != nil {
		if v, ok := active.get(key); ok {
			Stats.IncCacheHits()
			return v, nil
		}
	}
	if fallback != nil {
		if v, ok := fallback.get(key); ok {
			Stats.IncCacheHits()
			if g.shouldRefillFromFallback(v) {
				g.tryRefillActiveFromFallback(key, v)
			}
			return v, nil
		}
	}
	Stats.IncCacheMisses()
	return g.load(key)
}

func (g *Group) currentCaches() (active Cache, fallback Cache) {
	g.routeMu.RLock()
	defer g.routeMu.RUnlock()
	active = g.activeCache
	if g.fallbackCache == nil {
		return active, nil
	}
	if g.fallbackTTL <= 0 {
		return active, nil
	}
	if g.fallbackTTL > 0 && !g.switchedAt.IsZero() && time.Since(g.switchedAt) > g.fallbackTTL {
		return active, nil
	}
	return active, g.fallbackCache
}

// if mainCache doesn't have the key, it should return an error, so that the getter can fallback to getFromPeer
func (g *Group) load(key string) (value ByteView, err error) {
	loadFn := func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err := g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				g.Log("Failed to get from peer %v: %v", peer, err)
			}
		}
		return g.getLocally(key)
	}

	if g.singleflightWaitTTL <= 0 {
		v, err := g.loader.Do(key, loadFn)
		if err == nil {
			value = v.(ByteView)
			return value, nil
		}
		return ByteView{}, err
	}

	resCh := g.loader.DoChan(key, loadFn)
	timer := time.NewTimer(g.singleflightWaitTTL)
	defer timer.Stop()

	select {
	case res := <-resCh:
		if res.Err != nil {
			return ByteView{}, res.Err
		}
		return res.Val.(ByteView), nil
	case <-timer.C:
		if v, ok := g.degradeFromCaches(key); ok {
			g.Log("singleflight wait timeout key=%s, fallback served", key)
			return v, nil
		}
		g.Log("singleflight wait timeout key=%s, no fallback", key)
		return ByteView{}, ErrSingleflightWaitTimeout
	}
}

func (g *Group) degradeFromCaches(key string) (ByteView, bool) {
	active, fallback := g.currentCaches()
	if active != nil {
		if v, ok := active.get(key); ok {
			Stats.IncCacheHits()
			return v, true
		}
	}
	if fallback != nil {
		if v, ok := fallback.get(key); ok {
			Stats.IncCacheHits()
			if g.shouldRefillFromFallback(v) {
				g.tryRefillActiveFromFallback(key, v)
			}
			return v, true
		}
	}
	return ByteView{}, false
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	Stats.IncPeerLoads()
	request := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	response := &pb.Response{}
	err := peer.Get(request, response)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: response.Value}, nil
}

// if peer doesn't have the key, it should return an error, so that the getter can fallback to getLocally
func (g *Group) getLocally(key string) (ByteView, error) {
	if g.filter != nil && g.filterReady.Load() && !g.filter.Contains(key) {
		Stats.IncFilterMisses()
		return ByteView{}, fmt.Errorf("%w key %s not found in filter", ErrFilterNotFound, key)
	}
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	Stats.IncLocalLoads()
	value := g.stampActiveValue(cloneBytes(bytes))
	ttl := g.nextTTL()
	active, _ := g.currentCaches()
	if active == nil {
		return value, nil
	}
	if ttl > 0 {
		active.addWithTTL(key, value, ttl)
	} else {
		active.add(key, value)
	}
	if g.filter != nil {
		g.filter.Add(key)
	}
	return value, nil
}

func (g *Group) nextTTL() time.Duration {
	if g.cacheTTL <= 0 {
		return 0
	}
	if g.cacheTTLJitter <= 0 {
		return g.cacheTTL
	}

	rangeN := int64(g.cacheTTLJitter)*2 + 1
	delta := time.Duration(rand.Int63n(rangeN)) - g.cacheTTLJitter
	ttl := g.cacheTTL + delta
	if ttl <= 0 {
		return time.Nanosecond
	}
	return ttl
}

func (g *Group) getLocallyOnlyForWarmUp(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	if g.filter != nil {
		g.filter.Add(key)
	}
	return value, nil
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) Warmup(keys []string) {
	if g.filter == nil {
		g.Log("No filter set, skipping warmup")
		return
	}
	mark := true
	for _, key := range keys {
		if _, err := g.getLocallyOnlyForWarmUp(key); err != nil {
			mark = false
			g.Log("Failed to warmup key %s: %v", key, err)
		}
	}
	if mark {
		g.filterReady.Store(true)
		g.Log("Warmup completed successfully")
	}
}

func (g *Group) Invalidate(key string) {
	g.markInvalidated(key)
	active, fallback := g.currentCaches()
	if active != nil {
		active.remove(key)
	}
	if fallback != nil {
		fallback.remove(key)
	}
	if g.filter != nil {
		g.filter.Remove(key)
	}
}
