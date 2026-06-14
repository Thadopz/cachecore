package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	pb "github.com/Thadopz/cachecore/internal/groupcachepb"
	"github.com/Thadopz/cachecore/internal/singleflight"
)

// Group is a named cache namespace.
type Group struct {
	// name is the name of this group, must be unique and non-empty
	name string

	// getter is called when a key is not found in the cache.
	// It should return the data corresponding to the key.
	getter Getter

	// incrementer is called by Increment when a durable backend owns writes.
	// If nil, Increment uses the active cache as an in-memory counter store.
	incrementer    Incrementer
	incrementLocks *stripedLocks

	// mainCache stores this group's local cached values.
	mainCache Cache

	// peers is used to pick a peer to get the value for a key.
	// It is set by RegisterPeers and should not be nil after that.
	peers *HTTPPool

	// loader is used to ensure that each key is only fetched once
	// even if there are concurrent requests for the same key.
	loader *singleflight.Group

	// singleflightWaitTTL bounds how long a caller waits for an inflight load.
	// <= 0 keeps the original behavior (wait until completion).
	singleflightWaitTTL time.Duration

	// filter tracks key membership and owns its warmup/refresh lifecycle.
	filter *filterGate

	// filterRefresh is the configured refresh interval used at startup.
	filterRefresh time.Duration

	// janitor is used to periodically clean up expired items from the cache.
	// it is optional and can be nil if not needed.
	janitor *Janitor

	// cacheTTL is the base TTL for cached values. 0 means no expiration.
	cacheTTL time.Duration

	// cacheTTLJitter adds random jitter in range [-cacheTTLJitter, +cacheTTLJitter]
	// to avoid synchronized expirations.
	cacheTTLJitter time.Duration

	// staleCache keeps successful values beyond their active TTL for timeout
	// fallback while an async singleflight refresh is still running.
	staleCache Cache

	// staleTTL is the extra duration a value can be served after active TTL expiry.
	staleTTL time.Duration

	// negativeCacheEnabled controls whether ErrNotFound is cached for a short TTL.
	negativeCacheEnabled bool

	// negativeTTL is used when writing negative cache entries.
	negativeTTL time.Duration
}

// Log writes a formatted group-scoped log message.
func (g *Group) Log(format string, v ...interface{}) {
	log.Printf("[Group %s] %s", g.name, fmt.Sprintf(format, v...))
}

// Get returns a cached or newly loaded value for key.
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	start := time.Now()
	defer func() {
		Stats.RecordGroupGetLatency(time.Since(start))
	}()

	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	Stats.IncGroupGets()
	active := g.mainCache
	if active != nil {
		if v, ok := active.get(key); ok {
			Stats.IncCacheHits()
			if v.isNotFound() {
				return ByteView{}, ErrNotFound
			}
			return v, nil
		}
	}
	Stats.IncCacheMisses()
	return g.load(ctx, key)
}

// Increment atomically increments the numeric value stored for key.
func (g *Group) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" {
		return 0, fmt.Errorf("key is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if g.peers != nil {
		if peer, ok := g.peers.pickPeer(key); ok {
			return g.incrementFromPeer(ctx, peer, key, delta)
		}
	}
	return g.incrementLocally(ctx, key, delta)
}

func (g *Group) incrementFromPeer(ctx context.Context, peer *httpGetter, key string, delta int64) (int64, error) {
	request := &pb.Request{
		Group: g.name,
		Key:   key,
		Delta: delta,
	}
	response := &pb.Response{}
	if err := peer.Increment(ctx, request, response); err != nil {
		return 0, err
	}
	return parseNumericValue(response.Value)
}

func (g *Group) incrementLocally(ctx context.Context, key string, delta int64) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" {
		return 0, fmt.Errorf("key is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	active := g.mainCache
	if active == nil {
		return 0, ErrIncrementUnsupported
	}

	if g.incrementer == nil {
		ttl := g.nextTTL()
		next, err := active.increment(key, delta, ttl, g.stampActiveValue)
		if err != nil {
			return 0, err
		}
		g.recordStale(key, g.stampActiveValue([]byte(strconv.FormatInt(next, 10))), ttl)
		g.filter.add(key)
		return next, nil
	}

	unlock := g.incrementLocks.lock(key)
	defer unlock()

	active = g.mainCache
	if active == nil {
		return 0, ErrIncrementUnsupported
	}
	next, err := g.incrementer.Increment(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	value := g.stampActiveValue([]byte(strconv.FormatInt(next, 10)))
	ttl := g.nextTTL()
	if ttl > 0 {
		active.addWithTTL(key, value, ttl)
	} else {
		active.add(key, value)
	}
	g.recordStale(key, value, ttl)
	g.filter.add(key)
	return next, nil
}

func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ByteView{}, err
	}
	loadCtx := context.WithoutCancel(ctx)
	loadFn := func() (interface{}, error) {
		if value, ok, err := g.loadFromPeer(loadCtx, key); ok || err != nil {
			return value, err
		}
		return g.getLocally(loadCtx, key)
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
	return g.waitForLoad(ctx, key, resCh)
}

func (g *Group) loadFromPeer(ctx context.Context, key string) (ByteView, bool, error) {
	if g.peers == nil {
		return ByteView{}, false, nil
	}
	peer, ok := g.peers.pickPeer(key)
	if !ok {
		return ByteView{}, false, nil
	}
	value, err := g.getFromPeer(ctx, peer, key)
	if err == nil {
		return value, true, nil
	}
	if errors.Is(err, ErrNotFound) {
		g.cacheNegative(key)
		return ByteView{}, true, ErrNotFound
	}
	g.Log("Failed to get from peer %v: %v", peer, err)
	return ByteView{}, false, nil
}

func (g *Group) cacheNegative(key string) {
	if !g.negativeCacheEnabled || g.negativeTTL <= 0 {
		return
	}
	active := g.mainCache
	if active == nil {
		return
	}
	negativeValue := g.stampActiveValue(nil).withNotFound()
	active.addWithTTL(key, negativeValue, g.negativeTTL)
}

func (g *Group) waitForLoad(ctx context.Context, key string, resCh <-chan singleflight.Result) (ByteView, error) {
	timer := time.NewTimer(g.singleflightWaitTTL)
	defer timer.Stop()

	select {
	case res := <-resCh:
		if res.Err != nil {
			return ByteView{}, res.Err
		}
		return res.Val.(ByteView), nil
	case <-ctx.Done():
		return ByteView{}, ctx.Err()
	case <-timer.C:
		return g.degradeAfterWaitTimeout(key)
	}
}

func (g *Group) degradeAfterWaitTimeout(key string) (ByteView, error) {
	if v, stale, ok := g.degradeFromCaches(key); ok {
		if stale {
			g.Log("singleflight wait timeout key=%s, stale value served", key)
		} else {
			g.Log("singleflight wait timeout key=%s, cached value served", key)
		}
		if v.isNotFound() {
			return ByteView{}, ErrNotFound
		}
		return v, nil
	}
	g.Log("singleflight wait timeout key=%s, no cached value", key)
	return ByteView{}, ErrSingleflightWaitTimeout
}

func (g *Group) degradeFromCaches(key string) (ByteView, bool, bool) {
	active := g.mainCache
	if active != nil {
		if v, ok := active.get(key); ok {
			Stats.IncCacheHits()
			return v, false, true
		}
	}
	if v, ok := g.getStale(key); ok {
		return v, true, true
	}
	return ByteView{}, false, false
}

func (g *Group) getFromPeer(ctx context.Context, peer *httpGetter, key string) (ByteView, error) {
	Stats.IncPeerLoads()
	request := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	response := &pb.Response{}
	err := peer.Get(ctx, request, response)
	if err != nil {
		return ByteView{}, err
	}
	value := g.stampActiveValue(cloneBytes(response.Value))
	ttl := g.nextTTL()
	active := g.mainCache
	if active != nil {
		if ttl > 0 {
			active.addWithTTL(key, value, ttl)
		} else {
			active.add(key, value)
		}
	}
	g.recordStale(key, value, ttl)
	g.filter.add(key)
	return value, nil
}

// if peer doesn't have the key, it should return an error, so that the getter can fallback to getLocally
func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	if g.filterRejects(key) {
		Stats.IncFilterMisses()
		return ByteView{}, fmt.Errorf("%w key %s not found in filter", ErrFilterNotFound, key)
	}
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		if g.negativeCacheEnabled && g.negativeTTL > 0 && errors.Is(err, ErrNotFound) {
			g.cacheNegative(key)
		}
		return ByteView{}, err
	}
	Stats.IncLocalLoads()
	value := g.stampActiveValue(cloneBytes(bytes))
	ttl := g.nextTTL()
	active := g.mainCache
	if active == nil {
		return value, nil
	}
	if ttl > 0 {
		active.addWithTTL(key, value, ttl)
	} else {
		active.add(key, value)
	}
	g.recordStale(key, value, ttl)
	g.filter.add(key)
	return value, nil
}

func (g *Group) filterRejects(key string) bool {
	return g.filter.rejects(key)
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

func (g *Group) recordStale(key string, value ByteView, activeTTL time.Duration) {
	if g.staleCache == nil || g.staleTTL <= 0 || activeTTL <= 0 || value.isNotFound() {
		return
	}
	g.staleCache.addWithTTL(key, value, activeTTL+g.staleTTL)
}

func (g *Group) getStale(key string) (ByteView, bool) {
	if g.staleCache == nil || g.staleTTL <= 0 {
		return ByteView{}, false
	}
	if v, ok := g.staleCache.get(key); ok {
		Stats.IncCacheHits()
		return v, true
	}
	return ByteView{}, false
}

func (g *Group) getLocallyOnlyForWarmUp(ctx context.Context, key string) (ByteView, error) {
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.filter.add(key)
	return value, nil
}

func (g *Group) stampActiveValue(b []byte) ByteView {
	return ByteView{b: b}
}

// RegisterPeers registers the HTTP peer pool used for distributed lookups.
func (g *Group) RegisterPeers(peers *HTTPPool) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
}

// Warmup preloads keys into the configured filter.
func (g *Group) Warmup(keys []string) {
	if g.filter == nil {
		g.Log("No filter set, skipping warmup")
		return
	}
	if g.filter.warmup(keys, func(ctx context.Context, key string) error {
		_, err := g.getLocallyOnlyForWarmUp(ctx, key)
		if err != nil {
			g.Log("Failed to warmup key %s: %v", key, err)
		}
		return err
	}) {
		g.Log("Warmup completed successfully")
	}
}

// Invalidate removes key locally and asks peers to remove it as well.
func (g *Group) Invalidate(key string) {
	g.invalidateLocal(key)
	if g.peers == nil {
		return
	}
	if err := g.peers.broadcastInvalidate(&pb.Request{Group: g.name, Key: key}); err != nil {
		g.Log("broadcast invalidate key=%s failed: %v", key, err)
	}
}

func (g *Group) invalidateLocal(key string) {
	active := g.mainCache
	if active != nil {
		active.remove(key)
	}
	if g.staleCache != nil {
		g.staleCache.remove(key)
	}
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}
