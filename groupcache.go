package cache

import (
	"errors"
	"fmt"
	"goCache/singleflight"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "goCache/groupcachepb"
)

type Getter interface {
	Get(key string) ([]byte, error)
}

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

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

	// controllerStop stops the background auto switch controller.
	controllerStop chan struct{}

	// peers is used to pick a peer to get the value for a key.
	// It is set by RegisterPeers and should not be nil after that.
	peers PeerPicker

	// loader is used to ensure that each key is only fetched once
	// even if there are concurrent requests for the same key.
	loader *singleflight.Group

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

type Filter interface {
	// Add adds an item to the filter.
	Add(item string)

	// Contains checks if an item is in the filter.
	// It should return true if the item is probably in the filter,
	// and false if it is definitely not in the filter.
	Contains(item string) bool

	// Remove removes an item from the filter.
	// It should ensure that the item is no longer considered to be in the filter.
	Remove(item string)
}

type Option func(*Options)

type CacheMode int

const (
	CacheModeUnsharded CacheMode = iota
	CacheModeSharded
)

type AutoSwitchPolicy struct {
	Enable bool

	// MissRateHigh triggers switch to sharded mode after HighConsecutive windows.
	MissRateHigh float64

	// MissRateLow triggers switch to unsharded mode after LowConsecutive windows.
	MissRateLow float64

	// HighConsecutive / LowConsecutive define required consecutive intervals.
	HighConsecutive int
	LowConsecutive  int
}

var (
	ErrFilterNotFound = errors.New("key not found in filter")
	ErrSwitchCooldown = errors.New("cache mode switch is in cooldown")
	mu                sync.RWMutex
	groups            = make(map[string]*Group)
)

type Options struct {
	Filter         Filter
	Janitor        *Janitor
	onEvicted      func(key string, value ByteView)
	cache          Cache
	useShards      bool
	shards         uint32
	cacheTTL       time.Duration
	ttlJitter      time.Duration
	fallbackTTL    time.Duration
	cooldown       time.Duration
	fallbackTTLSet bool
	cooldownSet    bool
	autoPolicy     AutoSwitchPolicy
}

func WithFilter(filter Filter) Option {
	// WithFilter returns an Option that sets the filter for a Group.
	return func(o *Options) {
		o.Filter = filter
	}
}

func WithJanitor(interval time.Duration) Option {
	// WithJanitor returns an Option that sets the janitor for a Group.
	return func(o *Options) {
		o.Janitor = NewJanitor(interval)
	}
}

func WithOnEvicted(onEvicted func(key string, value ByteView)) Option {
	// WithOnEvicted returns an Option that sets the onEvicted callback for a Group's cache.
	return func(o *Options) {
		o.onEvicted = onEvicted
	}
}

func WithCache(cache Cache) Option {
	// WithCache injects a custom cache implementation.
	// It takes precedence over WithShardedCache.
	return func(o *Options) {
		o.cache = cache
	}
}

func WithShardedCache(shards uint32) Option {
	// WithShardedCache enables sharded cache initialization in NewGroup.
	return func(o *Options) {
		o.useShards = true
		o.shards = shards
	}
}

func WithRandomTTL(baseTTL, jitter time.Duration) Option {
	// WithRandomTTL sets a base TTL and random jitter for cache entries.
	// Effective TTL will be baseTTL + random(-jitter, +jitter), and will be clamped to > 0.
	return func(o *Options) {
		if baseTTL <= 0 {
			o.cacheTTL = 0
			o.ttlJitter = 0
			return
		}
		if jitter < 0 {
			jitter = -jitter
		}
		o.cacheTTL = baseTTL
		o.ttlJitter = jitter
	}
}

func WithFallbackTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.fallbackTTL = ttl
		o.fallbackTTLSet = true
	}
}

func WithSwitchCooldown(cooldown time.Duration) Option {
	return func(o *Options) {
		o.cooldown = cooldown
		o.cooldownSet = true
	}
}

func WithAutoSwitchByMissRate(policy AutoSwitchPolicy) Option {
	return func(o *Options) {
		o.autoPolicy = policy
	}
}

func NewGroup(name string, cacheBytes int64, getter Getter, opts ...Option) *Group {
	if name == "" {
		panic("group name is required")
	}
	if getter == nil {
		panic("nil Getter")
	}
	options := &Options{}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	mainCache := options.cache
	if mainCache == nil {
		if options.useShards {
			shards := options.shards
			if shards == 0 {
				shards = 256
			}
			perShardBytes := cacheBytes / int64(shards)
			if perShardBytes <= 0 {
				perShardBytes = 1
			}
			mainCache = newShardedCache(0, shards, perShardBytes, options.onEvicted)
		} else {
			mainCache = &cache{cacheBytes: cacheBytes, onEvicted: options.onEvicted}
		}
	}

	fallbackTTL := options.fallbackTTL
	if !options.fallbackTTLSet {
		fallbackTTL = 90 * time.Second
	}

	cooldown := options.cooldown
	if !options.cooldownSet {
		cooldown = 30 * time.Second
	}

	mode := CacheModeUnsharded
	if options.useShards {
		mode = CacheModeSharded
	}

	shardCount := options.shards
	if shardCount == 0 {
		shardCount = 256
	}

	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:             name,
		getter:           getter,
		activeCache:      mainCache,
		loader:           &singleflight.Group{},
		filter:           options.Filter,
		janitor:          options.Janitor,
		cacheTTL:         options.cacheTTL,
		cacheTTLJitter:   options.ttlJitter,
		mode:             mode,
		fallbackTTL:      fallbackTTL,
		switchCooldown:   cooldown,
		cacheBytes:       cacheBytes,
		onEvicted:        options.onEvicted,
		shardCount:       shardCount,
		autoSwitchPolicy: options.autoPolicy,
	}
	if g.janitor != nil {
		go g.janitor.Run(g)
	}
	groups[name] = g
	return g
}

func NewGroupWithFilter(name string, cacheBytes int64, getter Getter, filter Filter) *Group {
	if name == "" {
		panic("group name is required")
	}
	if filter == nil {
		panic("nil Filter")
	}
	return NewGroup(name, cacheBytes, getter, WithFilter(filter))
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
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
			if active != nil {
				ttl := g.nextTTL()
				if ttl > 0 {
					active.addWithTTL(key, v, ttl)
				} else {
					active.add(key, v)
				}
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

func (g *Group) Mode() CacheMode {
	g.routeMu.RLock()
	defer g.routeMu.RUnlock()
	return g.mode
}

func (g *Group) SwitchToSharded(shards uint32) error {
	if shards == 0 {
		shards = g.shardCount
		if shards == 0 {
			shards = 256
		}
	}
	g.shardCount = shards
	perShardBytes := g.cacheBytes / int64(shards)
	if perShardBytes <= 0 {
		perShardBytes = 1
	}
	newCache := newShardedCache(0, shards, perShardBytes, g.onEvicted)
	return g.switchTo(newCache, CacheModeSharded)
}

func (g *Group) SwitchToUnsharded() error {
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

	oldActive := g.activeCache
	g.activeCache = newCache
	g.fallbackCache = oldActive
	g.mode = mode
	g.switchedAt = time.Now()
	g.highStreak = 0
	g.lowStreak = 0
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

		shards := g.shardCount
		if shards == 0 {
			shards = 256
		}
		perShardBytes := g.cacheBytes / int64(shards)
		if perShardBytes <= 0 {
			perShardBytes = 1
		}
		oldActive := g.activeCache
		g.activeCache = newShardedCache(0, shards, perShardBytes, g.onEvicted)
		g.fallbackCache = oldActive
		g.mode = CacheModeSharded
		g.switchedAt = time.Now()
		g.highStreak = 0
		g.lowStreak = 0
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

	oldActive := g.activeCache
	g.activeCache = &cache{cacheBytes: g.cacheBytes, onEvicted: g.onEvicted}
	g.fallbackCache = oldActive
	g.mode = CacheModeUnsharded
	g.switchedAt = time.Now()
	g.highStreak = 0
	g.lowStreak = 0
	Stats.IncSwitchToUnsharded()
	return nil
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

// if mainCache doesn't have the key, it should return an error, so that the getter can fallback to getFromPeer
func (g *Group) load(key string) (value ByteView, err error) {
	v, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err := g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				g.Log("Failed to get from peer %v: %v", peer, err)
			}
		}
		return g.getLocally(key)
	})
	if err == nil {
		value = v.(ByteView)
		return value, nil
	}
	return
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
	value := ByteView{b: cloneBytes(bytes)}
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
