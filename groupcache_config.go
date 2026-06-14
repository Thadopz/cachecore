package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Thadopz/cachecore/internal/singleflight"
)

// Getter loads bytes for a key that is missing from cache.
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc adapts a function to the Getter interface.
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get calls f(ctx, key).
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// Incrementer applies atomic numeric increments to a backing store.
type Incrementer interface {
	Increment(ctx context.Context, key string, delta int64) (int64, error)
}

// IncrementerFunc adapts a function to the Incrementer interface.
type IncrementerFunc func(ctx context.Context, key string, delta int64) (int64, error)

// Increment calls f(ctx, key, delta).
func (f IncrementerFunc) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return f(ctx, key, delta)
}

// Filter describes a probabilistic membership filter.
type Filter interface {
	// Add adds an item to the filter.
	Add(item string)

	// Contains checks if an item is in the filter.
	// It should return true if the item is probably in the filter,
	// and false if it is definitely not in the filter.
	Contains(item string) bool
}

// ResettableFilter extends Filter with a full reset operation.
type ResettableFilter interface {
	Filter
	// Reset clears accumulated keys before a warmup-key rebuild.
	Reset()
}

// Option configures a Group.
type Option func(*Options)

var (
	// ErrFilterNotFound reports that a key was rejected by the filter.
	ErrFilterNotFound = errors.New("key not found in filter")
	// ErrNotFound reports that a key is absent from the backing store.
	ErrNotFound = errors.New("key not found")
	// ErrNonNumericValue reports that a cached value cannot be parsed as an integer.
	ErrNonNumericValue = errors.New("cache value is not numeric")
	// ErrIncrementUnsupported reports that a peer cannot apply increments.
	ErrIncrementUnsupported = errors.New("peer does not support increment")
	// ErrSingleflightWaitTimeout reports that a caller timed out waiting for a shared load.
	ErrSingleflightWaitTimeout = errors.New("singleflight wait timeout")
	mu                         sync.RWMutex
	groups                     = make(map[string]*Group)
)

// Options contains Group configuration values.
type Options struct {
	Filter               Filter
	incrementer          Incrementer
	Janitor              *Janitor
	onEvicted            func(key string, value ByteView)
	cache                Cache
	useShards            bool
	useSLRU              bool
	shards               uint32
	slruProtectedRatio   float64
	cacheTTL             time.Duration
	ttlJitter            time.Duration
	staleTTL             time.Duration
	loadWaitTTL          time.Duration
	negativeTTL          time.Duration
	loadWaitTTLSet       bool
	negativeTTLSet       bool
	negativeCacheEnabled bool
	filterRefresh        time.Duration
}

// WithFilter installs a membership filter for cache misses.
func WithFilter(filter Filter) Option {
	return func(o *Options) {
		o.Filter = filter
	}
}

// WithFilterRefresh sets the background filter refresh interval.
func WithFilterRefresh(interval time.Duration) Option {
	return func(o *Options) {
		o.filterRefresh = interval
	}
}

// WithIncrementer installs a durable increment backend.
func WithIncrementer(incrementer Incrementer) Option {
	return func(o *Options) {
		o.incrementer = incrementer
	}
}

// WithJanitor enables periodic cleanup of expired cache entries.
func WithJanitor(interval time.Duration) Option {
	return func(o *Options) {
		o.Janitor = NewJanitor(interval)
	}
}

// WithOnEvicted registers a callback for cache evictions.
func WithOnEvicted(onEvicted func(key string, value ByteView)) Option {
	return func(o *Options) {
		o.onEvicted = onEvicted
	}
}

// WithCache uses a caller-provided cache implementation.
func WithCache(cache Cache) Option {
	return func(o *Options) {
		o.cache = cache
	}
}

// WithShardedCache enables sharded local cache storage.
func WithShardedCache(shards uint32) Option {
	return func(o *Options) {
		o.useShards = true
		o.shards = shards
	}
}

// WithSLRU enables segmented LRU cache eviction.
func WithSLRU(protectedRatio float64) Option {
	return func(o *Options) {
		o.useSLRU = true
		o.slruProtectedRatio = protectedRatio
	}
}

// WithRandomTTL enables cache expiration with optional jitter.
func WithRandomTTL(baseTTL, jitter time.Duration) Option {
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

// WithSingleflightWaitTTL bounds how long callers wait for shared loads.
func WithSingleflightWaitTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.loadWaitTTL = ttl
		o.loadWaitTTLSet = true
	}
}

// WithStaleWhileRevalidate enables serving stale values during refresh.
func WithStaleWhileRevalidate(ttl time.Duration) Option {
	return func(o *Options) {
		o.staleTTL = ttl
	}
}

// WithNegativeCache enables short-lived caching of not-found results.
func WithNegativeCache(ttl time.Duration) Option {
	return func(o *Options) {
		o.negativeTTL = ttl
		o.negativeTTLSet = true
		o.negativeCacheEnabled = ttl > 0
	}
}

// NewGroup creates a named cache group.
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

	mainCache := newConfiguredCache(cacheBytes, options, options.onEvicted, true)

	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:                 name,
		getter:               getter,
		incrementer:          options.incrementer,
		incrementLocks:       newStripedLocks(defaultIncrementLockStripes),
		mainCache:            mainCache,
		loader:               &singleflight.Group{},
		singleflightWaitTTL:  options.loadWaitTTL,
		negativeTTL:          options.negativeTTL,
		negativeCacheEnabled: options.negativeCacheEnabled,
		filter:               newFilterGate(options.Filter),
		filterRefresh:        options.filterRefresh,
		janitor:              options.Janitor,
		cacheTTL:             options.cacheTTL,
		cacheTTLJitter:       options.ttlJitter,
		staleTTL:             options.staleTTL,
	}
	if g.staleTTL > 0 && g.cacheTTL > 0 {
		g.staleCache = newConfiguredCache(cacheBytes, options, nil, false)
	}
	if g.janitor != nil {
		go g.janitor.Run(g)
	}
	if g.filterRefresh > 0 {
		g.StartFilterRefresh(g.filterRefresh)
	}
	groups[name] = g
	return g
}

func newConfiguredCache(cacheBytes int64, options *Options, onEvicted onEvictedFunc, useProvided bool) Cache {
	if useProvided && options.cache != nil {
		return options.cache
	}
	if options.useShards {
		shards := normalizedShardCount(options.shards)
		perShardBytes := cacheBytes / int64(shards)
		if perShardBytes <= 0 {
			perShardBytes = 1
		}
		return newShardedCache(0, shards, perShardBytes, onEvicted, options.useSLRU, options.slruProtectedRatio)
	}
	return &cache{
		cacheBytes:         cacheBytes,
		onEvicted:          onEvicted,
		useSLRU:            options.useSLRU,
		slruProtectedRatio: options.slruProtectedRatio,
	}
}

func normalizedShardCount(shards uint32) uint32 {
	if shards == 0 {
		return 256
	}
	return shards
}

// NewGroupWithFilter creates a named cache group with a membership filter.
func NewGroupWithFilter(name string, cacheBytes int64, getter Getter, filter Filter) *Group {
	if name == "" {
		panic("group name is required")
	}
	if filter == nil {
		panic("nil Filter")
	}
	return NewGroup(name, cacheBytes, getter, WithFilter(filter))
}

// GetGroup returns the group registered with name.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}
