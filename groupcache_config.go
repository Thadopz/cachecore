package cache

import (
	"context"
	"errors"
	"github.com/Thadopz/cachecore/internal/singleflight"
	"sync"
	"time"
)

type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type Incrementer interface {
	Increment(ctx context.Context, key string, delta int64) (int64, error)
}

type IncrementerFunc func(ctx context.Context, key string, delta int64) (int64, error)

func (f IncrementerFunc) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return f(ctx, key, delta)
}

type Filter interface {
	// Add adds an item to the filter.
	Add(item string)

	// Contains checks if an item is in the filter.
	// It should return true if the item is probably in the filter,
	// and false if it is definitely not in the filter.
	Contains(item string) bool
}

type ResettableFilter interface {
	Filter
	// Reset clears accumulated keys before a warmup-key rebuild.
	Reset()
}

type Option func(*Options)

var (
	ErrFilterNotFound          = errors.New("key not found in filter")
	ErrNotFound                = errors.New("key not found")
	ErrNonNumericValue         = errors.New("cache value is not numeric")
	ErrIncrementUnsupported    = errors.New("peer does not support increment")
	ErrSingleflightWaitTimeout = errors.New("singleflight wait timeout")
	mu                         sync.RWMutex
	groups                     = make(map[string]*Group)
)

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

func WithFilter(filter Filter) Option {
	return func(o *Options) {
		o.Filter = filter
	}
}

func WithFilterRefresh(interval time.Duration) Option {
	return func(o *Options) {
		o.filterRefresh = interval
	}
}

func WithIncrementer(incrementer Incrementer) Option {
	return func(o *Options) {
		o.incrementer = incrementer
	}
}

func WithJanitor(interval time.Duration) Option {
	return func(o *Options) {
		o.Janitor = NewJanitor(interval)
	}
}

func WithOnEvicted(onEvicted func(key string, value ByteView)) Option {
	return func(o *Options) {
		o.onEvicted = onEvicted
	}
}

func WithCache(cache Cache) Option {
	return func(o *Options) {
		o.cache = cache
	}
}

func WithShardedCache(shards uint32) Option {
	return func(o *Options) {
		o.useShards = true
		o.shards = shards
	}
}

func WithSLRU(protectedRatio float64) Option {
	return func(o *Options) {
		o.useSLRU = true
		o.slruProtectedRatio = protectedRatio
	}
}

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

func WithSingleflightWaitTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.loadWaitTTL = ttl
		o.loadWaitTTLSet = true
	}
}

func WithStaleWhileRevalidate(ttl time.Duration) Option {
	return func(o *Options) {
		o.staleTTL = ttl
	}
}

func WithNegativeCache(ttl time.Duration) Option {
	return func(o *Options) {
		o.negativeTTL = ttl
		o.negativeTTLSet = true
		o.negativeCacheEnabled = ttl > 0
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
			mainCache = newShardedCache(0, shards, perShardBytes, options.onEvicted, options.useSLRU, options.slruProtectedRatio)
		} else {
			mainCache = &cache{
				cacheBytes:         cacheBytes,
				onEvicted:          options.onEvicted,
				useSLRU:            options.useSLRU,
				slruProtectedRatio: options.slruProtectedRatio,
			}
		}
	}

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
		if options.useShards {
			shards := options.shards
			if shards == 0 {
				shards = 256
			}
			perShardBytes := cacheBytes / int64(shards)
			if perShardBytes <= 0 {
				perShardBytes = 1
			}
			g.staleCache = newShardedCache(0, shards, perShardBytes, nil, options.useSLRU, options.slruProtectedRatio)
		} else {
			g.staleCache = &cache{
				cacheBytes:         cacheBytes,
				useSLRU:            options.useSLRU,
				slruProtectedRatio: options.slruProtectedRatio,
			}
		}
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
