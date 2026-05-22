package cache

import (
	"context"
	"errors"
	"goCache/singleflight"
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
	ErrFilterNotFound          = errors.New("key not found in filter")
	ErrNotFound                = errors.New("key not found")
	ErrSwitchCooldown          = errors.New("cache mode switch is in cooldown")
	ErrSingleflightWaitTimeout = errors.New("singleflight wait timeout")
	mu                         sync.RWMutex
	groups                     = make(map[string]*Group)
)

type Options struct {
	Filter               Filter
	Janitor              *Janitor
	onEvicted            func(key string, value ByteView)
	cache                Cache
	useShards            bool
	shards               uint32
	cacheTTL             time.Duration
	ttlJitter            time.Duration
	fallbackTTL          time.Duration
	cooldown             time.Duration
	loadWaitTTL          time.Duration
	negativeTTL          time.Duration
	fallbackTTLSet       bool
	cooldownSet          bool
	loadWaitTTLSet       bool
	negativeTTLSet       bool
	negativeCacheEnabled bool
	autoPolicy           AutoSwitchPolicy
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

func WithSingleflightWaitTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.loadWaitTTL = ttl
		o.loadWaitTTLSet = true
	}
}

func WithNegativeCache(ttl time.Duration) Option {
	return func(o *Options) {
		o.negativeTTL = ttl
		o.negativeTTLSet = true
		o.negativeCacheEnabled = ttl > 0
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
		name:                 name,
		getter:               getter,
		router:               newCacheRouter(mainCache, mode, fallbackTTL, cooldown, cacheBytes, options.onEvicted, shardCount, options.autoPolicy),
		loader:               &singleflight.Group{},
		singleflightWaitTTL:  options.loadWaitTTL,
		negativeTTL:          options.negativeTTL,
		negativeCacheEnabled: options.negativeCacheEnabled,
		filter:               options.Filter,
		filterRefresh:        options.filterRefresh,
		janitor:              options.Janitor,
		cacheTTL:             options.cacheTTL,
		cacheTTLJitter:       options.ttlJitter,
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
