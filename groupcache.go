package cache

import (
	"errors"
	"fmt"
	"goCache/singleflight"
	"log"
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

	// mainCache is the cache that holds the key-value pairs.
	// It is protected by a mutex to ensure thread safety.
	mainCache Cache

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

var (
	ErrFilterNotFound = errors.New("key not found in filter")
	mu                sync.RWMutex
	groups            = make(map[string]*Group)
)

type Options struct {
	Filter    Filter
	Janitor   *Janitor
	onEvicted func(key string, value ByteView)
	cache     Cache
	useShards bool
	shards    uint32
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

func NewGroup(name string, cacheBytes int64, getter Getter, opts ...Option) *Group {
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

	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: mainCache,
		loader:    &singleflight.Group{},
		filter:    options.Filter,
		janitor:   options.Janitor,
	}
	if g.janitor != nil {
		go g.janitor.Run(g)
	}
	groups[name] = g
	return g
}

func NewGroupWithFilter(name string, cacheBytes int64, getter Getter, filter Filter) *Group {
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
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	Stats.IncGroupGets()
	if v, ok := g.mainCache.get(key); ok {
		Stats.IncCacheHits()
		return v, nil
	}
	Stats.IncCacheMisses()
	return g.load(key)
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
	g.mainCache.add(key, value)
	if g.filter != nil {
		g.filter.Add(key)
	}
	return value, nil
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
			g.mainCache.clearupExpired()
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
