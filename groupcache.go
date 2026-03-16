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
	name        string
	getter      Getter
	mainCache   cache
	peers       PeerPicker
	loader      *singleflight.Group
	filter      Filter
	janitor     *Janitor
	filterReady atomic.Bool
}

type Filter interface {
	Add(item string)
	Contains(item string) bool
	Remove(item string)
}

type Option func(*Options)

var (
	ErrFilterNotFound = errors.New("key not found in filter")
	mu                sync.RWMutex
	groups            = make(map[string]*Group)
)

type Options struct {
	Filter  Filter
	Janitor *Janitor
}

func WithFilter(filter Filter) Option {
	return func(o *Options) {
		o.Filter = filter
	}
}

func WithJanitor(interval time.Duration) Option {
	return func(o *Options) {
		o.Janitor = NewJanitor(interval)
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

	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
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
