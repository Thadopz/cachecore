package cache

import (
	"log"
	atomic "sync/atomic"
	"time"
)

type MetricsSnapshot struct {
	APIRequests      uint64
	APIErrors        uint64
	GroupGets        uint64
	CacheHits        uint64
	CacheMisses      uint64
	PeerLoads        uint64
	LocalLoads       uint64
	PeerHTTPRequests uint64
}

type Metrics struct {
	APIRequests      uint64
	APIErrors        uint64
	GroupGets        uint64
	CacheHits        uint64
	CacheMisses      uint64
	PeerLoads        uint64
	LocalLoads       uint64
	PeerHTTPRequests uint64
}

var Stats = &Metrics{}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		APIRequests:      atomic.LoadUint64(&m.APIRequests),
		APIErrors:        atomic.LoadUint64(&m.APIErrors),
		GroupGets:        atomic.LoadUint64(&m.GroupGets),
		CacheHits:        atomic.LoadUint64(&m.CacheHits),
		CacheMisses:      atomic.LoadUint64(&m.CacheMisses),
		PeerLoads:        atomic.LoadUint64(&m.PeerLoads),
		LocalLoads:       atomic.LoadUint64(&m.LocalLoads),
		PeerHTTPRequests: atomic.LoadUint64(&m.PeerHTTPRequests),
	}
}

func (m *Metrics) IncAPIRequests() {
	atomic.AddUint64(&m.APIRequests, 1)
}

func (m *Metrics) IncAPIErrors() {
	atomic.AddUint64(&m.APIErrors, 1)
}

func (m *Metrics) IncGroupGets() {
	atomic.AddUint64(&m.GroupGets, 1)
}

func (m *Metrics) IncCacheHits() {
	atomic.AddUint64(&m.CacheHits, 1)
}

func (m *Metrics) IncCacheMisses() {
	atomic.AddUint64(&m.CacheMisses, 1)
}

func (m *Metrics) IncPeerLoads() {
	atomic.AddUint64(&m.PeerLoads, 1)
}

func (m *Metrics) IncLocalLoads() {
	atomic.AddUint64(&m.LocalLoads, 1)
}

func (m *Metrics) IncPeerHTTPRequests() {
	atomic.AddUint64(&m.PeerHTTPRequests, 1)
}

func (m *Metrics) StartLogger(interval time.Duration) {
	if interval <= 0 {
		interval = time.Second * 5
	}
	prev := m.Snapshot()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		current := m.Snapshot()
		seconds := interval.Seconds()
		if seconds <= 0 {
			seconds = 1
		}

		apiQPS := float64(current.APIRequests-prev.APIRequests) / seconds
		peerQPS := float64(current.PeerHTTPRequests-prev.PeerHTTPRequests) / seconds

		hitRate := 0.0
		if current.GroupGets > 0 {
			hitRate = float64(current.CacheHits) / float64(current.GroupGets) * 100
		}

		log.Printf("stats interval=%s api_qps=%.2f peer_qps=%.2f totals(api=%d api_err=%d gets=%d hits=%d misses=%d peer_load=%d local_load=%d peer_http=%d hit_rate=%.2f%%)",
			interval,
			apiQPS,
			peerQPS,
			current.APIRequests,
			current.APIErrors,
			current.GroupGets,
			current.CacheHits,
			current.CacheMisses,
			current.PeerLoads,
			current.LocalLoads,
			current.PeerHTTPRequests,
			hitRate,
		)

		prev = current
	}
}

func (m *Metrics) SatisLogger(interval time.Duration) {
	m.StartLogger(interval)
}
