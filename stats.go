package cache

import (
	"log"
	"math"
	"sort"
	"sync"
	atomic "sync/atomic"
	"time"
)

const defaultLatencySampleCap = 8192

type MetricsSnapshot struct {
	APIRequests           uint64
	APIErrors             uint64
	APILatencyCount       uint64
	APILatencyTotalMicros uint64
	APILastP95Micros      uint64
	APILastP99Micros      uint64
	GroupGets             uint64
	GroupGetLatencyCount  uint64
	GroupGetTotalMicros   uint64
	GroupGetLastP95Micros uint64
	GroupGetLastP99Micros uint64
	CacheHits             uint64
	CacheMisses           uint64
	CacheGetLatencyCount  uint64
	CacheGetTotalMicros   uint64
	CacheGetLastP95Micros uint64
	CacheGetLastP99Micros uint64
	PeerLoads             uint64
	LocalLoads            uint64
	PeerHTTPRequests      uint64
	FilterMisses          uint64
}

type Metrics struct {
	APIRequests           uint64
	APIErrors             uint64
	APILatencyCount       uint64
	APILatencyTotalMicros uint64
	APILastP95Micros      uint64
	APILastP99Micros      uint64
	GroupGets             uint64
	GroupGetLatencyCount  uint64
	GroupGetTotalMicros   uint64
	GroupGetLastP95Micros uint64
	GroupGetLastP99Micros uint64
	CacheHits             uint64
	CacheMisses           uint64
	CacheGetLatencyCount  uint64
	CacheGetTotalMicros   uint64
	CacheGetLastP95Micros uint64
	CacheGetLastP99Micros uint64
	PeerLoads             uint64
	LocalLoads            uint64
	PeerHTTPRequests      uint64
	FilterMisses          uint64

	latencySamplingEnabled atomic.Bool
	latencySampleEvery     uint64
	apiLatencySamples      uint64
	groupGetLatencySamples uint64
	cacheGetLatencySamples uint64
	apiLatencyWindow       latencyWindow
	groupGetLatencyWindow  latencyWindow
	cacheGetLatencyWindow  latencyWindow
}

var Stats = newMetrics()

func newMetrics() *Metrics {
	m := &Metrics{}
	m.latencySamplingEnabled.Store(true)
	atomic.StoreUint64(&m.latencySampleEvery, 1)
	return m
}

type latencyWindow struct {
	mu      sync.Mutex
	samples []uint32
	cap     int
}

func (w *latencyWindow) observe(us uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cap <= 0 {
		w.cap = defaultLatencySampleCap
	}
	if w.samples == nil {
		w.samples = make([]uint32, 0, w.cap)
	}

	if us > uint64(^uint32(0)) {
		us = uint64(^uint32(0))
	}
	v := uint32(us)
	if len(w.samples) < w.cap {
		w.samples = append(w.samples, v)
		return
	}

	// Keep recent latency samples with low overhead using a ring-like overwrite.
	copy(w.samples, w.samples[1:])
	w.samples[w.cap-1] = v
}

func percentileIndex(n int, p float64) int {
	idx := int(math.Ceil(float64(n)*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func (w *latencyWindow) p95p99AndReset() (uint64, uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(w.samples)
	if n == 0 {
		return 0, 0, false
	}

	buf := make([]uint32, n)
	copy(buf, w.samples)
	sort.Slice(buf, func(i, j int) bool { return buf[i] < buf[j] })

	p95 := uint64(buf[percentileIndex(n, 0.95)])
	p99 := uint64(buf[percentileIndex(n, 0.99)])

	w.samples = w.samples[:0]
	return p95, p99, true
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		APIRequests:           atomic.LoadUint64(&m.APIRequests),
		APIErrors:             atomic.LoadUint64(&m.APIErrors),
		APILatencyCount:       atomic.LoadUint64(&m.APILatencyCount),
		APILatencyTotalMicros: atomic.LoadUint64(&m.APILatencyTotalMicros),
		APILastP95Micros:      atomic.LoadUint64(&m.APILastP95Micros),
		APILastP99Micros:      atomic.LoadUint64(&m.APILastP99Micros),
		GroupGets:             atomic.LoadUint64(&m.GroupGets),
		GroupGetLatencyCount:  atomic.LoadUint64(&m.GroupGetLatencyCount),
		GroupGetTotalMicros:   atomic.LoadUint64(&m.GroupGetTotalMicros),
		GroupGetLastP95Micros: atomic.LoadUint64(&m.GroupGetLastP95Micros),
		GroupGetLastP99Micros: atomic.LoadUint64(&m.GroupGetLastP99Micros),
		CacheHits:             atomic.LoadUint64(&m.CacheHits),
		CacheMisses:           atomic.LoadUint64(&m.CacheMisses),
		CacheGetLatencyCount:  atomic.LoadUint64(&m.CacheGetLatencyCount),
		CacheGetTotalMicros:   atomic.LoadUint64(&m.CacheGetTotalMicros),
		CacheGetLastP95Micros: atomic.LoadUint64(&m.CacheGetLastP95Micros),
		CacheGetLastP99Micros: atomic.LoadUint64(&m.CacheGetLastP99Micros),
		FilterMisses:          atomic.LoadUint64(&m.FilterMisses),
		PeerLoads:             atomic.LoadUint64(&m.PeerLoads),
		LocalLoads:            atomic.LoadUint64(&m.LocalLoads),
		PeerHTTPRequests:      atomic.LoadUint64(&m.PeerHTTPRequests),
	}
}

func (m *Metrics) IncAPIRequests() {
	atomic.AddUint64(&m.APIRequests, 1)
}

func (m *Metrics) IncAPIErrors() {
	atomic.AddUint64(&m.APIErrors, 1)
}

func (m *Metrics) RecordAPILatency(d time.Duration) {
	if d < 0 {
		return
	}
	us := uint64(d / time.Microsecond)
	atomic.AddUint64(&m.APILatencyCount, 1)
	atomic.AddUint64(&m.APILatencyTotalMicros, us)
	if m.shouldSampleLatency(&m.apiLatencySamples) {
		m.apiLatencyWindow.observe(us)
	}
}

func (m *Metrics) IncGroupGets() {
	atomic.AddUint64(&m.GroupGets, 1)
}

func (m *Metrics) RecordGroupGetLatency(d time.Duration) {
	if d < 0 {
		return
	}
	us := uint64(d / time.Microsecond)
	atomic.AddUint64(&m.GroupGetLatencyCount, 1)
	atomic.AddUint64(&m.GroupGetTotalMicros, us)
	if m.shouldSampleLatency(&m.groupGetLatencySamples) {
		m.groupGetLatencyWindow.observe(us)
	}
}

func (m *Metrics) IncCacheHits() {
	atomic.AddUint64(&m.CacheHits, 1)
}

func (m *Metrics) IncCacheMisses() {
	atomic.AddUint64(&m.CacheMisses, 1)
}

func (m *Metrics) RecordCacheGetLatency(d time.Duration) {
	if d < 0 {
		return
	}
	us := uint64(d / time.Microsecond)
	atomic.AddUint64(&m.CacheGetLatencyCount, 1)
	atomic.AddUint64(&m.CacheGetTotalMicros, us)
	if m.shouldSampleLatency(&m.cacheGetLatencySamples) {
		m.cacheGetLatencyWindow.observe(us)
	}
}

func (m *Metrics) SetLatencySamplingEnabled(enabled bool) {
	m.latencySamplingEnabled.Store(enabled)
}

func (m *Metrics) SetLatencySampleRate(rate float64) {
	if rate <= 0 {
		m.latencySamplingEnabled.Store(false)
		return
	}
	m.latencySamplingEnabled.Store(true)
	if rate >= 1 {
		atomic.StoreUint64(&m.latencySampleEvery, 1)
		return
	}
	every := uint64(1 / rate)
	if every == 0 {
		every = 1
	}
	atomic.StoreUint64(&m.latencySampleEvery, every)
}

func (m *Metrics) shouldSampleLatency(counter *uint64) bool {
	if !m.latencySamplingEnabled.Load() {
		return false
	}
	every := atomic.LoadUint64(&m.latencySampleEvery)
	if every <= 1 {
		return true
	}
	return atomic.AddUint64(counter, 1)%every == 0
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

func (m *Metrics) IncFilterMisses() {
	atomic.AddUint64(&m.FilterMisses, 1)
}

func (m *Metrics) StartLogger(interval time.Duration) {
	if interval <= 0 {
		interval = time.Second * 5
	}
	prev := m.Snapshot()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		apiP95, apiP99, apiOK := m.apiLatencyWindow.p95p99AndReset()
		groupP95, groupP99, groupOK := m.groupGetLatencyWindow.p95p99AndReset()
		cacheP95, cacheP99, cacheOK := m.cacheGetLatencyWindow.p95p99AndReset()
		if apiOK {
			atomic.StoreUint64(&m.APILastP95Micros, apiP95)
			atomic.StoreUint64(&m.APILastP99Micros, apiP99)
		}
		if groupOK {
			atomic.StoreUint64(&m.GroupGetLastP95Micros, groupP95)
			atomic.StoreUint64(&m.GroupGetLastP99Micros, groupP99)
		}
		if cacheOK {
			atomic.StoreUint64(&m.CacheGetLastP95Micros, cacheP95)
			atomic.StoreUint64(&m.CacheGetLastP99Micros, cacheP99)
		}

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

		apiAvgMs := 0.0
		if current.APILatencyCount > 0 {
			apiAvgMs = float64(current.APILatencyTotalMicros) / float64(current.APILatencyCount) / 1000.0
		}
		groupAvgMs := 0.0
		if current.GroupGetLatencyCount > 0 {
			groupAvgMs = float64(current.GroupGetTotalMicros) / float64(current.GroupGetLatencyCount) / 1000.0
		}
		cacheAvgMs := 0.0
		if current.CacheGetLatencyCount > 0 {
			cacheAvgMs = float64(current.CacheGetTotalMicros) / float64(current.CacheGetLatencyCount) / 1000.0
		}

		log.Printf("stats interval=%s api_qps=%.2f peer_qps=%.2f lat_ms(api_avg=%.3f api_p95=%.3f api_p99=%.3f group_avg=%.3f group_p95=%.3f group_p99=%.3f cache_avg=%.3f cache_p95=%.3f cache_p99=%.3f) totals(api=%d api_err=%d gets=%d hits=%d misses=%d peer_load=%d local_load=%d peer_http=%d hit_rate=%.2f%%)",
			interval,
			apiQPS,
			peerQPS,
			apiAvgMs,
			float64(current.APILastP95Micros)/1000.0,
			float64(current.APILastP99Micros)/1000.0,
			groupAvgMs,
			float64(current.GroupGetLastP95Micros)/1000.0,
			float64(current.GroupGetLastP99Micros)/1000.0,
			cacheAvgMs,
			float64(current.CacheGetLastP95Micros)/1000.0,
			float64(current.CacheGetLastP99Micros)/1000.0,
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
