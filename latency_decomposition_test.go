package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "goCache/groupcachepb"

	"google.golang.org/protobuf/proto"
)

type latencyBenchResult struct {
	name        string
	workers     int
	duration    time.Duration
	ops         uint64
	errors      uint64
	samples     int
	qps         float64
	avg         time.Duration
	p50         time.Duration
	p95         time.Duration
	p99         time.Duration
	max         time.Duration
	sampleEvery uint64
}

func TestLatencyDecomposition(t *testing.T) {
	if os.Getenv("LATENCY_DECOMP") != "1" {
		t.Skip("set LATENCY_DECOMP=1 to run latency decomposition")
	}

	workers := envInt("LATENCY_DECOMP_WORKERS", 800)
	duration := envDuration("LATENCY_DECOMP_DURATION", 2*time.Second)

	Stats.SetLatencySamplingEnabled(false)
	t.Cleanup(func() {
		Stats.SetLatencySamplingEnabled(true)
	})

	results := []latencyBenchResult{
		measureInternalLocalHit(t, workers, duration),
		measureHTTPNoop(t, workers, duration),
		measureHTTPLocalHit(t, workers, duration),
		measurePeerProtoRoundTrip(t, workers, duration),
	}

	for _, r := range results {
		t.Logf("RESULT name=%s workers=%d duration=%s ops=%d errors=%d qps=%.0f avg=%s p50=%s p95=%s p99=%s max=%s samples=%d sample_every=%d",
			r.name, r.workers, r.duration, r.ops, r.errors, r.qps, r.avg, r.p50, r.p95, r.p99, r.max, r.samples, r.sampleEvery)
	}
}

func measureInternalLocalHit(t *testing.T, workers int, duration time.Duration) latencyBenchResult {
	t.Helper()

	g := NewGroup(fmt.Sprintf("latency-internal-%d", time.Now().UnixNano()), 64<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), WithShardedCache(256))
	if _, err := g.Get(context.Background(), "hot-key"); err != nil {
		t.Fatalf("warmup failed: %v", err)
	}

	return runLatencyScenario("internal-local-hit", workers, duration, 32, func() error {
		_, err := g.Get(context.Background(), "hot-key")
		return err
	})
}

func measureHTTPNoop(t *testing.T, workers int, duration time.Duration) latencyBenchResult {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := latencyHTTPClient(workers)
	url := server.URL + "/ping"
	return runLatencyScenario("http-noop", workers, duration, 1, func() error {
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %d", resp.StatusCode)
		}
		return nil
	})
}

func measureHTTPLocalHit(t *testing.T, workers int, duration time.Duration) latencyBenchResult {
	t.Helper()

	g := NewGroup(fmt.Sprintf("latency-http-local-%d", time.Now().UnixNano()), 64<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), WithShardedCache(256))
	if _, err := g.Get(context.Background(), "hot-key"); err != nil {
		t.Fatalf("warmup failed: %v", err)
	}

	server := httptest.NewServer(latencyAPIHandler(g))
	defer server.Close()

	client := latencyHTTPClient(workers)
	url := server.URL + "/api?key=hot-key"
	return runLatencyScenario("http-local-hit", workers, duration, 1, func() error {
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %d", resp.StatusCode)
		}
		return nil
	})
}

func measurePeerProtoRoundTrip(t *testing.T, workers int, duration time.Duration) latencyBenchResult {
	t.Helper()

	response := &pb.Response{Value: []byte("peer-value")}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		t.Fatalf("marshal peer response failed: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != defaultBasePath+"get" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req pb.Request
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBytes)
	}))
	defer server.Close()

	g := NewGroup(fmt.Sprintf("latency-peer-%d", time.Now().UnixNano()), 64<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, ErrNotFound
	}), WithShardedCache(256))
	getter := &httpGetter{baseURL: server.URL + defaultBasePath}

	return runLatencyScenario("peer-proto-roundtrip", workers, duration, 1, func() error {
		_, err := g.getFromPeer(context.Background(), getter, "peer-key")
		return err
	})
}

func runLatencyScenario(name string, workers int, duration time.Duration, sampleEvery uint64, fn func() error) latencyBenchResult {
	if workers <= 0 {
		workers = 1
	}
	if duration <= 0 {
		duration = time.Second
	}
	if sampleEvery == 0 {
		sampleEvery = 1
	}

	start := make(chan struct{})
	stop := make(chan struct{})
	samplesCh := make(chan []int64, workers)
	var totalOps uint64
	var totalErrors uint64
	var totalNanos uint64
	var maxNanos uint64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localSamples := make([]int64, 0, 1024)
			var localOps uint64
			var localErrors uint64
			var localNanos uint64
			var localMax uint64
			<-start
			for {
				select {
				case <-stop:
					atomic.AddUint64(&totalOps, localOps)
					atomic.AddUint64(&totalErrors, localErrors)
					atomic.AddUint64(&totalNanos, localNanos)
					updateMaxUint64(&maxNanos, localMax)
					samplesCh <- localSamples
					return
				default:
				}

				started := time.Now()
				if err := fn(); err != nil {
					localErrors++
					continue
				}
				elapsed := time.Since(started)
				nanos := uint64(elapsed.Nanoseconds())
				localNanos += nanos
				if nanos > localMax {
					localMax = nanos
				}

				localOps++
				if localOps%sampleEvery == 0 {
					localSamples = append(localSamples, int64(nanos))
				}
			}
		}()
	}

	wallStart := time.Now()
	close(start)
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	close(samplesCh)
	wallElapsed := time.Since(wallStart)

	samples := make([]int64, 0, 8192)
	for s := range samplesCh {
		samples = append(samples, s...)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	ops := atomic.LoadUint64(&totalOps)
	avg := time.Duration(0)
	if ops > 0 {
		avg = time.Duration(atomic.LoadUint64(&totalNanos) / ops)
	}

	return latencyBenchResult{
		name:        name,
		workers:     workers,
		duration:    wallElapsed,
		ops:         ops,
		errors:      atomic.LoadUint64(&totalErrors),
		samples:     len(samples),
		qps:         float64(ops) / wallElapsed.Seconds(),
		avg:         avg,
		p50:         percentileDuration(samples, 0.50),
		p95:         percentileDuration(samples, 0.95),
		p99:         percentileDuration(samples, 0.99),
		max:         time.Duration(atomic.LoadUint64(&maxNanos)),
		sampleEvery: sampleEvery,
	}
}

func latencyHTTPClient(workers int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        workers * 2,
			MaxIdleConnsPerHost: workers * 2,
			MaxConnsPerHost:     workers * 2,
			DisableCompression:  true,
		},
		Timeout: 10 * time.Second,
	}
}

func latencyAPIHandler(g *Group) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		view, err := g.Get(r.Context(), r.URL.Query().Get("key"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(view.ByteSlice())
	})
}

func percentileDuration(samples []int64, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	idx := int(float64(len(samples)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return time.Duration(samples[idx])
}

func updateMaxUint64(addr *uint64, candidate uint64) {
	for {
		current := atomic.LoadUint64(addr)
		if candidate <= current {
			return
		}
		if atomic.CompareAndSwapUint64(addr, current, candidate) {
			return
		}
	}
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func BenchmarkProtoMarshalRequest(b *testing.B) {
	req := &pb.Request{Group: "scores", Key: "hot-key"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(req)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) == 0 {
			b.Fatal("empty proto request")
		}
	}
}

func BenchmarkProtoUnmarshalRequest(b *testing.B) {
	req := &pb.Request{Group: "scores", Key: "hot-key"}
	data, err := proto.Marshal(req)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out pb.Request
		if err := proto.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
		if out.Key == "" {
			b.Fatal("empty key")
		}
	}
}

func BenchmarkHTTPResponseWrite(b *testing.B) {
	body := []byte("value:hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		_, _ = io.Copy(w, bytes.NewReader(body))
		if w.Body.Len() != len(body) {
			b.Fatal("short write")
		}
	}
}

func BenchmarkPeerHTTPClientDoProtoRequest(b *testing.B) {
	response := &pb.Response{Value: []byte("peer-value")}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		b.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req pb.Request
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBytes)
	}))
	defer server.Close()

	cases := []struct {
		name   string
		client *http.Client
	}{
		{name: "default-client", client: http.DefaultClient},
		{name: "dedicated-peer-client", client: newPeerHTTPClient()},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			oldClient := peerHTTPClient
			peerHTTPClient = tc.client
			defer func() {
				peerHTTPClient = oldClient
			}()

			getter := &httpGetter{baseURL: server.URL + defaultBasePath}
			req := &pb.Request{Group: "scores", Key: "Tom"}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(parallel *testing.PB) {
				for parallel.Next() {
					var out pb.Response
					if err := getter.Get(context.Background(), req, &out); err != nil {
						b.Fatal(err)
					}
					if len(out.GetValue()) == 0 {
						b.Fatal("empty peer value")
					}
				}
			})
		})
	}
}
