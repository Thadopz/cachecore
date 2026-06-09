package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Thadopz/cachecore/internal/groupcachepb"
	"github.com/Thadopz/cachecore/internal/singleflight"

	"google.golang.org/protobuf/proto"
)

func TestHTTPPoolServeHTTPEncodesNotFound(t *testing.T) {
	groupName := "test-http-notfound-encode"
	_ = NewGroup(groupName, 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, ErrNotFound
	}), WithNegativeCache(200*time.Millisecond))

	pool := NewHTTPPool("http://self")
	reqMsg := &pb.Request{Group: groupName, Key: "missing"}
	body, err := proto.Marshal(reqMsg)
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, pool.rpcPath("get"), bytes.NewReader(body))
	w := httptest.NewRecorder()
	pool.ServeHTTP(w, req)

	if got, want := w.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	var resp pb.Response
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if !resp.GetNotFound() {
		t.Fatalf("response should set notFound=true")
	}
	if got, want := resp.GetCode(), int32(http.StatusNotFound); got != want {
		t.Fatalf("unexpected response code: got %d, want %d", got, want)
	}
}

func TestHTTPGetterMapsNotFoundFlagToErrNotFound(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &pb.Response{
			Code:     http.StatusNotFound,
			ErrMsg:   ErrNotFound.Error(),
			NotFound: true,
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer s.Close()

	getter := &httpGetter{baseURL: s.URL + defaultBasePath}
	err := getter.Get(context.Background(), &pb.Request{Group: "g", Key: "missing"}, &pb.Response{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestHTTPPoolServeHTTPEncodesFilterNotFoundWithoutNotFoundFlag(t *testing.T) {
	groupName := "test-http-filter-notfound-encode"
	_ = NewGroup(groupName, 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, ErrFilterNotFound
	}))

	pool := NewHTTPPool("http://self")
	reqMsg := &pb.Request{Group: groupName, Key: "missing"}
	body, err := proto.Marshal(reqMsg)
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, pool.rpcPath("get"), bytes.NewReader(body))
	w := httptest.NewRecorder()
	pool.ServeHTTP(w, req)

	if got, want := w.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	var resp pb.Response
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if resp.GetNotFound() {
		t.Fatalf("response should keep notFound=false for ErrFilterNotFound")
	}
	if got, want := resp.GetCode(), int32(http.StatusNotFound); got != want {
		t.Fatalf("unexpected response code: got %d, want %d, errMsg=%q", got, want, resp.GetErrMsg())
	}
}

func TestHTTPGetterDoesNotMapFilterNotFoundToErrNotFound(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &pb.Response{
			Code:     http.StatusNotFound,
			ErrMsg:   ErrFilterNotFound.Error(),
			NotFound: false,
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer s.Close()

	getter := &httpGetter{baseURL: s.URL + defaultBasePath}
	err := getter.Get(context.Background(), &pb.Request{Group: "g", Key: "missing"}, &pb.Response{})
	if err == nil {
		t.Fatalf("expected getter error for filter not found")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("filter-not-found response must not map to ErrNotFound: %v", err)
	}
}

func TestHTTPPoolServeHTTPIncrementUpdatesLocalCache(t *testing.T) {
	groupName := "test-http-increment"
	var counter int64 = 4
	g := NewGroup(groupName, 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter should not be called after increment fills cache")
	}), WithIncrementer(IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		return atomic.AddInt64(&counter, delta), nil
	})))

	pool := NewHTTPPool("http://self")
	reqMsg := &pb.Request{Group: groupName, Key: "counter", Delta: 3}
	body, err := proto.Marshal(reqMsg)
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, pool.rpcPath("increment"), bytes.NewReader(body))
	w := httptest.NewRecorder()
	pool.ServeHTTP(w, req)

	if got, want := w.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	var resp pb.Response
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if got, want := string(resp.GetValue()), "7"; got != want {
		t.Fatalf("unexpected increment value: got %q, want %q", got, want)
	}

	view, err := g.Get(context.Background(), "counter")
	if err != nil {
		t.Fatalf("get after increment failed: %v", err)
	}
	if got, want := view.String(), "7"; got != want {
		t.Fatalf("unexpected cached value: got %q, want %q", got, want)
	}
}

func TestPeerIncrementRoutesToOwner(t *testing.T) {
	var peerCalls int32
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != defaultBasePath+"increment" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		atomic.AddInt32(&peerCalls, 1)
		var req pb.Request
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if got, want := req.GetDelta(), int64(5); got != want {
			http.Error(w, fmt.Sprintf("unexpected delta: got %d want %d", got, want), http.StatusBadRequest)
			return
		}
		resp := &pb.Response{Value: []byte("12")}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer peerServer.Close()

	selfServer := httptest.NewServer(http.NotFoundHandler())
	defer selfServer.Close()

	pool := NewHTTPPool(selfServer.URL)
	pool.Set(selfServer.URL, peerServer.URL)

	var keyForPeer string
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("peer-increment-%d", i)
		if pool.peers.Get(k) == peerServer.URL {
			keyForPeer = k
			break
		}
	}
	if keyForPeer == "" {
		t.Fatalf("failed to find key that maps to peer %s", peerServer.URL)
	}

	g := NewGroup("test-peer-increment", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("local getter should not be called")
	}), WithIncrementer(IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		return 0, errors.New("local incrementer should not be called")
	})))
	g.RegisterPeers(pool)

	next, err := g.Increment(context.Background(), keyForPeer, 5)
	if err != nil {
		t.Fatalf("peer increment failed: %v", err)
	}
	if got, want := next, int64(12); got != want {
		t.Fatalf("unexpected peer increment result: got %d, want %d", got, want)
	}
	if got := atomic.LoadInt32(&peerCalls); got != 1 {
		t.Fatalf("peer should be called once: got %d", got)
	}
}

func TestPeerPathEndToEndNotFoundNoLocalFallback(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != defaultBasePath+"get" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		resp := &pb.Response{
			Code:     http.StatusNotFound,
			ErrMsg:   ErrNotFound.Error(),
			NotFound: true,
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer peerServer.Close()

	selfServer := httptest.NewServer(http.NotFoundHandler())
	defer selfServer.Close()

	pool := NewHTTPPool(selfServer.URL)
	pool.Set(selfServer.URL, peerServer.URL)

	var keyForPeer string
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("peer-miss-%d", i)
		if pool.peers.Get(k) == peerServer.URL {
			keyForPeer = k
			break
		}
	}
	if keyForPeer == "" {
		t.Fatalf("failed to find key that maps to peer %s", peerServer.URL)
	}

	var localCalls int32
	g := NewGroup("test-peer-path-notfound", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&localCalls, 1)
		return []byte("local-should-not-be-called"), nil
	}), WithNegativeCache(200*time.Millisecond))
	g.RegisterPeers(pool)

	_, err := g.Get(context.Background(), keyForPeer)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from peer path, got: %v", err)
	}
	if got := atomic.LoadInt32(&localCalls); got != 0 {
		t.Fatalf("local getter should not be called when peer returns ErrNotFound: got %d", got)
	}
}

func TestPeerPathCachesPeerValue(t *testing.T) {
	var peerCalls int32
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != defaultBasePath+"get" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		atomic.AddInt32(&peerCalls, 1)
		resp := &pb.Response{Value: []byte("peer-value")}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer peerServer.Close()

	selfServer := httptest.NewServer(http.NotFoundHandler())
	defer selfServer.Close()

	pool := NewHTTPPool(selfServer.URL)
	pool.Set(selfServer.URL, peerServer.URL)

	var keyForPeer string
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("peer-cache-%d", i)
		if pool.peers.Get(k) == peerServer.URL {
			keyForPeer = k
			break
		}
	}
	if keyForPeer == "" {
		t.Fatalf("failed to find key that maps to peer %s", peerServer.URL)
	}

	var localCalls int32
	g := NewGroup("test-peer-cache-value", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&localCalls, 1)
		return []byte("local-value"), nil
	}))
	g.RegisterPeers(pool)

	v, err := g.Get(context.Background(), keyForPeer)
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if got, want := v.String(), "peer-value"; got != want {
		t.Fatalf("unexpected first value: got %q, want %q", got, want)
	}

	v, err = g.Get(context.Background(), keyForPeer)
	if err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if got, want := v.String(), "peer-value"; got != want {
		t.Fatalf("unexpected second value: got %q, want %q", got, want)
	}

	if got := atomic.LoadInt32(&peerCalls); got != 1 {
		t.Fatalf("peer should be called once after cache fill: got %d", got)
	}
	if got := atomic.LoadInt32(&localCalls); got != 0 {
		t.Fatalf("local getter should not be called on peer hit: got %d", got)
	}

	active := g.mainCache
	cached, ok := active.get(keyForPeer)
	if !ok {
		t.Fatalf("expected peer value to be cached")
	}
	if got, want := cached.String(), "peer-value"; got != want {
		t.Fatalf("unexpected cached value: got %q, want %q", got, want)
	}
}

func TestHTTPPoolServeHTTPInvalidateRemovesCachedEntry(t *testing.T) {
	var calls int32
	groupName := "test-http-invalidate-local"
	g := NewGroup(groupName, 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("value"), nil
	}))

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("warm get failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("unexpected getter calls after warmup: got %d, want 1", got)
	}

	pool := NewHTTPPool("http://self")
	reqMsg := &pb.Request{Group: groupName, Key: "Tom"}
	body, err := proto.Marshal(reqMsg)
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, pool.rpcPath("invalidate"), bytes.NewReader(body))
	w := httptest.NewRecorder()
	pool.ServeHTTP(w, req)

	if got, want := w.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	if _, err := g.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("get after invalidate failed: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("invalidate should force reload: got %d calls, want 2", got)
	}
}

func TestInvalidateBroadcastRemovesPeerEntry(t *testing.T) {
	groupName := "test-invalidate-broadcast"

	var remoteCalls int32
	remoteGroup := NewGroup(groupName, 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&remoteCalls, 1)
		return []byte("remote-" + key), nil
	}))
	if _, err := remoteGroup.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("remote warm get failed: %v", err)
	}
	if got := atomic.LoadInt32(&remoteCalls); got != 1 {
		t.Fatalf("unexpected remote getter calls after warmup: got %d, want 1", got)
	}

	remotePool := NewHTTPPool("http://remote")
	remoteServer := httptest.NewServer(remotePool)
	defer remoteServer.Close()

	localPool := NewHTTPPool("http://self")
	localPool.Set("http://self", remoteServer.URL)

	localGroup := &Group{
		name:      groupName,
		mainCache: &cache{cacheBytes: 1 << 20},
		peers:     localPool,
		loader:    &singleflight.Group{},
	}

	localGroup.Invalidate("Tom")

	if _, err := remoteGroup.Get(context.Background(), "Tom"); err != nil {
		t.Fatalf("remote get after broadcast invalidate failed: %v", err)
	}
	if got := atomic.LoadInt32(&remoteCalls); got != 2 {
		t.Fatalf("broadcast invalidate should clear remote cache: got %d calls, want 2", got)
	}
}

func TestHTTPGetterInvalidateTimesOut(t *testing.T) {
	oldTimeout := defaultInvalidateTimeout
	defaultInvalidateTimeout = 50 * time.Millisecond
	defer func() {
		defaultInvalidateTimeout = oldTimeout
	}()

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer s.Close()

	getter := &httpGetter{baseURL: s.URL + defaultBasePath}
	start := time.Now()
	err := getter.Invalidate(&pb.Request{Group: "g", Key: "Tom"})
	if err == nil {
		t.Fatalf("expected invalidate to fail on timeout")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("invalidate should respect timeout, took %s", elapsed)
	}
}
