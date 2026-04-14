package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "goCache/groupcachepb"
	"goCache/singleflight"

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

func TestPeerPathCachesPeerValueWithEpoch(t *testing.T) {
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
	g := NewGroup("test-peer-cache-epoch", 1<<20, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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

	active, _ := g.currentCaches()
	cached, ok := active.get(keyForPeer)
	if !ok {
		t.Fatalf("expected peer value to be cached in active cache")
	}
	if got, want := cached.String(), "peer-value"; got != want {
		t.Fatalf("unexpected cached value: got %q, want %q", got, want)
	}
	if cached.epoch == 0 {
		t.Fatalf("cached peer value should carry active epoch")
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
		name:   groupName,
		peers:  localPool,
		loader: &singleflight.Group{},
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
