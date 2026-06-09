package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	groupcache "github.com/Thadopz/cachecore"
)

func TestAPIHandlerReturns404ForErrNotFound(t *testing.T) {
	g := groupcache.NewGroup("test-api-not-found", 1<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, groupcache.ErrNotFound
	}))

	before := groupcache.Stats.Snapshot()
	req := httptest.NewRequest(http.MethodGet, "/api?key=missing", nil)
	w := httptest.NewRecorder()

	newAPIHandler(g).ServeHTTP(w, req)

	if got, want := w.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	after := groupcache.Stats.Snapshot()
	if got, want := after.APIRequests-before.APIRequests, uint64(1); got != want {
		t.Fatalf("unexpected api request delta: got %d, want %d", got, want)
	}
	if got := after.APIErrors - before.APIErrors; got != 0 {
		t.Fatalf("not found should not count as api error: got delta %d", got)
	}
}

func TestAPIHandlerReturns500ForBackendError(t *testing.T) {
	g := groupcache.NewGroup("test-api-backend-error", 1<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("backend exploded")
	}))

	before := groupcache.Stats.Snapshot()
	req := httptest.NewRequest(http.MethodGet, "/api?key=broken", nil)
	w := httptest.NewRecorder()

	newAPIHandler(g).ServeHTTP(w, req)

	if got, want := w.Code, http.StatusInternalServerError; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	after := groupcache.Stats.Snapshot()
	if got, want := after.APIRequests-before.APIRequests, uint64(1); got != want {
		t.Fatalf("unexpected api request delta: got %d, want %d", got, want)
	}
	if got, want := after.APIErrors-before.APIErrors, uint64(1); got != want {
		t.Fatalf("unexpected api error delta: got %d, want %d", got, want)
	}
}

func TestAPIHandlerReturns404ForErrFilterNotFound(t *testing.T) {
	g := groupcache.NewGroup("test-api-filter-not-found", 1<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		if key == "Tom" {
			return []byte("630"), nil
		}
		return nil, errors.New("getter should not be called")
	}), groupcache.WithFilter(newNoopFilter()))
	g.Warmup([]string{"Tom"})

	before := groupcache.Stats.Snapshot()
	req := httptest.NewRequest(http.MethodGet, "/api?key=missing", nil)
	w := httptest.NewRecorder()

	newAPIHandler(g).ServeHTTP(w, req)

	if got, want := w.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}

	after := groupcache.Stats.Snapshot()
	if got, want := after.APIRequests-before.APIRequests, uint64(1); got != want {
		t.Fatalf("unexpected api request delta: got %d, want %d", got, want)
	}
	if got := after.APIErrors - before.APIErrors; got != 0 {
		t.Fatalf("filter not found should not count as api error: got delta %d", got)
	}
}

func TestAPIHandlerIncrementReturnsJSONValue(t *testing.T) {
	var counter int64 = 10
	g := groupcache.NewGroup("test-api-increment", 1<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter should not be called")
	}), groupcache.WithIncrementer(groupcache.IncrementerFunc(func(ctx context.Context, key string, delta int64) (int64, error) {
		counter += delta
		return counter, nil
	})))

	req := httptest.NewRequest(http.MethodPost, "/api/increment?key=counter&delta=5", nil)
	w := httptest.NewRecorder()

	newAPIHandler(g).ServeHTTP(w, req)

	if got, want := w.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d, want %d", got, want)
	}
	var payload struct {
		Key   string `json:"key"`
		Value int64  `json:"value"`
	}
	if err := json.NewDecoder(w.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if got, want := payload.Key, "counter"; got != want {
		t.Fatalf("unexpected key: got %q, want %q", got, want)
	}
	if got, want := payload.Value, int64(15); got != want {
		t.Fatalf("unexpected value: got %d, want %d", got, want)
	}
}
