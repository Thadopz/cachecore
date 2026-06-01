package cache

import (
	"context"
	"testing"
	"time"
)

func TestFilterGateNilIsNoop(t *testing.T) {
	var gate *filterGate

	gate.add("Tom")
	if gate.rejects("Tom") {
		t.Fatalf("nil gate should not reject keys")
	}
	if gate.warmup([]string{"Tom"}, func(context.Context, string) error { return nil }) {
		t.Fatalf("nil gate should not report warmup success")
	}

	gate.startRefresh(time.Millisecond, func([]string) {})
	gate.stopRefresh()
}

func TestFilterGateDoesNotRejectBeforeReady(t *testing.T) {
	gate := newFilterGate(newTestFilter())

	if gate.rejects("missing") {
		t.Fatalf("filter gate should allow misses before warmup is ready")
	}

	gate.ready.Store(true)
	if !gate.rejects("missing") {
		t.Fatalf("ready filter gate should reject missing keys")
	}

	gate.add("Tom")
	if gate.rejects("Tom") {
		t.Fatalf("filter gate should allow recorded keys")
	}
}

func TestFilterGateStartRefreshKeepsExistingLoop(t *testing.T) {
	gate := newFilterGate(newTestFilter())
	gate.startRefresh(time.Hour, func([]string) {})
	defer gate.stopRefresh()

	gate.warmupMu.Lock()
	firstStop := gate.refreshStop
	gate.warmupMu.Unlock()
	if firstStop == nil {
		t.Fatalf("refresh start should install a stop channel")
	}

	gate.startRefresh(time.Hour, func([]string) {})

	gate.warmupMu.Lock()
	secondStop := gate.refreshStop
	gate.warmupMu.Unlock()
	if secondStop != firstStop {
		t.Fatalf("refresh start should keep the existing loop")
	}
}
