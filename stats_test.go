package cache

import "testing"

func TestPercentileIndexNearestRank(t *testing.T) {
	tests := []struct {
		name string
		n    int
		p    float64
		want int
	}{
		{name: "single sample p95", n: 1, p: 0.95, want: 0},
		{name: "two samples p95", n: 2, p: 0.95, want: 1},
		{name: "two samples p99", n: 2, p: 0.99, want: 1},
		{name: "ten samples p95", n: 10, p: 0.95, want: 9},
		{name: "ten samples p99", n: 10, p: 0.99, want: 9},
		{name: "clamp lower", n: 5, p: -1, want: 0},
		{name: "clamp upper", n: 5, p: 2, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentileIndex(tt.n, tt.p)
			if got != tt.want {
				t.Fatalf("percentileIndex(%d, %.2f) = %d, want %d", tt.n, tt.p, got, tt.want)
			}
		})
	}
}

func TestLatencyWindowP95P99AndReset(t *testing.T) {
	var w latencyWindow
	for _, us := range []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		w.observe(us)
	}

	p95, p99, ok := w.p95p99AndReset()
	if !ok {
		t.Fatalf("p95p99AndReset() ok = false, want true")
	}
	if p95 != 10 {
		t.Fatalf("p95 = %d, want 10", p95)
	}
	if p99 != 10 {
		t.Fatalf("p99 = %d, want 10", p99)
	}

	_, _, ok = w.p95p99AndReset()
	if ok {
		t.Fatalf("p95p99AndReset() after reset ok = true, want false")
	}
}
