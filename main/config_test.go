package main

import (
	"reflect"
	"testing"

	groupcache "goCache"
)

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{name: "empty", input: "", want: nil},
		{name: "spaces only", input: "   ", want: nil},
		{name: "single", input: "Tom", want: []string{"Tom"}},
		{name: "multiple", input: "Tom,Jack,Sam", want: []string{"Tom", "Jack", "Sam"}},
		{name: "trim blanks", input: " Tom, , Jack ,, Sam ", want: []string{"Tom", "Jack", "Sam"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitCSV(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitCSV(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestDefaultWarmupKeys(t *testing.T) {
	got := defaultWarmupKeys()
	if len(got) != len(db)+101 {
		t.Fatalf("defaultWarmupKeys length = %d, want %d", len(got), len(db)+101)
	}
	if got[0] != "Jack" || got[1] != "Sam" || got[2] != "Tom" {
		t.Fatalf("defaultWarmupKeys should start with sorted db keys, got %#v", got[:3])
	}
	if got[len(got)-1] != "key100" {
		t.Fatalf("defaultWarmupKeys should include seeded numeric keys, got last=%q", got[len(got)-1])
	}
}

func TestNewWindowMissRateSampler(t *testing.T) {
	snapshots := []groupcache.MetricsSnapshot{
		{GroupGets: 100, CacheMisses: 40},
		{GroupGets: 120, CacheMisses: 50},
		{GroupGets: 120, CacheMisses: 50},
		{GroupGets: 150, CacheMisses: 65},
	}
	var i int
	sampler := newWindowMissRateSampler(func() groupcache.MetricsSnapshot {
		s := snapshots[i]
		if i < len(snapshots)-1 {
			i++
		}
		return s
	})

	if got, want := sampler(), 0.5; got != want {
		t.Fatalf("first window miss rate = %v, want %v", got, want)
	}
	if got, want := sampler(), -1.0; got != want {
		t.Fatalf("empty window should be skipped with sentinel value: got %v, want %v", got, want)
	}
	if got, want := sampler(), 0.5; got != want {
		t.Fatalf("second non-empty window miss rate = %v, want %v", got, want)
	}
}
