package main

import (
	"context"
	"reflect"
	"testing"
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

func TestCreateGroupSupportsSLRUStrategiesWithSyntheticBackend(t *testing.T) {
	for _, strategy := range []string{"unsharded", "sharded", "slru", "sharded-slru"} {
		t.Run(strategy, func(t *testing.T) {
			g := createGroup(strategy, 4, false, 0, 0, 0, "127.0.0.1:0", "synthetic", 1<<20, false)
			view, err := g.Get(context.Background(), "key123")
			if err != nil {
				t.Fatalf("get failed: %v", err)
			}
			if got, want := view.String(), "123"; got != want {
				t.Fatalf("synthetic value = %q, want %q", got, want)
			}
		})
	}
}
