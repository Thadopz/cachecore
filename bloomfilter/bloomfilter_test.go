package bloomfilter

import "testing"

func TestNewWithCustomParams(t *testing.T) {
	bf := New(128, 3)
	if bf == nil {
		t.Fatalf("expected bloom filter, got nil")
	}
	if len(bf.arr) != 128 {
		t.Fatalf("unexpected bitset size: got %d, want %d", len(bf.arr), 128)
	}
	if bf.k != 3 {
		t.Fatalf("unexpected hash count: got %d, want %d", bf.k, 3)
	}
}

func TestNewFallbackDefaults(t *testing.T) {
	bf := New(0, -1)
	if bf == nil {
		t.Fatalf("expected bloom filter, got nil")
	}
	if len(bf.arr) != defaultBitsetSize {
		t.Fatalf("unexpected default bitset size: got %d, want %d", len(bf.arr), defaultBitsetSize)
	}
	if bf.k != defaultHashCount {
		t.Fatalf("unexpected default hash count: got %d, want %d", bf.k, defaultHashCount)
	}
}

func TestAddAndContains(t *testing.T) {
	bf := New(1000, 6)
	item := "Tom"
	bf.Add(item)
	if !bf.Contains(item) {
		t.Fatalf("expected item to be contained after add")
	}
}

func TestContainsNotAddedKey(t *testing.T) {
	bf := New(1000, 6)
	bf.Add("Tom")

	if bf.Contains("Jack") {
		t.Fatalf("unexpected positive for key that was not added")
	}
}

func TestResetClearsFilter(t *testing.T) {
	bf := New(1000, 3)
	bf.Add("Tom")
	if !bf.Contains("Tom") {
		t.Fatalf("expected item to be contained before reset")
	}
	bf.Reset()
	if bf.Contains("Tom") {
		t.Fatalf("expected reset to clear the filter")
	}
}

func TestNewDefault(t *testing.T) {
	bf := NewDefault()
	if bf == nil {
		t.Fatalf("expected bloom filter, got nil")
	}
	if len(bf.arr) != defaultBitsetSize {
		t.Fatalf("unexpected default bitset size: got %d, want %d", len(bf.arr), defaultBitsetSize)
	}
	if bf.k != defaultHashCount {
		t.Fatalf("unexpected default hash count: got %d, want %d", bf.k, defaultHashCount)
	}
}
