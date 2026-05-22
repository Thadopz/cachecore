package cache

import "testing"

func TestDJB33IncludesEveryByte(t *testing.T) {
	tests := []struct {
		left  string
		right string
	}{
		{left: "a", right: "b"},
		{left: "abc1", right: "abc2"},
		{left: "12345x", right: "12345y"},
	}

	for _, tt := range tests {
		if left, right := djb33(0, tt.left), djb33(0, tt.right); left == right {
			t.Fatalf("hash collision for keys that differ by a participating byte: %q and %q both hashed to %d", tt.left, tt.right, left)
		}
	}
}

func TestDJB33SeedChangesHash(t *testing.T) {
	if first, second := djb33(0, "Tom"), djb33(1, "Tom"); first == second {
		t.Fatalf("expected seed to affect hash, got %d for both seeds", first)
	}
}
