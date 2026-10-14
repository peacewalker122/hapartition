package merkle

import (
	"testing"

	"github.com/peacewalker122/hapartition/pkg/store"
)

func TestBuildEmptyStore(t *testing.T) {
	st := store.New()
	tree := Build(st, 64)

	if tree.NumLeaves() != 64 {
		t.Fatalf("expected 64 leaves, got %d", tree.NumLeaves())
	}
	if tree.RootHash() == nil {
		t.Fatal("expected non-nil root hash")
	}
	for i := 0; i < 64; i++ {
		if tree.LeafKeyCount(i) != 0 {
			t.Errorf("leaf %d: expected 0 keys, got %d", i, tree.LeafKeyCount(i))
		}
	}
}

func TestBuildSingleKey(t *testing.T) {
	st := store.New()
	st.Set("hello", "world")

	tree := Build(st, 64)

	totalKeys := 0
	for i := 0; i < 64; i++ {
		totalKeys += tree.LeafKeyCount(i)
	}
	if totalKeys != 1 {
		t.Fatalf("expected 1 total key, got %d", totalKeys)
	}
}

func TestDiffIdentical(t *testing.T) {
	st := store.New()
	for i := 0; i < 100; i++ {
		st.Set(string(rune('a'+i%26)), "val")
	}

	a := Build(st, 64)
	b := Build(st, 64)

	diffs := Diff(a, b)
	if len(diffs) != 0 {
		t.Fatalf("expected no diffs for identical stores, got %v", diffs)
	}
}

func TestDiffDifferent(t *testing.T) {
	stA := store.New()
	stA.Set("key_a", "value_a")

	stB := store.New()
	stB.Set("key_b", "value_b")

	a := Build(stA, 64)
	b := Build(stB, 64)

	diffs := Diff(a, b)
	if len(diffs) == 0 {
		t.Fatal("expected diffs for different stores, got none")
	}
	t.Logf("differing leaves: %v", diffs)
}

func TestDiffSameKeyDifferentValue(t *testing.T) {
	stA := store.New()
	stA.Set("key", "alpha")

	stB := store.New()
	stB.Set("key", "beta")

	a := Build(stA, 64)
	b := Build(stB, 64)

	diffs := Diff(a, b)
	if len(diffs) == 0 {
		t.Fatal("expected diffs for same-key different-value, got none")
	}
}

func TestLeafBounds(t *testing.T) {
	start, end := LeafBounds(64, 0)
	if start != 0 {
		t.Fatalf("leaf 0 start: expected 0, got %d", start)
	}
	if end == 0 {
		t.Fatal("leaf 0 end: expected >0")
	}

	// Last leaf should end at maxUint64.
	start, end = LeafBounds(64, 63)
	if end != maxUint64 {
		t.Fatalf("leaf 63 end: expected %d, got %d", maxUint64, end)
	}
}

func TestLeafRangeMaximumBoundary(t *testing.T) {
	boundary := uint64(42)
	if inLeafRange(boundary, 0, boundary, false) {
		t.Fatal("ordinary leaf included exclusive end")
	}
	if !inLeafRange(boundary, boundary, maxUint64, true) {
		t.Fatal("last leaf omitted adjacent boundary")
	}
	if !inLeafRange(maxUint64, boundary, maxUint64, true) {
		t.Fatal("last leaf omitted MaxUint64")
	}
	if inLeafRange(maxUint64, boundary, maxUint64, false) {
		t.Fatal("non-final leaf included MaxUint64 at exclusive end")
	}
}

func TestLeafEntries(t *testing.T) {
	st := store.New()
	st.Set("alpha", "1")
	st.Set("beta", "2")
	st.Set("gamma", "3")

	tree := Build(st, 64)

	for i := 0; i < 64; i++ {
		entries := LeafEntries(st, tree.NumLeaves(), i)
		if len(entries) > 0 {
			t.Logf("leaf %d has %d entries", i, len(entries))
		}
	}
}

func TestDeterministicHashes(t *testing.T) {
	st1 := store.New()
	st1.Set("a", "1")
	st1.Set("b", "2")

	st2 := store.New()
	st2.Set("a", "1")
	st2.Set("b", "2")

	a := Build(st1, 64)
	b := Build(st2, 64)

	if !bytesEqual(a.RootHash(), b.RootHash()) {
		t.Fatal("expected same root hash for identical stores")
	}
}

func TestRoundUpPowerOfTwo(t *testing.T) {
	tests := []struct {
		n, expected int
	}{
		{0, 0},
		{-1, 0},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{31, 32},
		{33, 64},
		{64, 64},
		{65, 128},
	}
	for _, tc := range tests {
		got := roundUpPowerOfTwo(tc.n)
		if got != tc.expected {
			t.Errorf("roundUpPowerOfTwo(%d) = %d, want %d", tc.n, got, tc.expected)
		}
	}
}

func TestBuildWithNonPowerOfTwo(t *testing.T) {
	st := store.New()
	st.Set("x", "y")

	// Request 50 leaves → rounds up to 64.
	tree := Build(st, 50)
	if tree.NumLeaves() != 64 {
		t.Fatalf("expected 64 leaves (rounded up from 50), got %d", tree.NumLeaves())
	}
}

func TestRootHashChangesWithData(t *testing.T) {
	st := store.New()
	st.Set("k", "v")
	h1 := Build(st, 64).RootHash()

	st.Set("k2", "v2")
	h2 := Build(st, 64).RootHash()

	if bytesEqual(h1, h2) {
		t.Fatal("root hash should change when data changes")
	}
}
