package store

import (
	"sync"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("key1", "value1")
	v, ver, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if v != "value1" {
		t.Fatalf("expected value1, got %q", v)
	}
	if ver <= 0 {
		t.Fatalf("expected positive version, got %d", ver)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, _, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestDel(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	n, tombstones := s.Del("node-a", "a")
	if n != 1 || !tombstones["a"].Deleted || tombstones["a"].Origin != "node-a" {
		t.Fatalf("expected one tombstone for a, got %d, %#v", n, tombstones)
	}
	if s.Len() != 1 {
		t.Fatalf("expected one visible key, got %d", s.Len())
	}
	_, _, ok := s.Get("a")
	if ok {
		t.Fatal("expected 'a' to be gone after del")
	}

	firstVersion := tombstones["a"].Version
	n, tombstones = s.Del("node-a", "a", "a")
	if n != 0 || len(tombstones) != 1 || tombstones["a"].Version <= firstVersion {
		t.Fatalf("expected refreshed tombstone for a, got %d, %#v", n, tombstones)
	}
	if s.Len() != 1 {
		t.Fatalf("expected one visible key after repeat delete, got %d", s.Len())
	}
	if s.SetWithVersion("a", Entry{Value: "stale", Version: firstVersion, Origin: "node-z"}) {
		t.Fatal("expected refreshed tombstone to reject older concurrent write")
	}

	n, tombstones = s.Del("node-a", "b", "nonexistent", "b", "nonexistent")
	if n != 1 || len(tombstones) != 2 || !tombstones["nonexistent"].Deleted || !tombstones["b"].Deleted {
		t.Fatalf("expected one removed key and unique tombstones, got %d, %#v", n, tombstones)
	}
	if s.Len() != 0 {
		t.Fatalf("expected no visible keys after deleting b, got %d", s.Len())
	}

	absentVersion := tombstones["nonexistent"].Version
	n, tombstones = s.Del("node-a", "nonexistent")
	if n != 0 || len(tombstones) != 1 || tombstones["nonexistent"].Version <= absentVersion {
		t.Fatalf("expected refreshed tombstone for absent key, got %d, %#v", n, tombstones)
	}
}

func TestSetWithVersionLWW(t *testing.T) {
	s := New()

	// Initial write with version 100
	ok := s.SetWithVersion("key", Entry{Value: "first", Version: 100, Origin: "a"})
	if !ok {
		t.Fatal("expected first write to be applied")
	}
	v, ver, _ := s.Get("key")
	if v != "first" || ver != 100 {
		t.Fatalf("expected (first, 100), got (%q, %d)", v, ver)
	}

	// Older version — should be skipped (LWW)
	ok = s.SetWithVersion("key", Entry{Value: "second", Version: 50, Origin: "b"})
	if ok {
		t.Fatal("expected older version to be rejected")
	}
	v, ver, _ = s.Get("key")
	if v != "first" || ver != 100 {
		t.Fatalf("expected value to remain (first, 100), got (%q, %d)", v, ver)
	}

	// Equal version — should be skipped
	ok = s.SetWithVersion("key", Entry{Value: "first", Version: 100, Origin: "a"})
	if ok {
		t.Fatal("expected equal version to be rejected")
	}

	// Newer version — should overwrite
	ok = s.SetWithVersion("key", Entry{Value: "fourth", Version: 200, Origin: "a"})
	if !ok {
		t.Fatal("expected newer version to be applied")
	}
	v, ver, _ = s.Get("key")
	if v != "fourth" || ver != 200 {
		t.Fatalf("expected (fourth, 200), got (%q, %d)", v, ver)
	}
}

func TestLWWUsesOriginAndTombstones(t *testing.T) {
	left := New()
	right := New()
	left.SetWithVersion("key", Entry{Value: "z", Version: 10, Origin: "a"})
	right.SetWithVersion("key", Entry{Value: "a", Version: 10, Origin: "b"})
	left.SetWithVersion("same", Entry{Value: "z", Version: 1, Origin: "x"})
	right.SetWithVersion("same", Entry{Value: "a", Version: 1, Origin: "x"})
	for key, entry := range left.Snapshot() {
		right.SetWithVersion(key, entry)
	}
	for key, entry := range right.Snapshot() {
		left.SetWithVersion(key, entry)
	}
	if left.Snapshot()["key"] != right.Snapshot()["key"] || left.Snapshot()["key"].Origin != "b" {
		t.Fatalf("replicas did not converge on origin order: %#v / %#v", left.Snapshot(), right.Snapshot())
	}
	if left.Snapshot()["same"] != right.Snapshot()["same"] || left.Snapshot()["same"].Value != "z" {
		t.Fatalf("replicas did not converge on equal origin/version conflict: %#v / %#v", left.Snapshot(), right.Snapshot())
	}
	_, tombstones := left.Del("b", "key")
	right.SetWithVersion("key", tombstones["key"])
	if left.Len() != 1 || right.Len() != 1 || !right.Snapshot()["key"].Deleted {
		t.Fatalf("tombstone did not converge: %#v / %#v", left.Snapshot(), right.Snapshot())
	}
}

func TestSetAutoVersion(t *testing.T) {
	s := New()

	ver1 := s.Set("key", "first")
	if ver1 <= 0 {
		t.Fatalf("expected positive version, got %d", ver1)
	}
	_, v, _ := s.Get("key")
	if v != ver1 {
		t.Fatalf("expected stored version %d, got %d", ver1, v)
	}

	ver2 := s.Set("key", "second")
	if ver2 <= ver1 {
		t.Fatalf("expected monotonic version: %d <= %d", ver2, ver1)
	}
	_, v, _ = s.Get("key")
	if v != ver2 {
		t.Fatalf("expected stored version %d, got %d", ver2, v)
	}
}

func TestRange(t *testing.T) {
	s := New()
	s.Set("b", "2")
	s.Set("a", "1")
	s.Set("c", "3")
	s.Set("d", "4")

	results := s.Range("b", "d") // [b, d) → b, c only
	if len(results) != 2 {
		t.Fatalf("expected 2 entries in [b, d), got %d", len(results))
	}
	if results[0].Value != "2" {
		t.Errorf("first in range: expected 2, got %s", results[0].Value)
	}
	if results[1].Value != "3" {
		t.Errorf("second in range: expected 3, got %s", results[1].Value)
	}
}

func TestIterate(t *testing.T) {
	s := New()
	s.Set("z", "last")
	s.Set("a", "first")
	s.Set("m", "middle")

	var keys []string
	s.Iterate(func(key string, e Entry) bool {
		keys = append(keys, key)
		return true
	})

	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "a" || keys[1] != "m" || keys[2] != "z" {
		t.Fatalf("expected sorted [a m z], got %v", keys)
	}
}

func TestIterateEarlyStop(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")

	count := 0
	s.Iterate(func(key string, e Entry) bool {
		count++
		return count < 2 // stop after one
	})
	if count != 2 {
		t.Fatalf("expected 2 calls (one + stop), got %d", count)
	}
}

func TestConcurrency(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	n := 100

	// Concurrent writers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			s.Set(key, "value")
		}(i)
	}

	// Concurrent readers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			s.Get(key)
		}(i)
	}

	// Concurrent deleters
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			s.Del("test", key)
		}(i)
	}

	wg.Wait()
}
