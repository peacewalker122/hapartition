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

	n := s.Del("a")
	if n != 1 {
		t.Fatalf("expected 1 deleted, got %d", n)
	}
	_, _, ok := s.Get("a")
	if ok {
		t.Fatal("expected 'a' to be gone after del")
	}

	n = s.Del("a") // already deleted
	if n != 0 {
		t.Fatalf("expected 0 deleted for missing key, got %d", n)
	}

	n = s.Del("b", "nonexistent")
	if n != 1 {
		t.Fatalf("expected 1 deleted (b), got %d", n)
	}
}

func TestSetWithVersionLWW(t *testing.T) {
	s := New()

	// Initial write with version 100
	ok := s.SetWithVersion("key", "first", 100)
	if !ok {
		t.Fatal("expected first write to be applied")
	}
	v, ver, _ := s.Get("key")
	if v != "first" || ver != 100 {
		t.Fatalf("expected (first, 100), got (%q, %d)", v, ver)
	}

	// Older version — should be skipped (LWW)
	ok = s.SetWithVersion("key", "second", 50)
	if ok {
		t.Fatal("expected older version to be rejected")
	}
	v, ver, _ = s.Get("key")
	if v != "first" || ver != 100 {
		t.Fatalf("expected value to remain (first, 100), got (%q, %d)", v, ver)
	}

	// Equal version — should be skipped
	ok = s.SetWithVersion("key", "third", 100)
	if ok {
		t.Fatal("expected equal version to be rejected")
	}

	// Newer version — should overwrite
	ok = s.SetWithVersion("key", "fourth", 200)
	if !ok {
		t.Fatal("expected newer version to be applied")
	}
	v, ver, _ = s.Get("key")
	if v != "fourth" || ver != 200 {
		t.Fatalf("expected (fourth, 200), got (%q, %d)", v, ver)
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
			s.Del(key)
		}(i)
	}

	wg.Wait()
}
