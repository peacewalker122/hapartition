package store

import (
	"sync"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("key1", "value1")
	v, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if v != "value1" {
		t.Fatalf("expected value1, got %q", v)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, ok := s.Get("nonexistent")
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
	_, ok := s.Get("a")
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
