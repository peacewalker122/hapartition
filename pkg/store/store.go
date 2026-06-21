package store

import (
	"sync"
	"sync/atomic"
)

// Entry is a versioned stored value.
type Entry struct {
	Value   string
	Version int64
}

// Store is a goroutine-safe in-memory key-value store with versioning.
// Every write assigns a monotonically increasing version (global counter).
// When a write carries an explicit version, the store applies last-writer-wins
// (LWW) semantics: if the stored version is >= the incoming version, the
// write is silently skipped. This enables read repair at the cluster level
type Store struct {
	mu    sync.RWMutex
	data  map[string]Entry
	clock int64 // global monotonic version counter
}

// New creates a new empty Store.
func New() *Store {
	return &Store{
		data:  make(map[string]Entry),
		clock: 0,
	}
}

// Set stores a value under the given key with an auto-assigned version.
// Returns the assigned version (> 0) on success, or 0 if the write was rejected
// by LWW (which never happens with auto-assigned versions since the counter is
// strictly monotonic).
func (s *Store) Set(key, value string) int64 {
	ver := atomic.AddInt64(&s.clock, 1)
	s.SetWithVersion(key, value, ver)
	return ver
}

// SetWithVersion stores a value with the given version.
// If an existing entry has a version >= the given version, the write is
// skipped (last-writer-wins). Returns true if the write was applied.
func (s *Store) SetWithVersion(key, value string, version int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[key]; ok && existing.Version >= version {
		return false
	}
	s.data[key] = Entry{Value: value, Version: version}
	return true
}

// Get retrieves a value by key. Returns the value, its version, and whether
// the key exists.
func (s *Store) Get(key string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return "", 0, false
	}
	return e.Value, e.Version, true
}

// Snapshot returns a copy of all entries in the store. Used by anti-entropy
// to send the full state to peers.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]Entry, len(s.data))
	for k, e := range s.data {
		cp[k] = e
	}
	return cp
}

// Del removes one or more keys and returns the number of keys actually removed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			n++
		}
	}
	return n
}
