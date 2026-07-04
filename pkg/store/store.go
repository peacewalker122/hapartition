package store

import (
	"sync"
	"sync/atomic"

	"github.com/google/btree"
)

// Entry is a versioned stored value.
type Entry struct {
	Value   string
	Version int64
}

// item is the internal B-tree item, ordered by key.
type item struct {
	key   string
	value string
	ver   int64
}

func less(a, b item) bool { return a.key < b.key }

// Store is a goroutine-safe in-memory key-value store with versioning,
// backed by a B-tree for ordered key traversal.
// Every write assigns a monotonically increasing version (global counter).
// When a write carries an explicit version, the store applies last-writer-wins
// (LWW) semantics: if the stored version is >= the incoming version, the
// write is silently skipped.
type Store struct {
	mu    sync.RWMutex
	tree  *btree.BTreeG[item]
	clock atomic.Int64
}

// New creates a new empty Store.
func New() *Store {
	return &Store{
		tree:  btree.NewG[item](32, less),
		clock: atomic.Int64{},
	}
}

// Set stores a value under the given key with an auto-assigned version.
// Returns the assigned version (> 0) on success.
func (s *Store) Set(key, value string) int64 {
	ver := s.clock.Add(1)
	s.mu.Lock()
	s.tree.ReplaceOrInsert(item{key: key, value: value, ver: ver})
	s.mu.Unlock()
	return ver
}

// SetWithVersion stores a value with the given version.
// If an existing entry has a version >= the given version, the write is
// skipped (last-writer-wins). Returns true if the write was applied.
func (s *Store) SetWithVersion(key, value string, version int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.tree.Get(item{key: key})
	if ok && existing.ver >= version {
		return false
	}
	s.tree.ReplaceOrInsert(item{key: key, value: value, ver: version})

	// Advance clock if external version is ahead of ours.
	if version > s.clock.Load() {
		s.clock.Store(version)
	}
	return true
}

// Get retrieves a value by key. Returns the value, its version, and whether
// the key exists.
func (s *Store) Get(key string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.tree.Get(item{key: key})
	if !ok {
		return "", 0, false
	}
	return it.value, it.ver, true
}

// Snapshot returns a copy of all entries in the store. Used by anti-entropy
// to send the full state to peers.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]Entry, s.tree.Len())
	s.tree.Ascend(func(it item) bool {
		cp[it.key] = Entry{Value: it.value, Version: it.ver}
		return true
	})
	return cp
}

// Range returns all entries with keys in [start, end) in sorted order.
func (s *Store) Range(start, end string) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Entry
	s.tree.AscendRange(
		item{key: start},
		item{key: end},
		func(it item) bool {
			out = append(out, Entry{Value: it.value, Version: it.ver})
			return true
		},
	)
	return out
}

// Iterate calls fn for each entry in the store in sorted key order.
// If fn returns false, iteration stops.
func (s *Store) Iterate(fn func(key string, e Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.tree.Ascend(func(it item) bool {
		return fn(it.key, Entry{Value: it.value, Version: it.ver})
	})
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tree.Len()
}

// Del removes one or more keys and returns the number of keys actually removed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, k := range keys {
		if _, ok := s.tree.Delete(item{key: k}); ok {
			n++
		}
	}
	return n
}
