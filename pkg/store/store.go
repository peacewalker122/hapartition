package store

import (
	"sync"
	"sync/atomic"

	"github.com/google/btree"
)

type Entry struct {
	Value   string
	Version int64
	Origin  string
	Deleted bool
}

type item struct {
	key string
	Entry
}

func less(a, b item) bool { return a.key < b.key }

type Store struct {
	mu     sync.RWMutex
	tree   *btree.BTreeG[item]
	clock  atomic.Int64
	length int
}

func New() *Store {
	return &Store{tree: btree.NewG[item](32, less)}
}

func (s *Store) Set(key, value string) int64 {
	return s.SetFrom(key, value, "").Version
}

func (s *Store) SetFrom(key, value, origin string) Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := Entry{Value: value, Version: s.clock.Add(1), Origin: origin}
	old, ok := s.tree.Get(item{key: key})
	if !ok || newer(entry, old.Entry) {
		if !ok || old.Deleted {
			s.length++
		}
		s.tree.ReplaceOrInsert(item{key: key, Entry: entry})
	}
	return entry
}

func (s *Store) SetWithVersion(key string, entry Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.tree.Get(item{key: key})
	if ok && !newer(entry, old.Entry) {
		return false
	}
	if !entry.Deleted && (!ok || old.Deleted) {
		s.length++
	} else if ok && !old.Deleted && entry.Deleted {
		s.length--
	}
	s.tree.ReplaceOrInsert(item{key: key, Entry: entry})
	if entry.Version > s.clock.Load() {
		s.clock.Store(entry.Version)
	}
	return true
}

func newer(a, b Entry) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	if a.Origin != b.Origin {
		return a.Origin > b.Origin
	}
	if a.Deleted != b.Deleted {
		return a.Deleted
	}
	return a.Value > b.Value
}

func (s *Store) Get(key string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.tree.Get(item{key: key})
	if !ok || it.Deleted {
		return "", 0, false
	}
	return it.Value, it.Version, true
}

func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]Entry, s.tree.Len())
	s.tree.Ascend(func(it item) bool {
		cp[it.key] = it.Entry
		return true
	})
	return cp
}

func (s *Store) Range(start, end string) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Entry
	s.tree.AscendRange(item{key: start}, item{key: end}, func(it item) bool {
		if !it.Deleted {
			out = append(out, it.Entry)
		}
		return true
	})
	return out
}

func (s *Store) Iterate(fn func(key string, e Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.tree.Ascend(func(it item) bool {
		return it.Deleted || fn(it.key, it.Entry)
	})
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length

}
func (s *Store) Del(origin string, keys ...string) (int, map[string]Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	tombstones := make(map[string]Entry, len(keys))
	for _, key := range keys {
		if _, duplicate := tombstones[key]; duplicate {
			continue
		}
		old, exists := s.tree.Get(item{key: key})
		if exists && !old.Deleted {
			n++
			s.length--
		}
		entry := Entry{Version: s.clock.Add(1), Origin: origin, Deleted: true}
		s.tree.ReplaceOrInsert(item{key: key, Entry: entry})
		tombstones[key] = entry
	}
	return n, tombstones
}
