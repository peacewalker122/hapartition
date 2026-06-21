package store

import "sync"

// Store is a goroutine-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates a new empty Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set stores a value under the given key.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value by key. The bool is false when the key is missing.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
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
