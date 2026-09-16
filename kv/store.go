package kv

import "sync"

// Store is an in-memory key-value map applied from committed Raft entries.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Put writes key to value.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Len is the number of keys currently stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot copies the current map.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
