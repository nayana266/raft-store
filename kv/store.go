package kv

import (
	"encoding/json"
	"sync"
)

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

// Clone copies the current map.
func (s *Store) Clone() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Replace overwrites the map. Used when installing a Raft snapshot.
func (s *Store) Replace(m map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(m))
	for k, v := range m {
		s.data[k] = v
	}
}

// Snapshot serializes the map for Raft log compaction.
func (s *Store) Snapshot() ([]byte, error) {
	return json.Marshal(s.Clone())
}

// Restore replaces the map from a Snapshot() blob.
func (s *Store) Restore(data []byte) error {
	var m map[string]string
	if len(data) == 0 {
		s.Replace(map[string]string{})
		return nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	if m == nil {
		m = map[string]string{}
	}
	s.Replace(m)
	return nil
}
