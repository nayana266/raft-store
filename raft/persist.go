package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Storage is durable Raft state: currentTerm, votedFor, and the log.
// commitIndex is intentionally not stored; it is recovered after a new
// leader commits a current-term entry (the leadership no-op).
type Storage interface {
	Save(currentTerm int, votedFor string, log []LogEntry) error
	Load() (currentTerm int, votedFor string, log []LogEntry, err error)
}

type durableState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// FileStorage writes state.json atomically into a directory.
type FileStorage struct {
	dir string
}

// NewFileStorage stores Raft state under dir/state.json.
func NewFileStorage(dir string) *FileStorage {
	return &FileStorage{dir: dir}
}

func (s *FileStorage) path() string {
	return filepath.Join(s.dir, "state.json")
}

// Save fsyncs a temp file, then renames it over state.json.
func (s *FileStorage) Save(currentTerm int, votedFor string, log []LogEntry) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	body, err := json.Marshal(durableState{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
		Log:         log,
	})
	if err != nil {
		return err
	}
	tmp := s.path() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, s.path()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// Load reads state.json. A missing file is not an error (fresh node).
func (s *FileStorage) Load() (int, string, []LogEntry, error) {
	body, err := os.ReadFile(s.path())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil, nil
		}
		return 0, "", nil, err
	}
	var st durableState
	if err := json.Unmarshal(body, &st); err != nil {
		return 0, "", nil, fmt.Errorf("corrupt raft state: %w", err)
	}
	return st.CurrentTerm, st.VotedFor, st.Log, nil
}

func (n *RaftNode) persistLocked() {
	if n.storage == nil {
		return
	}
	if err := n.storage.Save(n.currentTerm, n.votedFor, n.log); err != nil {
		n.logger.Error("persist failed", "err", err)
	}
}
