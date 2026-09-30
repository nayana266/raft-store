package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// DurableState is what a node writes to disk: current term, vote, log, and
// the latest snapshot (if any). commitIndex is not stored; a new leader
// recovers it by committing a current-term no-op.
//
// After compaction the log no longer starts at index 0. log[0] is a dummy
// whose Index/Term are lastIncludedIndex/lastIncludedTerm of the snapshot.
type DurableState struct {
	CurrentTerm   int               `json:"current_term"`
	VotedFor      string            `json:"voted_for"`
	Log           []LogEntry        `json:"log"`
	Snapshot      []byte            `json:"snapshot,omitempty"`
	SnapshotPeers map[string]string `json:"snapshot_peers,omitempty"`
	SnapshotHTTP  map[string]string `json:"snapshot_http,omitempty"`
}

// Storage is durable Raft state.
type Storage interface {
	Save(DurableState) error
	Load() (DurableState, error)
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
func (s *FileStorage) Save(st DurableState) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	body, err := json.Marshal(st)
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
func (s *FileStorage) Load() (DurableState, error) {
	body, err := os.ReadFile(s.path())
	if err != nil {
		if os.IsNotExist(err) {
			return DurableState{}, nil
		}
		return DurableState{}, err
	}
	var st DurableState
	if err := json.Unmarshal(body, &st); err != nil {
		return DurableState{}, fmt.Errorf("corrupt raft state: %w", err)
	}
	return st, nil
}

func (n *RaftNode) persistLocked() {
	if n.storage == nil {
		return
	}
	st := DurableState{
		CurrentTerm:   n.currentTerm,
		VotedFor:      n.votedFor,
		Log:           n.log,
		Snapshot:      n.snapshot,
		SnapshotPeers: n.basePeers,
		SnapshotHTTP:  n.baseHTTP,
	}
	if err := n.storage.Save(st); err != nil {
		n.logger.Error("persist failed", "err", err)
	}
}
