package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStorage(dir)
	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Command: []byte("hello")},
	}
	if err := s.Save(DurableState{CurrentTerm: 3, VotedFor: "n2", Log: log, Snapshot: []byte("snap")}); err != nil {
		t.Fatal(err)
	}
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 3 || st.VotedFor != "n2" {
		t.Fatalf("term=%d voted=%q", st.CurrentTerm, st.VotedFor)
	}
	if len(st.Log) != 2 || !bytes.Equal(st.Log[1].Command, []byte("hello")) {
		t.Fatalf("log = %+v", st.Log)
	}
	if !bytes.Equal(st.Snapshot, []byte("snap")) {
		t.Fatalf("snapshot = %q", st.Snapshot)
	}
}

func TestFileStorageMissingFileIsEmpty(t *testing.T) {
	s := NewFileStorage(t.TempDir())
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 0 || st.VotedFor != "" || st.Log != nil || st.Snapshot != nil {
		t.Fatalf("got %+v", st)
	}
}

func TestFileStorageOldStateWithoutSnapshotStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	body := []byte(`{"current_term":2,"voted_for":"n1","log":[{"Term":0,"Index":0},{"Term":1,"Index":1,"Command":"YQ=="}]}`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := NewFileStorage(dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 2 || st.VotedFor != "n1" || len(st.Log) != 2 || st.Snapshot != nil {
		t.Fatalf("got %+v", st)
	}
}

func TestNodeRecoversTermVoteAndLog(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig("n1", "n1")
	cfg.Storage = NewFileStorage(dir)
	n := NewNode(cfg)
	n.Start()
	waitUntil(t, time.Second, func() bool { return n.State() == Leader })
	idx, _, err := n.Propose([]byte("keep-me"))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.WaitApplied(idx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	term := n.CurrentTerm()
	n.Stop()

	n2 := NewNode(cfg)
	if n2.CurrentTerm() != term {
		t.Fatalf("recovered term %d, want %d", n2.CurrentTerm(), term)
	}
	if n2.VotedFor() != "n1" {
		t.Fatalf("votedFor=%q", n2.VotedFor())
	}
	found := false
	for _, e := range n2.LogSnapshot() {
		if bytes.Equal(e.Command, []byte("keep-me")) {
			found = true
		}
	}
	if !found {
		t.Fatal("committed command missing from recovered log")
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
}
