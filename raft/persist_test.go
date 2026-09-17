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
	if err := s.Save(3, "n2", log); err != nil {
		t.Fatal(err)
	}
	term, voted, got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 3 || voted != "n2" {
		t.Fatalf("term=%d voted=%q", term, voted)
	}
	if len(got) != 2 || !bytes.Equal(got[1].Command, []byte("hello")) {
		t.Fatalf("log = %+v", got)
	}
}

func TestFileStorageMissingFileIsEmpty(t *testing.T) {
	s := NewFileStorage(t.TempDir())
	term, voted, log, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 || voted != "" || log != nil {
		t.Fatalf("got term=%d voted=%q log=%v", term, voted, log)
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
