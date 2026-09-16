package raft

import (
	"bytes"
	"testing"
)

func TestAppendEntriesLogMatchingRejectsGap(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 1, Index: 4, Command: []byte("x")}},
	})
	if resp.Success {
		t.Fatal("should reject when prevLogIndex is past the end of the log")
	}
}

func TestAppendEntriesLogMatchingRejectsTermMismatch(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}},
		LeaderCommit: 0,
	})
	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  9, // does not match stored term 1
		Entries:      []LogEntry{{Term: 2, Index: 2, Command: []byte("b")}},
	})
	if resp.Success {
		t.Fatal("should reject on prevLogTerm mismatch")
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("log should be unchanged, last index = %d", n.LastLogIndex())
	}
}

func TestAppendEntriesAppendsAndAdvancesCommit(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	var applied []ApplyMsg
	n.SetApply(func(m ApplyMsg) { applied = append(applied, m) })

	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("one")},
			{Term: 1, Command: []byte("two")},
		},
		LeaderCommit: 2,
	})
	if !resp.Success {
		t.Fatal("matching append should succeed")
	}
	if n.LastLogIndex() != 2 {
		t.Fatalf("last index = %d, want 2", n.LastLogIndex())
	}
	if n.CommitIndex() != 2 {
		t.Fatalf("commitIndex = %d, want 2", n.CommitIndex())
	}
	if n.LastApplied() != 2 {
		t.Fatalf("lastApplied = %d, want 2", n.LastApplied())
	}
	if len(applied) != 2 {
		t.Fatalf("applied %d entries, want 2", len(applied))
	}
	if !bytes.Equal(applied[0].Command, []byte("one")) || !bytes.Equal(applied[1].Command, []byte("two")) {
		t.Fatalf("applied commands = %q %q", applied[0].Command, applied[1].Command)
	}
}

func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("a")},
			{Term: 1, Command: []byte("b")},
			{Term: 1, Command: []byte("c")},
		},
	})

	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("B")},
			{Term: 2, Command: []byte("C")},
		},
		LeaderCommit: 3,
	})
	if !resp.Success {
		t.Fatal("conflict repair should succeed")
	}
	log := n.LogSnapshot()
	if len(log) != 4 { // dummy + 3
		t.Fatalf("log len = %d, want 4", len(log))
	}
	if log[1].Term != 1 || !bytes.Equal(log[1].Command, []byte("a")) {
		t.Fatalf("index 1 should be unchanged, got %+v", log[1])
	}
	if log[2].Term != 2 || !bytes.Equal(log[2].Command, []byte("B")) {
		t.Fatalf("index 2 should be replaced, got %+v", log[2])
	}
	if log[3].Term != 2 || !bytes.Equal(log[3].Command, []byte("C")) {
		t.Fatalf("index 3 should be replaced, got %+v", log[3])
	}
}

func TestLeaderCommitRequiresMajorityAndCurrentTerm(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 2
	n.log = append(n.log,
		LogEntry{Term: 1, Index: 1, Command: []byte("old")},
		LogEntry{Term: 2, Index: 2, Command: []byte("new")},
	)
	n.matchIndex = map[string]int{"n1": 2, "n2": 1, "n3": 0}
	n.advanceCommitLocked()
	if n.commitIndex != 0 {
		t.Fatalf("index 1 is previous-term and not majority-replicated at a current-term index; commitIndex = %d", n.commitIndex)
	}

	n.matchIndex["n2"] = 2
	n.advanceCommitLocked()
	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2 (majority has current-term entry)", n.commitIndex)
	}
	n.mu.Unlock()
}

func TestProposeRejectedOnFollower(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	_, _, err := n.Propose([]byte("x"))
	if err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}
