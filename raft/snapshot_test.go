package raft

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

// byteSM snapshots the concatenation of applied commands (joined by 0xff).
type byteSM struct {
	applied [][]byte
}

func (s *byteSM) onApply(m ApplyMsg) {
	if len(m.Command) == 0 {
		return
	}
	s.applied = append(s.applied, append([]byte(nil), m.Command...))
}

func (s *byteSM) Snapshot() ([]byte, error) {
	return json.Marshal(s.applied)
}

func (s *byteSM) Restore(data []byte) error {
	if len(data) == 0 {
		s.applied = nil
		return nil
	}
	return json.Unmarshal(data, &s.applied)
}

func TestCompactTruncatesLogAndKeepsLastIncluded(t *testing.T) {
	sm := &byteSM{}
	cfg := testConfig("n1", "n1")
	cfg.Snapshotter = sm
	cfg.SnapshotThreshold = 4
	n := NewNode(cfg)
	n.SetApply(sm.onApply)
	n.Start()
	defer n.Stop()
	waitUntil(t, time.Second, func() bool { return n.State() == Leader })

	for i := 0; i < 5; i++ {
		idx, _, err := n.Propose([]byte{byte('a' + i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.WaitApplied(idx, time.Second); err != nil {
			t.Fatal(err)
		}
	}

	waitUntil(t, time.Second, func() bool { return n.SnapshotIndex() > 0 })
	snap := n.SnapshotIndex()
	if snap < 4 {
		t.Fatalf("snapshot_index = %d, want >= 4", snap)
	}
	if n.LogLen() > 2 {
		t.Fatalf("log_len = %d after compact, want a short suffix", n.LogLen())
	}
	log := n.LogSnapshot()
	if log[0].Index != snap {
		t.Fatalf("dummy index = %d, want %d", log[0].Index, snap)
	}

	idx, _, err := n.Propose([]byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.WaitApplied(idx, time.Second); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range n.LogSnapshot() {
		if bytes.Equal(e.Command, []byte("after")) {
			found = true
		}
	}
	if !found {
		t.Fatal("entry appended after compact missing from log")
	}
}

func TestLaggingFollowerReceivesInstallSnapshot(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	net := NewMemoryNetwork()
	nodes := make([]*RaftNode, 0, 3)
	sms := make(map[string]*byteSM, 3)
	for _, id := range ids {
		sm := &byteSM{}
		sms[id] = sm
		cfg := testConfig(id, ids...)
		cfg.Snapshotter = sm
		cfg.SnapshotThreshold = 4
		n := NewNode(cfg)
		n.SetApply(sm.onApply)
		net.Add(n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	leader := waitForUniqueLeader(t, nodes)
	var lagging *RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			lagging = n
			break
		}
	}
	net.Isolate(lagging.ID())

	for i := 0; i < 8; i++ {
		idx, _, err := leader.Propose([]byte{byte('A' + i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := leader.WaitApplied(idx, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 2*time.Second, func() bool { return leader.SnapshotIndex() > 0 })
	if lagging.SnapshotIndex() != 0 {
		t.Fatalf("isolated node should not have compacted, snapshot_index=%d", lagging.SnapshotIndex())
	}

	net.Heal(lagging.ID())
	waitUntil(t, 3*time.Second, func() bool {
		return lagging.SnapshotIndex() > 0 || lagging.LastApplied() >= leader.LastApplied()
	})
	waitUntil(t, 3*time.Second, func() bool {
		return lagging.LastApplied() >= leader.CommitIndex()
	})
	if lagging.LastApplied() < leader.SnapshotIndex() {
		t.Fatalf("lagging lastApplied=%d snapshot on leader=%d", lagging.LastApplied(), leader.SnapshotIndex())
	}
}

func TestAppendEntriesAfterLocalCompact(t *testing.T) {
	sm := &byteSM{}
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.snapshotter = sm
	n.snapshotThreshold = 3
	n.SetApply(sm.onApply)

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
		LeaderCommit: 3,
	})
	if n.SnapshotIndex() < 3 {
		t.Fatalf("expected compact at 3, snapshot_index=%d log_len=%d", n.SnapshotIndex(), n.LogLen())
	}

	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 1, Command: []byte("d")}},
		LeaderCommit: 4,
	})
	if !resp.Success {
		t.Fatal("append after compact should succeed with prevLog = lastIncluded")
	}
	if n.LastLogIndex() != 4 {
		t.Fatalf("last index = %d, want 4", n.LastLogIndex())
	}
	if n.LastApplied() != 4 {
		t.Fatalf("lastApplied = %d, want 4", n.LastApplied())
	}
}
