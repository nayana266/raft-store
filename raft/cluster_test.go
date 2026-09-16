package raft

import (
	"bytes"
	"testing"
	"time"
)

func TestLogReplicationCommitsOnMajority(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	idx, term, err := leader.Propose([]byte("put k=v"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if term != leader.CurrentTerm() {
		t.Fatalf("proposal term %d != leader term %d", term, leader.CurrentTerm())
	}
	if err := leader.WaitApplied(idx, 2*time.Second); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		for _, n := range nodes {
			if n.CommitIndex() < idx {
				return false
			}
		}
		return true
	})

	for _, n := range nodes {
		log := n.LogSnapshot()
		if idx >= len(log) || !bytes.Equal(log[idx].Command, []byte("put k=v")) {
			t.Fatalf("node %s missing committed command at %d", n.ID(), idx)
		}
	}
}

func TestCommittedEntrySurvivesLeaderFailure(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	idx, _, err := leader.Propose([]byte("durable"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if err := leader.WaitApplied(idx, 2*time.Second); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	net.Isolate(leader.ID())

	var remaining []*RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForUniqueLeader(t, remaining)

	waitUntil(t, 2*time.Second, func() bool {
		return newLeader.CommitIndex() >= idx
	})
	log := newLeader.LogSnapshot()
	if idx >= len(log) || !bytes.Equal(log[idx].Command, []byte("durable")) {
		t.Fatal("committed entry was lost after leader isolation")
	}
}

func TestUncommittedEntryOnMinorityIsNotLostFromMajorityView(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	// Partition the leader alone, then try a write. It cannot get a majority.
	net.Isolate(leader.ID())
	idx, _, err := leader.Propose([]byte("zombie"))
	if err != nil && err != ErrNotLeader {
		t.Fatalf("Propose: %v", err)
	}
	if err == nil {
		err = leader.WaitApplied(idx, 200*time.Millisecond)
		if err == nil {
			t.Fatal("isolated leader should not consider a write committed")
		}
	}

	var remaining []*RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForUniqueLeader(t, remaining)
	_, _, err = newLeader.Propose([]byte("majority-write"))
	if err != nil {
		t.Fatalf("majority Propose: %v", err)
	}

	net.Heal(leader.ID())

	waitUntil(t, 3*time.Second, func() bool {
		return leader.State() == Follower && leader.LeaderID() == newLeader.ID()
	})
}

func TestFollowerRedirectsReadsToLeader(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	for _, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		_, _, err := n.Propose([]byte("nope"))
		if err != ErrNotLeader {
			t.Fatalf("follower propose err = %v, want ErrNotLeader", err)
		}
		if n.LeaderID() != leader.ID() {
			t.Fatalf("follower leader hint = %q, want %s", n.LeaderID(), leader.ID())
		}
	}
}
