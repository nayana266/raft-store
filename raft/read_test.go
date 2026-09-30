package raft

import (
	"errors"
	"testing"
	"time"
)

func TestReadIndexOnHealthyLeader(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	idx, err := leader.ReadIndex(2 * time.Second)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx < 1 {
		t.Fatalf("ReadIndex = %d, want a committed current-term index", idx)
	}
	if leader.LastApplied() < idx {
		t.Fatalf("lastApplied %d < readIndex %d", leader.LastApplied(), idx)
	}
}

func TestReadIndexFailsWhenLeaderIsolated(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	if _, err := leader.ReadIndex(2 * time.Second); err != nil {
		t.Fatalf("warmup ReadIndex: %v", err)
	}

	net.Isolate(leader.ID())
	_, err := leader.ReadIndex(2 * time.Second)
	if !errors.Is(err, ErrStaleLeader) && !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("isolated ReadIndex err = %v, want stale/timeout/not-leader", err)
	}
}

func TestReadIndexSucceedsWithOneFollowerIsolated(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	var follower string
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			follower = n.ID()
			break
		}
	}
	net.Isolate(follower)

	if _, err := leader.ReadIndex(2 * time.Second); err != nil {
		t.Fatalf("ReadIndex with one follower down: %v", err)
	}
}
