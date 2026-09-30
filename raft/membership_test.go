package raft

import (
	"testing"
	"time"
)

func TestAddServerReplicatesToJoiner(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	cfg := testConfig("n4", "n4")
	cfg.Join = true
	n4 := NewNode(cfg)
	net.Add(n4)
	n4.Start()
	t.Cleanup(n4.Stop)

	time.Sleep(150 * time.Millisecond)
	if n4.State() == Leader {
		t.Fatal("joiner must not campaign")
	}

	idx, err := leader.AddServer("n4", "n4", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.WaitApplied(idx, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		for _, n := range append(nodes, n4) {
			ids := n.PeerIDs()
			found := false
			for _, id := range ids {
				if id == "n4" {
					found = true
				}
			}
			if !found || len(ids) != 4 {
				return false
			}
		}
		return true
	})

	cmdIdx, _, err := leader.Propose([]byte("hello-n4"))
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.WaitApplied(cmdIdx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return n4.LastApplied() >= cmdIdx
	})
}

func TestRemoveServerDropsFollower(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	var follower *RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			follower = n
			break
		}
	}
	idx, err := leader.RemoveServer(follower.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.WaitApplied(idx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return len(leader.PeerIDs()) == 2
	})
	if _, _, err := leader.Propose([]byte("after-remove")); err != nil {
		t.Fatalf("majority of remaining members should still commit: %v", err)
	}
}

func TestAddServerRejectedIfAlreadyMember(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	if _, err := leader.AddServer("n2", "n2", ""); err != ErrAlreadyMember {
		t.Fatalf("err = %v, want ErrAlreadyMember", err)
	}
}

func TestCannotRemoveLeader(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	if _, err := leader.RemoveServer(leader.ID()); err != ErrRemoveLeader {
		t.Fatalf("err = %v, want ErrRemoveLeader", err)
	}
}
