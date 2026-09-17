package raft

import (
	"testing"
	"time"
)

func TestFaultsIsolateDropsBothDirections(t *testing.T) {
	f := NewFaults()
	if !f.Allow("n1", "n2") {
		t.Fatal("healthy network should allow traffic")
	}
	f.Isolate("n2")
	if f.Allow("n1", "n2") || f.Allow("n2", "n3") {
		t.Fatal("isolated node should be unreachable in both directions")
	}
	if !f.Allow("n1", "n3") {
		t.Fatal("non-isolated pair should still talk")
	}
	f.Heal("n2")
	if !f.Allow("n1", "n2") {
		t.Fatal("healed node should be reachable")
	}
}

func TestFaultsDisconnectCutsOnePair(t *testing.T) {
	f := NewFaults()
	f.Disconnect("n1", "n2")
	if f.Allow("n1", "n2") || f.Allow("n2", "n1") {
		t.Fatal("disconnected pair should be blocked")
	}
	if !f.Allow("n1", "n3") {
		t.Fatal("other pairs should still work")
	}
	f.Reconnect("n1", "n2")
	if !f.Allow("n1", "n2") {
		t.Fatal("reconnected pair should work")
	}
}

func TestFilterTransportReturnsUnreachable(t *testing.T) {
	inner := &stubTransport{
		requestVote: func(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{VoteGranted: true}, nil
		},
		appendEntries: func(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
			return &AppendEntriesResponse{Success: true}, nil
		},
	}
	f := NewFaults()
	tr := NewFilterTransport("n1", inner, f)

	if _, err := tr.SendRequestVote("n2", &RequestVoteRequest{}); err != nil {
		t.Fatalf("healthy send: %v", err)
	}
	f.Isolate("n2")
	if _, err := tr.SendRequestVote("n2", &RequestVoteRequest{}); err != ErrUnreachable {
		t.Fatalf("isolated send err = %v, want ErrUnreachable", err)
	}
	if _, err := tr.SendAppendEntries("n2", &AppendEntriesRequest{}); err != ErrUnreachable {
		t.Fatalf("isolated append err = %v, want ErrUnreachable", err)
	}
}

func TestFilterTransportPartitionStopsHeartbeatsAndTriggersElection(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	net.Isolate(leader.ID())

	var remaining []*RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForUniqueLeader(t, remaining)
	if newLeader.ID() == leader.ID() {
		t.Fatal("partitioned leader should not remain the majority leader")
	}
	time.Sleep(20 * time.Millisecond)
}
