package raft

import (
	"testing"
	"time"
)

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := NewNode(testConfig("n1", "n1"))
	if n.State() != Follower {
		t.Fatalf("state = %s, want follower", n.State())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("term = %d, want 0", n.CurrentTerm())
	}
	if n.VotedFor() != "" {
		t.Fatalf("votedFor = %q, want empty", n.VotedFor())
	}
	if n.LastLogIndex() != 0 {
		t.Fatalf("log index = %d, want 0 (dummy)", n.LastLogIndex())
	}
}

func TestSingleNodeElectionBecomesLeader(t *testing.T) {
	n := NewNode(testConfig("n1", "n1"))
	n.Start()
	defer n.Stop()

	waitUntil(t, time.Second, func() bool { return n.State() == Leader })
	if n.CurrentTerm() < 1 {
		t.Fatalf("term = %d, want >= 1", n.CurrentTerm())
	}
	if n.VotedFor() != "n1" {
		t.Fatalf("votedFor = %q, want n1", n.VotedFor())
	}
	if n.LeaderID() != "n1" {
		t.Fatalf("leaderID = %q, want n1", n.LeaderID())
	}
}

func TestStartElectionBecomesCandidateAndVotesForSelf(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.SetTransport(&stubTransport{}) // votes never come back

	n.mu.Lock()
	n.startElectionLocked()
	state, term, voted := n.state, n.currentTerm, n.votedFor
	n.mu.Unlock()

	if state != Candidate {
		t.Fatalf("state = %s, want candidate", state)
	}
	if term != 1 {
		t.Fatalf("term = %d, want 1", term)
	}
	if voted != "n1" {
		t.Fatalf("votedFor = %q, want n1", voted)
	}
}

func TestCandidateBecomesLeaderOnMajorityVotes(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.SetTransport(&stubTransport{
		requestVote: func(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
		appendEntries: func(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
			return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
		},
	})

	n.mu.Lock()
	n.startElectionLocked()
	n.mu.Unlock()

	waitUntil(t, time.Second, func() bool { return n.State() == Leader })
	if n.CurrentTerm() != 1 {
		t.Fatalf("term = %d, want 1", n.CurrentTerm())
	}
}

func TestCandidateStaysCandidateWithoutMajority(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.SetTransport(&stubTransport{
		requestVote: func(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
		},
	})

	n.mu.Lock()
	n.startElectionLocked()
	n.mu.Unlock()

	time.Sleep(30 * time.Millisecond)
	if n.State() != Candidate {
		t.Fatalf("state = %s, want candidate", n.State())
	}
}

func TestRequestVoteGrantedOnFirstRequest(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	resp := n.HandleRequestVote(&RequestVoteRequest{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}
	if resp.Term != 1 {
		t.Fatalf("resp.term = %d, want 1", resp.Term)
	}
	if n.CurrentTerm() != 1 {
		t.Fatalf("node term = %d, want 1", n.CurrentTerm())
	}
	if n.VotedFor() != "n2" {
		t.Fatalf("votedFor = %q, want n2", n.VotedFor())
	}
	if n.State() != Follower {
		t.Fatalf("state = %s, want follower", n.State())
	}
}

func TestRequestVoteRejectedIfAlreadyVotedForOther(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.HandleRequestVote(&RequestVoteRequest{Term: 1, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0})
	resp := n.HandleRequestVote(&RequestVoteRequest{Term: 1, CandidateID: "n3", LastLogIndex: 0, LastLogTerm: 0})
	if resp.VoteGranted {
		t.Fatal("should not grant a second vote in the same term")
	}
	if n.VotedFor() != "n2" {
		t.Fatalf("votedFor = %q, want n2", n.VotedFor())
	}
}

func TestRequestVoteIdempotentForSameCandidate(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	req := &RequestVoteRequest{Term: 1, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0}
	if !n.HandleRequestVote(req).VoteGranted {
		t.Fatal("first vote should be granted")
	}
	if !n.HandleRequestVote(req).VoteGranted {
		t.Fatal("repeat vote for the same candidate should still be granted")
	}
}

func TestRequestVoteRejectedIfTermStale(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.HandleRequestVote(&RequestVoteRequest{Term: 5, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0})
	resp := n.HandleRequestVote(&RequestVoteRequest{Term: 4, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0})
	if resp.VoteGranted {
		t.Fatal("stale term should not receive a vote")
	}
	if resp.Term != 5 {
		t.Fatalf("resp.term = %d, want 5 so the candidate can step down", resp.Term)
	}
}

func TestRequestVoteRejectedIfCandidateLogStale(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 3, Index: 1, Command: []byte("x")})
	n.currentTerm = 3
	n.mu.Unlock()

	resp := n.HandleRequestVote(&RequestVoteRequest{
		Term:         4,
		CandidateID:  "n2",
		LastLogIndex: 1,
		LastLogTerm:  2, // older term than our last entry
	})
	if resp.VoteGranted {
		t.Fatal("candidate with stale log must not win the vote")
	}
}

func TestRequestVoteGrantedIfCandidateLogMoreUpToDateByTerm(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 2, Index: 1}, LogEntry{Term: 2, Index: 2})
	n.currentTerm = 2
	n.mu.Unlock()

	resp := n.HandleRequestVote(&RequestVoteRequest{
		Term:         3,
		CandidateID:  "n2",
		LastLogIndex: 1, // shorter, but higher term
		LastLogTerm:  3,
	})
	if !resp.VoteGranted {
		t.Fatal("higher last-log-term should count as up-to-date")
	}
}

func TestHigherTermRPCStepsDownLeader(t *testing.T) {
	n := NewNode(testConfig("n1", "n1"))
	n.Start()
	defer n.Stop()
	waitUntil(t, time.Second, func() bool { return n.State() == Leader })
	oldTerm := n.CurrentTerm()

	resp := n.HandleRequestVote(&RequestVoteRequest{
		Term:         oldTerm + 5,
		CandidateID:  "n2",
		LastLogIndex: n.LastLogIndex(),
		LastLogTerm:  n.CurrentTerm(),
	})
	if n.State() != Follower {
		t.Fatalf("state = %s, want follower after higher-term RPC", n.State())
	}
	if n.CurrentTerm() != oldTerm+5 {
		t.Fatalf("term = %d, want %d", n.CurrentTerm(), oldTerm+5)
	}
	if !resp.VoteGranted {
		t.Fatal("stepped-down node should grant the vote when log is current")
	}
}

func TestAppendEntriesFromLeaderConvertsCandidateToFollower(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.SetTransport(&stubTransport{})

	n.mu.Lock()
	n.startElectionLocked()
	n.mu.Unlock()
	if n.State() != Candidate {
		t.Fatalf("setup: state = %s, want candidate", n.State())
	}
	term := n.CurrentTerm()

	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         term,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})
	if !resp.Success {
		t.Fatal("heartbeat with matching prev log should succeed")
	}
	if n.State() != Follower {
		t.Fatalf("state = %s, want follower", n.State())
	}
	if n.LeaderID() != "n2" {
		t.Fatalf("leaderID = %q, want n2", n.LeaderID())
	}
}

func TestStaleAppendEntriesRejected(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2"))
	n.HandleRequestVote(&RequestVoteRequest{Term: 3, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0})

	resp := n.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if resp.Success {
		t.Fatal("stale AppendEntries should be rejected")
	}
	if resp.Term != 3 {
		t.Fatalf("resp.term = %d, want 3", resp.Term)
	}
}

func TestElectionTimeoutOnFollowerStartsElection(t *testing.T) {
	n := NewNode(testConfig("n1", "n1", "n2", "n3"))
	n.SetTransport(&stubTransport{})
	n.Start()
	defer n.Stop()

	waitUntil(t, time.Second, func() bool {
		return n.State() == Candidate || n.CurrentTerm() >= 1
	})
}

func TestThreeNodeClusterElectsUniqueLeader(t *testing.T) {
	_, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)

	time.Sleep(150 * time.Millisecond)
	var leaders []string
	for _, n := range nodes {
		if n.State() == Leader {
			leaders = append(leaders, n.ID())
		}
	}
	if len(leaders) != 1 {
		t.Fatalf("expected one stable leader, got %v", leaders)
	}
	if leaders[0] != leader.ID() {
		t.Fatalf("leader changed unexpectedly from %s to %s", leader.ID(), leaders[0])
	}

	for _, n := range nodes {
		if n.LeaderID() != leader.ID() {
			t.Fatalf("node %s leaderID = %q, want %s", n.ID(), n.LeaderID(), leader.ID())
		}
		if n.CurrentTerm() != leader.CurrentTerm() {
			t.Fatalf("node %s term = %d, want %d", n.ID(), n.CurrentTerm(), leader.CurrentTerm())
		}
	}
}

func TestIsolatedLeaderTriggersNewElection(t *testing.T) {
	net, nodes := startMemoryCluster(t, "n1", "n2", "n3")
	leader := waitForUniqueLeader(t, nodes)
	oldID := leader.ID()
	oldTerm := leader.CurrentTerm()

	net.Isolate(oldID)

	var remaining []*RaftNode
	for _, n := range nodes {
		if n.ID() != oldID {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForUniqueLeader(t, remaining)
	if newLeader.ID() == oldID {
		t.Fatal("isolated node should not remain the cluster leader")
	}
	if newLeader.CurrentTerm() <= oldTerm {
		t.Fatalf("new term %d should be greater than %d", newLeader.CurrentTerm(), oldTerm)
	}
}
