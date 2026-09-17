package raft

import (
	"sync"
)

// RequestVoteRequest is the RequestVote RPC argument list (Raft Figure 2).
type RequestVoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResponse is the RequestVote RPC reply.
type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesRequest is the AppendEntries RPC argument list (Raft Figure 2).
type AppendEntriesRequest struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesResponse is the AppendEntries RPC reply.
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// Transport sends Raft RPCs to a named peer. Implementations must not assume
// the caller holds the Raft mutex — and must not deadlock if the receiver
// immediately issues another RPC.
type Transport interface {
	SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

// MemoryNetwork is an in-process transport used by unit and cluster tests.
// It can isolate a node (simulating a crash or full partition) without any
// real sockets.
type MemoryNetwork struct {
	mu     sync.Mutex
	nodes  map[string]*RaftNode
	Faults *Faults
}

// NewMemoryNetwork creates an empty in-process cluster network.
func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		nodes:  make(map[string]*RaftNode),
		Faults: NewFaults(),
	}
}

// Add registers a node and installs a memory transport on it.
func (net *MemoryNetwork) Add(n *RaftNode) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[n.ID()] = n
	n.SetTransport(&memoryTransport{net: net, from: n.ID()})
}

// Isolate drops all RPCs to and from id (crash / hard partition).
func (net *MemoryNetwork) Isolate(id string) { net.Faults.Isolate(id) }

// Heal restores traffic to and from id.
func (net *MemoryNetwork) Heal(id string) { net.Faults.Heal(id) }

// Disconnect blocks RPCs in both directions between a and b.
func (net *MemoryNetwork) Disconnect(a, b string) { net.Faults.Disconnect(a, b) }

func (net *MemoryNetwork) reachable(from, to string) bool {
	return net.Faults.Allow(from, to)
}

func (net *MemoryNetwork) lookup(id string) *RaftNode {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.nodes[id]
}

type memoryTransport struct {
	net  *MemoryNetwork
	from string
}

func (t *memoryTransport) SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.net.mu.Lock()
	ok := t.net.reachable(t.from, to)
	peer := t.net.nodes[to]
	t.net.mu.Unlock()
	if !ok || peer == nil {
		return nil, ErrUnreachable
	}
	return peer.HandleRequestVote(req), nil
}

func (t *memoryTransport) SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.net.mu.Lock()
	ok := t.net.reachable(t.from, to)
	peer := t.net.nodes[to]
	t.net.mu.Unlock()
	if !ok || peer == nil {
		return nil, ErrUnreachable
	}
	return peer.HandleAppendEntries(req), nil
}

// stubTransport is a programmable transport for election unit tests.
type stubTransport struct {
	requestVote   func(to string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	appendEntries func(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

func (s *stubTransport) SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if s.requestVote == nil {
		return nil, ErrUnreachable
	}
	return s.requestVote(to, req)
}

func (s *stubTransport) SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if s.appendEntries == nil {
		return nil, ErrUnreachable
	}
	return s.appendEntries(to, req)
}
