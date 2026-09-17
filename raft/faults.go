package raft

import "sync"

// Faults injects network failures: isolate a node or cut a pair of peers.
// One Faults object is shared by every node's FilterTransport.
type Faults struct {
	mu       sync.Mutex
	isolated map[string]bool
	blocked  map[string]map[string]bool // from -> to
}

// NewFaults returns an injector with a healthy network.
func NewFaults() *Faults {
	return &Faults{
		isolated: make(map[string]bool),
		blocked:  make(map[string]map[string]bool),
	}
}

// Allow reports whether an RPC from -> to should be delivered.
func (f *Faults) Allow(from, to string) bool {
	if f == nil {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.isolated[from] || f.isolated[to] {
		return false
	}
	if f.blocked[from] != nil && f.blocked[from][to] {
		return false
	}
	return true
}

// Isolate drops all RPCs to and from id (a hard partition / unplug the cable).
func (f *Faults) Isolate(id string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.isolated[id] = true
}

// Heal restores traffic to and from id.
func (f *Faults) Heal(id string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.isolated, id)
}

// Disconnect blocks RPCs in both directions between a and b.
func (f *Faults) Disconnect(a, b string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked[a] == nil {
		f.blocked[a] = make(map[string]bool)
	}
	if f.blocked[b] == nil {
		f.blocked[b] = make(map[string]bool)
	}
	f.blocked[a][b] = true
	f.blocked[b][a] = true
}

// Reconnect restores traffic between a and b.
func (f *Faults) Reconnect(a, b string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked[a] != nil {
		delete(f.blocked[a], b)
	}
	if f.blocked[b] != nil {
		delete(f.blocked[b], a)
	}
}

// HealAll clears every partition and isolation.
func (f *Faults) HealAll() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.isolated = make(map[string]bool)
	f.blocked = make(map[string]map[string]bool)
}

// Isolated reports whether id is fully partitioned.
func (f *Faults) Isolated(id string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isolated[id]
}

// FilterTransport wraps an inner Transport and drops RPCs the Faults injector forbids.
type FilterTransport struct {
	from   string
	inner  Transport
	faults *Faults
}

// NewFilterTransport returns a Transport that consults faults before each send.
func NewFilterTransport(from string, inner Transport, faults *Faults) *FilterTransport {
	return &FilterTransport{from: from, inner: inner, faults: faults}
}

func (t *FilterTransport) SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if t.faults != nil && !t.faults.Allow(t.from, to) {
		return nil, ErrUnreachable
	}
	return t.inner.SendRequestVote(to, req)
}

func (t *FilterTransport) SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if t.faults != nil && !t.faults.Allow(t.from, to) {
		return nil, ErrUnreachable
	}
	return t.inner.SendAppendEntries(to, req)
}

// Close closes the inner transport when it supports it.
func (t *FilterTransport) Close() {
	if c, ok := t.inner.(interface{ Close() }); ok {
		c.Close()
	}
}
