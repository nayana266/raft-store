package raft

import (
	"errors"
	"sort"
)

var (
	// ErrAlreadyMember is returned when AddServer names a node already in the config.
	ErrAlreadyMember = errors.New("already a member")
	// ErrNotMember is returned when RemoveServer names a node that is not in the config.
	ErrNotMember = errors.New("not a member")
	// ErrConfigInProgress is returned when a previous membership change is not committed.
	ErrConfigInProgress = errors.New("uncommitted membership change")
	// ErrLastMember is returned when RemoveServer would leave the cluster empty.
	ErrLastMember = errors.New("cannot remove the last member")
	// ErrRemoveLeader is returned when asking the current leader to remove itself.
	ErrRemoveLeader = errors.New("cannot remove the leader; pick a follower")
)

const (
	ChangeAdd    = "add"
	ChangeRemove = "remove"
)

// MembershipChange is a single-server config change stored in the Raft log.
// The new configuration takes effect as soon as the entry is appended
// (Raft dissertation §4.1). Only one uncommitted change is allowed at a time.
type MembershipChange struct {
	Type      string            `json:"type"`
	ID        string            `json:"id"`
	Addr      string            `json:"addr,omitempty"`
	HTTP      string            `json:"http,omitempty"`
	Peers     map[string]string `json:"peers,omitempty"`
	HTTPAddrs map[string]string `json:"http_addrs,omitempty"`
}

// PeerHook is called whenever voting membership changes. It must not call back into Raft.
type PeerHook func(peers, httpAddrs map[string]string)

// SetPeerHook installs a callback for membership updates (gRPC dial map, HTTP redirects).
func (n *RaftNode) SetPeerHook(h PeerHook) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerHook = h
}

// AddServer proposes adding id at raft addr (and optional HTTP advertise addr).
func (n *RaftNode) AddServer(id, addr, httpAddr string) (int, error) {
	if id == "" || addr == "" {
		return 0, errors.New("id and raft address required")
	}
	n.mu.Lock()
	idx, err := n.proposeChangeLocked(&MembershipChange{
		Type: ChangeAdd,
		ID:   id,
		Addr: addr,
		HTTP: httpAddr,
	})
	n.mu.Unlock()
	return idx, err
}

// RemoveServer proposes removing id from the cluster. The current leader cannot remove itself.
func (n *RaftNode) RemoveServer(id string) (int, error) {
	if id == "" {
		return 0, errors.New("id required")
	}
	n.mu.Lock()
	idx, err := n.proposeChangeLocked(&MembershipChange{Type: ChangeRemove, ID: id})
	n.mu.Unlock()
	return idx, err
}

func (n *RaftNode) proposeChangeLocked(ch *MembershipChange) (int, error) {
	select {
	case <-n.stopCh:
		return 0, ErrStopped
	default:
	}
	if n.state != Leader {
		return 0, ErrNotLeader
	}
	if n.hasUncommittedConfigLocked() {
		return 0, ErrConfigInProgress
	}
	newPeers := cloneStringMap(n.peerAddrs)
	newHTTP := cloneStringMap(n.peerHTTP)
	switch ch.Type {
	case ChangeAdd:
		if _, ok := n.peerAddrs[ch.ID]; ok {
			return 0, ErrAlreadyMember
		}
		newPeers[ch.ID] = ch.Addr
		if ch.HTTP != "" {
			newHTTP[ch.ID] = ch.HTTP
		}
	case ChangeRemove:
		if _, ok := n.peerAddrs[ch.ID]; !ok {
			return 0, ErrNotMember
		}
		if ch.ID == n.id {
			return 0, ErrRemoveLeader
		}
		if len(n.peerIDs) <= 1 {
			return 0, ErrLastMember
		}
		delete(newPeers, ch.ID)
		delete(newHTTP, ch.ID)
	default:
		return 0, errors.New("unknown membership change")
	}

	index := n.lastLogIndexLocked() + 1
	cp := *ch
	cp.Peers = newPeers
	cp.HTTPAddrs = newHTTP
	n.log = append(n.log, LogEntry{
		Term:   n.currentTerm,
		Index:  index,
		Change: &cp,
	})
	n.rebuildMembershipLocked()
	n.ensurePeerTrackingLocked()
	n.notifyPeersLocked()
	n.persistLocked()
	n.matchIndex[n.id] = index
	n.nextIndex[n.id] = index + 1
	n.advanceCommitLocked()
	n.applyCommittedLocked()
	n.broadcastAppendEntriesLocked()
	n.logger.Info("membership change", "type", ch.Type, "target", ch.ID, "index", index, "peers", n.peerIDs)
	return index, nil
}

func (n *RaftNode) hasUncommittedConfigLocked() bool {
	last := n.lastLogIndexLocked()
	for idx := n.commitIndex + 1; idx <= last; idx++ {
		e, ok := n.entryAtLocked(idx)
		if ok && e.Change != nil {
			return true
		}
	}
	return false
}

func (n *RaftNode) rebuildMembershipLocked() {
	peers := cloneStringMap(n.basePeers)
	httpAddrs := cloneStringMap(n.baseHTTP)
	for _, e := range n.log[1:] {
		if e.Change != nil {
			applyChange(peers, httpAddrs, e.Change)
		}
	}
	n.peerAddrs = peers
	n.peerHTTP = httpAddrs
	n.rebuildPeerIDsLocked()
}

func (n *RaftNode) rebuildPeerIDsLocked() {
	ids := make([]string, 0, len(n.peerAddrs))
	for id := range n.peerAddrs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	n.peerIDs = ids
}

func applyChange(peers, httpAddrs map[string]string, ch *MembershipChange) {
	if len(ch.Peers) > 0 {
		for k := range peers {
			delete(peers, k)
		}
		for k, v := range ch.Peers {
			peers[k] = v
		}
		for k := range httpAddrs {
			delete(httpAddrs, k)
		}
		for k, v := range ch.HTTPAddrs {
			httpAddrs[k] = v
		}
		return
	}
	switch ch.Type {
	case ChangeAdd:
		peers[ch.ID] = ch.Addr
		if ch.HTTP != "" {
			httpAddrs[ch.ID] = ch.HTTP
		}
	case ChangeRemove:
		delete(peers, ch.ID)
		delete(httpAddrs, ch.ID)
	}
}

func (n *RaftNode) membershipAtLocked(index int) (peers, httpAddrs map[string]string) {
	peers = cloneStringMap(n.basePeers)
	httpAddrs = cloneStringMap(n.baseHTTP)
	for _, e := range n.log[1:] {
		if e.Index > index {
			break
		}
		if e.Change != nil {
			applyChange(peers, httpAddrs, e.Change)
		}
	}
	return peers, httpAddrs
}

func (n *RaftNode) ensurePeerTrackingLocked() {
	last := n.lastLogIndexLocked()
	snap := n.lastIncludedIndexLocked()
	for _, p := range n.peerIDs {
		if _, ok := n.nextIndex[p]; !ok {
			if snap > 0 {
				n.nextIndex[p] = snap
			} else {
				n.nextIndex[p] = last + 1
				if n.nextIndex[p] < 1 {
					n.nextIndex[p] = 1
				}
			}
			n.matchIndex[p] = 0
		}
	}
}

func (n *RaftNode) notifyPeersLocked() {
	if s, ok := n.transport.(interface{ SetPeerAddr(string, string) }); ok {
		for id, addr := range n.peerAddrs {
			s.SetPeerAddr(id, addr)
		}
	}
	if n.peerHook != nil {
		n.peerHook(cloneStringMap(n.peerAddrs), cloneStringMap(n.peerHTTP))
	}
}

func (n *RaftNode) isVoterLocked() bool {
	_, ok := n.peerAddrs[n.id]
	return ok
}

func cloneStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// PeerHTTP returns the advertised HTTP address for a peer, if known.
func (n *RaftNode) PeerHTTP(id string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peerHTTP[id]
}
