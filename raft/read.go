package raft

import "time"

// ReadIndex is Raft §6.4: confirm this node is still the leader, then return a
// commit index that is safe to read. The caller must wait until lastApplied
// reaches that index before serving the value (ReadIndex already waits).
//
// An isolated leader fails fast with ErrStaleLeader instead of returning a
// stale local map read.
func (n *RaftNode) ReadIndex(timeout time.Duration) (int, error) {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	deadline := time.Now().Add(timeout)

	if err := n.waitCurrentTermCommitted(timeout); err != nil {
		return 0, err
	}

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIndex := n.commitIndex
	term := n.currentTerm
	n.mu.Unlock()

	remain := time.Until(deadline)
	if remain <= 0 {
		return 0, ErrTimeout
	}
	if err := n.confirmLeadership(term, remain); err != nil {
		return 0, err
	}
	remain = time.Until(deadline)
	if remain <= 0 {
		return 0, ErrTimeout
	}
	if err := n.WaitApplied(readIndex, remain); err != nil {
		return 0, err
	}
	if n.State() != Leader {
		return 0, ErrNotLeader
	}
	return readIndex, nil
}

func (n *RaftNode) committedCurrentTermLocked() bool {
	term, ok := n.termAtLocked(n.commitIndex)
	return ok && term == n.currentTerm
}

func (n *RaftNode) waitCurrentTermCommitted(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return ErrNotLeader
		}
		committed := n.committedCurrentTermLocked()
		if !committed {
			n.broadcastAppendEntriesLocked()
		}
		n.mu.Unlock()
		if committed {
			return nil
		}
		select {
		case <-n.stopCh:
			return ErrStopped
		case <-timer.C:
			return ErrTimeout
		case <-tick.C:
		}
	}
}

func (n *RaftNode) confirmLeadership(term int, timeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return ErrNotLeader
	}
	majority := n.majority()
	peers := make([]string, 0, len(n.peerIDs))
	for _, id := range n.peerIDs {
		if id != n.id {
			peers = append(peers, id)
		}
	}
	n.mu.Unlock()

	acks := 1
	if acks >= majority {
		return nil
	}

	results := make(chan bool, len(peers))
	for _, peer := range peers {
		go n.sendConfirmHeartbeat(peer, term, results)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	pending := len(peers)
	for acks < majority {
		select {
		case <-n.stopCh:
			return ErrStopped
		case <-timer.C:
			return ErrTimeout
		case ok := <-results:
			pending--
			if ok {
				acks++
			}
			if acks >= majority {
				return nil
			}
			if acks+pending < majority {
				return ErrStaleLeader
			}
		}
	}
	return nil
}

func (n *RaftNode) sendConfirmHeartbeat(peer string, term int, results chan<- bool) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term || n.transport == nil {
		n.mu.Unlock()
		results <- false
		return
	}
	req := n.confirmHeartbeatLocked()
	n.mu.Unlock()

	resp, err := n.transport.SendAppendEntries(peer, req)
	if err != nil {
		results <- false
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.becomeFollowerLocked(resp.Term, "")
		n.resetElectionTimerLocked()
		results <- false
		return
	}
	results <- n.state == Leader && n.currentTerm == term
}

func (n *RaftNode) confirmHeartbeatLocked() *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: n.lastLogIndexLocked(),
		PrevLogTerm:  n.lastLogTermLocked(),
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}
}
