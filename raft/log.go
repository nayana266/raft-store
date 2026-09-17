package raft

func (n *RaftNode) lastLogIndexLocked() int {
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTermLocked() int {
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) broadcastAppendEntriesLocked() {
	if n.transport == nil {
		return
	}
	for _, peer := range n.peerIDs {
		if peer == n.id {
			continue
		}
		if n.replicating[peer] {
			continue
		}
		n.replicating[peer] = true
		go n.replicateTo(peer)
	}
}

func (n *RaftNode) replicateTo(peer string) {
	defer func() {
		n.mu.Lock()
		n.replicating[peer] = false
		n.mu.Unlock()
	}()

	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		req, term, ok := n.makeAppendRequestLocked(peer)
		n.mu.Unlock()
		if !ok {
			return
		}

		resp, err := n.transport.SendAppendEntries(peer, req)
		if err != nil {
			return
		}

		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			n.resetElectionTimerLocked()
			n.mu.Unlock()
			return
		}
		if resp.Success {
			n.matchIndex[peer] = req.PrevLogIndex + len(req.Entries)
			n.nextIndex[peer] = n.matchIndex[peer] + 1
			n.advanceCommitLocked()
			n.applyCommittedLocked()
			if n.lastLogIndexLocked() > n.matchIndex[peer] {
				n.mu.Unlock()
				continue
			}
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.mu.Unlock()
	}
}

func (n *RaftNode) makeAppendRequestLocked(peer string) (*AppendEntriesRequest, int, bool) {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
		n.nextIndex[peer] = 1
	}
	if next-1 >= len(n.log) {
		next = len(n.log)
		n.nextIndex[peer] = next
	}
	prevIndex := next - 1
	entries := cloneEntries(n.log[next:])
	return &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.log[prevIndex].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}, n.currentTerm, true
}

// HandleAppendEntries is the AppendEntries RPC receiver (heartbeats and replication).
func (n *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{Term: n.currentTerm, Success: false}

	if req.Term < n.currentTerm {
		return resp
	}

	// A valid leader for this term: step down if we were candidate/leader.
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimerLocked()
	resp.Term = n.currentTerm

	if req.PrevLogIndex >= len(n.log) {
		return resp
	}
	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return resp
	}

	logDirty := false
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + i
		if idx < len(n.log) {
			if n.log[idx].Term != e.Term {
				n.log = n.log[:idx]
				n.appendEntriesLocked(req.Entries[i:], idx)
				logDirty = true
				break
			}
			continue
		}
		n.appendEntriesLocked(req.Entries[i:], idx)
		logDirty = true
		break
	}
	if logDirty {
		n.persistLocked()
	}

	if req.LeaderCommit > n.commitIndex {
		last := n.lastLogIndexLocked()
		n.commitIndex = req.LeaderCommit
		if n.commitIndex > last {
			n.commitIndex = last
		}
		n.applyCommittedLocked()
	}

	resp.Success = true
	return resp
}

func (n *RaftNode) appendEntriesLocked(entries []LogEntry, startIndex int) {
	for i, e := range entries {
		cmd := append([]byte(nil), e.Command...)
		n.log = append(n.log, LogEntry{
			Term:    e.Term,
			Index:   startIndex + i,
			Command: cmd,
		})
	}
}

// advanceCommitLocked commits the highest current-term index replicated on a majority.
// Earlier entries (including previous terms) become committed indirectly.
func (n *RaftNode) advanceCommitLocked() {
	last := n.lastLogIndexLocked()
	for idx := last; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 0
		for _, p := range n.peerIDs {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			break
		}
	}
}

func (n *RaftNode) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		msg := ApplyMsg{
			Index:   n.lastApplied,
			Command: n.log[n.lastApplied].Command,
		}
		if n.apply != nil {
			n.apply(msg)
		}
	}
}
