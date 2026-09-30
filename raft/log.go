package raft

func (n *RaftNode) lastLogIndexLocked() int {
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTermLocked() int {
	return n.log[len(n.log)-1].Term
}

// lastIncludedIndexLocked is the snapshot's last included raft index (log[0]).
func (n *RaftNode) lastIncludedIndexLocked() int {
	return n.log[0].Index
}

func (n *RaftNode) lastIncludedTermLocked() int {
	return n.log[0].Term
}

// sliceIndexLocked maps a raft log index onto n.log. -1 means the index was
// compacted away; >= len(n.log) means it has not been appended yet.
func (n *RaftNode) sliceIndexLocked(raftIndex int) int {
	return raftIndex - n.log[0].Index
}

func (n *RaftNode) entryAtLocked(raftIndex int) (LogEntry, bool) {
	si := n.sliceIndexLocked(raftIndex)
	if si < 0 || si >= len(n.log) {
		return LogEntry{}, false
	}
	return n.log[si], true
}

func (n *RaftNode) termAtLocked(raftIndex int) (int, bool) {
	e, ok := n.entryAtLocked(raftIndex)
	if !ok {
		return 0, false
	}
	return e.Term, true
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
		if n.shouldInstallSnapshotLocked(peer) {
			req := n.installSnapshotRequestLocked()
			term := n.currentTerm
			n.mu.Unlock()

			resp, err := n.transport.SendInstallSnapshot(peer, req)
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
			n.matchIndex[peer] = req.LastIncludedIndex
			n.nextIndex[peer] = req.LastIncludedIndex + 1
			n.advanceCommitLocked()
			n.applyCommittedLocked()
			more := n.lastLogIndexLocked() > n.matchIndex[peer]
			n.mu.Unlock()
			if more {
				continue
			}
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
		snap := n.lastIncludedIndexLocked()
		if n.nextIndex[peer] > snap+1 {
			n.nextIndex[peer]--
		} else if snap > 0 {
			n.nextIndex[peer] = snap
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.mu.Unlock()
	}
}

func (n *RaftNode) shouldInstallSnapshotLocked(peer string) bool {
	snap := n.lastIncludedIndexLocked()
	return snap > 0 && n.nextIndex[peer] <= snap
}

func (n *RaftNode) makeAppendRequestLocked(peer string) (*AppendEntriesRequest, int, bool) {
	next := n.nextIndex[peer]
	snap := n.lastIncludedIndexLocked()
	if next <= snap {
		return nil, n.currentTerm, false
	}
	last := n.lastLogIndexLocked()
	if next > last+1 {
		next = last + 1
		n.nextIndex[peer] = next
	}
	prevIndex := next - 1
	prevTerm, ok := n.termAtLocked(prevIndex)
	if !ok {
		return nil, n.currentTerm, false
	}
	start := n.sliceIndexLocked(next)
	if start < 1 {
		start = 1
	}
	if start > len(n.log) {
		start = len(n.log)
	}
	entries := cloneEntries(n.log[start:])
	return &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
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
	n.joining = false
	resp.Term = n.currentTerm

	if req.PrevLogIndex < n.lastIncludedIndexLocked() {
		return resp
	}
	prev, ok := n.entryAtLocked(req.PrevLogIndex)
	if !ok {
		return resp
	}
	if prev.Term != req.PrevLogTerm {
		return resp
	}

	logDirty := false
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + i
		existing, exists := n.entryAtLocked(idx)
		if exists {
			if existing.Term != e.Term {
				si := n.sliceIndexLocked(idx)
				if si < 1 {
					si = 1
				}
				n.log = n.log[:si]
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
		n.rebuildMembershipLocked()
		n.notifyPeersLocked()
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
		var ch *MembershipChange
		if e.Change != nil {
			cp := *e.Change
			cp.Peers = cloneStringMap(e.Change.Peers)
			cp.HTTPAddrs = cloneStringMap(e.Change.HTTPAddrs)
			ch = &cp
		}
		n.log = append(n.log, LogEntry{
			Term:    e.Term,
			Index:   startIndex + i,
			Command: cmd,
			Change:  ch,
		})
	}
}

// advanceCommitLocked commits the highest current-term index replicated on a majority.
// Earlier entries (including previous terms) become committed indirectly.
func (n *RaftNode) advanceCommitLocked() {
	last := n.lastLogIndexLocked()
	for idx := last; idx > n.commitIndex; idx-- {
		term, ok := n.termAtLocked(idx)
		if !ok || term != n.currentTerm {
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
		next := n.lastApplied + 1
		e, ok := n.entryAtLocked(next)
		if !ok {
			break
		}
		n.lastApplied = next
		if n.apply != nil && e.Change == nil {
			n.apply(ApplyMsg{Index: next, Command: e.Command})
		}
	}
	n.maybeCompactLocked()
}
