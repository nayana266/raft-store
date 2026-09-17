package raft

// Snapshotter is the state machine's snapshot hook. Snapshot must capture
// every command applied so far; Restore replaces the state machine entirely.
// Implementations must not call back into Raft (they run under the Raft mutex).
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

// InstallSnapshotRequest is the InstallSnapshot RPC argument list (Raft §7).
// We send the whole snapshot in one RPC (no chunking).
type InstallSnapshotRequest struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotResponse is the InstallSnapshot RPC reply.
type InstallSnapshotResponse struct {
	Term int
}

func (n *RaftNode) installSnapshotRequestLocked() *InstallSnapshotRequest {
	return &InstallSnapshotRequest{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.lastIncludedIndexLocked(),
		LastIncludedTerm:  n.lastIncludedTermLocked(),
		Data:              append([]byte(nil), n.snapshot...),
	}
}

func (n *RaftNode) maybeCompactLocked() {
	if n.snapshotter == nil || n.snapshotThreshold <= 0 {
		return
	}
	applied := n.lastApplied - n.lastIncludedIndexLocked()
	if applied < n.snapshotThreshold {
		return
	}
	n.compactLocked(n.lastApplied)
}

func (n *RaftNode) compactLocked(index int) {
	if index <= n.lastIncludedIndexLocked() {
		return
	}
	e, ok := n.entryAtLocked(index)
	if !ok {
		return
	}
	data, err := n.snapshotter.Snapshot()
	if err != nil {
		n.logger.Error("snapshot failed", "err", err)
		return
	}
	si := n.sliceIndexLocked(index)
	if si < 0 || si >= len(n.log) {
		return
	}
	suffix := cloneEntries(n.log[si+1:])
	n.log = append([]LogEntry{{Term: e.Term, Index: index}}, suffix...)
	n.snapshot = data
	n.persistLocked()
	n.logger.Info("compacted log",
		"last_included_index", index,
		"log_len", len(n.log)-1)
}

// HandleInstallSnapshot is the InstallSnapshot RPC receiver (Raft §7).
func (n *RaftNode) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &InstallSnapshotResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimerLocked()
	resp.Term = n.currentTerm

	if req.LastIncludedIndex <= n.lastIncludedIndexLocked() {
		return resp
	}

	n.log = []LogEntry{{Term: req.LastIncludedTerm, Index: req.LastIncludedIndex}}
	n.snapshot = append([]byte(nil), req.Data...)
	if n.snapshotter != nil && len(n.snapshot) > 0 {
		if err := n.snapshotter.Restore(n.snapshot); err != nil {
			n.logger.Error("restore snapshot", "err", err)
		}
	}
	n.lastApplied = req.LastIncludedIndex
	if n.commitIndex < req.LastIncludedIndex {
		n.commitIndex = req.LastIncludedIndex
	}
	n.persistLocked()
	n.logger.Info("installed snapshot",
		"last_included_index", req.LastIncludedIndex,
		"from", req.LeaderID)
	return resp
}

func (n *RaftNode) restoreSnapshotLocked() {
	if n.snapshotter == nil || len(n.snapshot) == 0 {
		return
	}
	if err := n.snapshotter.Restore(n.snapshot); err != nil {
		n.logger.Error("restore snapshot", "err", err)
		return
	}
	snapIdx := n.lastIncludedIndexLocked()
	if n.lastApplied < snapIdx {
		n.lastApplied = snapIdx
	}
	if n.commitIndex < snapIdx {
		n.commitIndex = snapIdx
	}
}

// SetSnapshotter installs the state-machine snapshot hook. If a snapshot was
// already loaded from disk, it is restored immediately.
func (n *RaftNode) SetSnapshotter(s Snapshotter) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotter = s
	n.restoreSnapshotLocked()
}

// SnapshotIndex is lastIncludedIndex of the latest snapshot (0 if none).
func (n *RaftNode) SnapshotIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIncludedIndexLocked()
}

// LogLen is the number of real (non-dummy) entries still held in memory.
func (n *RaftNode) LogLen() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) == 0 {
		return 0
	}
	return len(n.log) - 1
}
