package raft

import "time"

func (n *RaftNode) resetElectionTimerLocked() {
	span := n.electionTimeoutMax - n.electionTimeoutMin
	jitter := time.Duration(0)
	if span > 0 {
		jitter = time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = time.Now().Add(n.electionTimeoutMin + jitter)
}

// startElectionLocked converts this node to a candidate, votes for itself, and
// requests votes from every other peer (Raft paper §5.2).
func (n *RaftNode) startElectionLocked() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.votesReceived = map[string]bool{n.id: true}
	n.resetElectionTimerLocked()

	term := n.currentTerm
	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndexLocked(),
		LastLogTerm:  n.lastLogTermLocked(),
	}

	n.logger.Info("starting election", "term", term, "last_log_index", req.LastLogIndex)

	if len(n.votesReceived) >= n.majority() {
		n.becomeLeaderLocked()
		return
	}

	for _, peer := range n.peerIDs {
		if peer == n.id {
			continue
		}
		go n.sendRequestVote(peer, req)
	}
}

func (n *RaftNode) sendRequestVote(peer string, req *RequestVoteRequest) {
	if n.transport == nil {
		return
	}
	resp, err := n.transport.SendRequestVote(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollowerLocked(resp.Term, "")
		n.resetElectionTimerLocked()
		return
	}
	if n.state != Candidate || n.currentTerm != req.Term {
		return
	}
	if !resp.VoteGranted {
		return
	}
	n.votesReceived[peer] = true
	n.logger.Info("received vote", "from", peer, "term", n.currentTerm, "votes", len(n.votesReceived))
	if len(n.votesReceived) >= n.majority() {
		n.becomeLeaderLocked()
	}
}

// HandleRequestVote is the RequestVote RPC receiver (Raft paper Figure 2).
func (n *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}

	resp.Term = n.currentTerm
	upToDate := n.isLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	if canVote && upToDate {
		n.votedFor = req.CandidateID
		n.resetElectionTimerLocked()
		resp.VoteGranted = true
		n.logger.Info("granted vote", "to", req.CandidateID, "term", n.currentTerm)
	}
	return resp
}

// isLogUpToDateLocked implements the Raft "up-to-date" rule:
// compare last log terms first; if equal, the longer log is more up-to-date.
func (n *RaftNode) isLogUpToDateLocked(lastLogIndex, lastLogTerm int) bool {
	myTerm := n.lastLogTermLocked()
	myIndex := n.lastLogIndexLocked()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}
