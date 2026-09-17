package raft

import (
	"errors"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// State is the Raft role a node currently holds.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

var (
	// ErrNotLeader is returned when a client writes or reads on a non-leader.
	ErrNotLeader = errors.New("not leader")
	// ErrTimeout is returned when a proposal is not applied in time.
	ErrTimeout = errors.New("timeout")
	// ErrStopped is returned when the node has been shut down.
	ErrStopped = errors.New("node stopped")
	// ErrUnreachable is returned when a peer cannot be contacted.
	ErrUnreachable = errors.New("peer unreachable")
)

// LogEntry is a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// ApplyMsg is delivered to the state machine once an entry is committed.
type ApplyMsg struct {
	Index   int
	Command []byte
}

// ApplyFunc is invoked (under the Raft mutex) for each newly committed entry.
// Implementations must not call back into Raft.
type ApplyFunc func(ApplyMsg)

// Config tunes a Raft node. Timeouts should satisfy
// heartbeatInterval << electionTimeout, as in the Raft paper.
type Config struct {
	ID                 string
	PeerAddrs          map[string]string // id -> raft address; must include self
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	Logger             *slog.Logger
	Storage            Storage
}

// DefaultConfig returns paper-like timeouts (150–300 ms election, 50 ms heartbeat).
func DefaultConfig(id string, peerAddrs map[string]string) Config {
	return Config{
		ID:                 id,
		PeerAddrs:          peerAddrs,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// RaftNode is a single participant in a Raft cluster.
type RaftNode struct {
	mu sync.Mutex

	id        string
	peerAddrs map[string]string
	peerIDs   []string
	transport Transport
	apply     ApplyFunc
	storage   Storage
	logger    *slog.Logger
	rng       *rand.Rand

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	state       State
	currentTerm int
	votedFor    string
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex     map[string]int
	matchIndex    map[string]int
	replicating   map[string]bool
	votesReceived map[string]bool
	leaderID      string

	electionDeadline time.Time
	nextHeartbeat    time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewNode constructs a follower at term 0 with an empty in-memory log.
// Call SetTransport, optionally SetApply, then Start.
func NewNode(cfg Config) *RaftNode {
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 150*time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	peerIDs := make([]string, 0, len(cfg.PeerAddrs))
	for id := range cfg.PeerAddrs {
		peerIDs = append(peerIDs, id)
	}
	sort.Strings(peerIDs)

	h := fnv.New64a()
	_, _ = h.Write([]byte(cfg.ID))
	seed := int64(h.Sum64()) ^ time.Now().UnixNano()

	n := &RaftNode{
		id:                 cfg.ID,
		peerAddrs:          cfg.PeerAddrs,
		peerIDs:            peerIDs,
		logger:             logger.With("node", cfg.ID),
		rng:                rand.New(rand.NewSource(seed)),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		state:              Follower,
		votedFor:           "",
		log:                []LogEntry{{Term: 0, Index: 0}}, // dummy at index 0
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		replicating:        make(map[string]bool),
		votesReceived:      make(map[string]bool),
		stopCh:             make(chan struct{}),
	}
	n.storage = cfg.Storage
	if n.storage != nil {
		term, voted, lg, err := n.storage.Load()
		if err != nil {
			logger.Error("load raft state", "node", cfg.ID, "err", err)
		} else if len(lg) > 0 {
			n.currentTerm = term
			n.votedFor = voted
			n.log = lg
		}
	}
	return n
}

// SetTransport installs the RPC transport. Must be called before Start.
func (n *RaftNode) SetTransport(t Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = t
}

// SetApply installs the state-machine callback for committed log entries.
func (n *RaftNode) SetApply(fn ApplyFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.apply = fn
}

// Start launches the election/heartbeat ticker. The node begins as a follower.
func (n *RaftNode) Start() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.run()
	}()
}

// Stop halts the ticker and waits for in-flight loops to exit.
func (n *RaftNode) Stop() {
	n.mu.Lock()
	select {
	case <-n.stopCh:
		n.mu.Unlock()
		return
	default:
		close(n.stopCh)
	}
	n.mu.Unlock()
	n.wg.Wait()
}

func (n *RaftNode) run() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *RaftNode) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	switch n.state {
	case Follower, Candidate:
		if !now.Before(n.electionDeadline) {
			n.startElectionLocked()
		}
	case Leader:
		if !now.Before(n.nextHeartbeat) {
			n.nextHeartbeat = now.Add(n.heartbeatInterval)
			n.broadcastAppendEntriesLocked()
		}
	}
	n.applyCommittedLocked()
}

func (n *RaftNode) becomeFollowerLocked(term int, leaderID string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistLocked()
	}
	n.state = Follower
	n.leaderID = leaderID
}

func (n *RaftNode) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	last := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]int, len(n.peerIDs))
	n.matchIndex = make(map[string]int, len(n.peerIDs))
	n.replicating = make(map[string]bool, len(n.peerIDs))
	for _, p := range n.peerIDs {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}

	// A current-term no-op lets the leader commit leftover previous-term entries
	// (Raft paper §5.4.2).
	noop := LogEntry{Term: n.currentTerm, Index: last + 1, Command: nil}
	n.log = append(n.log, noop)
	n.matchIndex[n.id] = noop.Index
	n.nextIndex[n.id] = noop.Index + 1
	n.nextHeartbeat = time.Time{}

	n.persistLocked()
	n.advanceCommitLocked()
	n.applyCommittedLocked()
	n.logger.Info("became leader", "term", n.currentTerm, "log_index", noop.Index)
	n.broadcastAppendEntriesLocked()
}

func (n *RaftNode) majority() int {
	return len(n.peerIDs)/2 + 1
}

// Propose appends a command to the leader's log and kicks replication.
// Followers return ErrNotLeader. The entry is not committed until WaitApplied.
func (n *RaftNode) Propose(cmd []byte) (index int, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	select {
	case <-n.stopCh:
		return 0, 0, ErrStopped
	default:
	}
	if n.state != Leader {
		return 0, n.currentTerm, ErrNotLeader
	}
	index = n.lastLogIndexLocked() + 1
	term = n.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: append([]byte(nil), cmd...),
	}
	n.log = append(n.log, entry)
	n.persistLocked()
	n.matchIndex[n.id] = index
	n.nextIndex[n.id] = index + 1
	n.advanceCommitLocked()
	n.applyCommittedLocked()
	n.broadcastAppendEntriesLocked()
	return index, term, nil
}

// WaitApplied blocks until the given log index has been applied locally,
// the node is no longer able to commit it, or timeout elapses.
func (n *RaftNode) WaitApplied(index int, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			return ErrStopped
		case <-timer.C:
			return ErrTimeout
		case <-tick.C:
			n.mu.Lock()
			applied := n.lastApplied >= index
			committed := n.commitIndex >= index
			leader := n.state == Leader
			n.mu.Unlock()
			if applied {
				return nil
			}
			if !leader && !committed {
				return ErrNotLeader
			}
		}
	}
}

// ID returns this node's identifier.
func (n *RaftNode) ID() string { return n.id }

// State returns the current Raft role.
func (n *RaftNode) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// IsLeader reports whether this node is currently the leader.
func (n *RaftNode) IsLeader() bool {
	return n.State() == Leader
}

// CurrentTerm returns the latest term the node has seen.
func (n *RaftNode) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// VotedFor returns the candidate this node voted for in the current term, or "".
func (n *RaftNode) VotedFor() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

// LeaderID returns the last known leader, or "" if unknown.
func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderAddr returns the raft address of the last known leader, if any.
func (n *RaftNode) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return ""
	}
	return n.peerAddrs[n.leaderID]
}

// CommitIndex is the highest log index known to be committed.
func (n *RaftNode) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied is the highest log index applied to the local state machine.
func (n *RaftNode) LastApplied() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// LastLogIndex is the index of the last stored log entry.
func (n *RaftNode) LastLogIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogIndexLocked()
}

// LogSnapshot returns a copy of the in-memory log (including the dummy entry).
func (n *RaftNode) LogSnapshot() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneEntries(n.log)
}

// PeerIDs returns the configured cluster membership, sorted.
func (n *RaftNode) PeerIDs() []string {
	out := make([]string, len(n.peerIDs))
	copy(out, n.peerIDs)
	return out
}

// PeerAddr returns the raft listen address for a peer.
func (n *RaftNode) PeerAddr(id string) string {
	return n.peerAddrs[id]
}

func cloneEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = e
		if e.Command != nil {
			out[i].Command = append([]byte(nil), e.Command...)
		}
	}
	return out
}
