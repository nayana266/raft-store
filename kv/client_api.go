package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"raftkv/raft"
)

// Command is the replicated state-machine operation stored in the Raft log.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

const opPut = "Put"

// NotLeaderError tells a client which node currently holds leadership.
type NotLeaderError struct {
	LeaderID   string
	LeaderAddr string
	LeaderHTTP string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not leader (unknown leader)"
	}
	if e.LeaderHTTP != "" {
		return fmt.Sprintf("not leader; try %s (%s)", e.LeaderID, e.LeaderHTTP)
	}
	if e.LeaderAddr != "" {
		return fmt.Sprintf("not leader; try %s (%s)", e.LeaderID, e.LeaderAddr)
	}
	return "not leader; try " + e.LeaderID
}

func (e *NotLeaderError) Unwrap() error { return raft.ErrNotLeader }

// Server is the client-facing KV API. Only the Raft leader accepts Get/Put.
type Server struct {
	raft      *raft.RaftNode
	store     *Store
	timeout   time.Duration
	httpAddrs map[string]string
}

// NewServer wraps a Raft node and in-memory store. It registers the apply callback.
func NewServer(node *raft.RaftNode, store *Store) *Server {
	if store == nil {
		store = NewStore()
	}
	s := &Server{
		raft:    node,
		store:   store,
		timeout: 3 * time.Second,
	}
	node.SetApply(s.onApply)
	return s
}

// SetTimeout overrides the wait for a Put to commit.
func (s *Server) SetTimeout(d time.Duration) { s.timeout = d }

// SetHTTPAddrs records HTTP advertise addresses (id → host:port) used in redirects.
func (s *Server) SetHTTPAddrs(addrs map[string]string) { s.httpAddrs = addrs }

func (s *Server) onApply(msg raft.ApplyMsg) {
	if len(msg.Command) == 0 {
		return
	}
	var cmd Command
	if err := json.Unmarshal(msg.Command, &cmd); err != nil {
		return
	}
	if cmd.Op == opPut {
		s.store.Put(cmd.Key, cmd.Value)
	}
}

// Put replicates a write through Raft. Followers return NotLeaderError.
func (s *Server) Put(key, value string) error {
	if key == "" {
		return errors.New("empty key")
	}
	if !s.raft.IsLeader() {
		return s.notLeader()
	}
	body, err := json.Marshal(Command{Op: opPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	idx, _, err := s.raft.Propose(body)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.notLeader()
		}
		return err
	}
	if err := s.raft.WaitApplied(idx, s.timeout); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.notLeader()
		}
		return err
	}
	return nil
}

// Get reads the locally applied store. Only the leader serves reads.
func (s *Server) Get(key string) (string, bool, error) {
	if !s.raft.IsLeader() {
		return "", false, s.notLeader()
	}
	v, ok := s.store.Get(key)
	return v, ok, nil
}

// Status is a snapshot of Raft + KV for debugging and the HTTP /status endpoint.
type Status struct {
	ID           string   `json:"id"`
	State        string   `json:"state"`
	Term         int      `json:"term"`
	LeaderID     string   `json:"leader_id"`
	LeaderAddr   string   `json:"leader_addr,omitempty"`
	LeaderHTTP   string   `json:"leader_http,omitempty"`
	VotedFor     string   `json:"voted_for"`
	CommitIndex  int      `json:"commit_index"`
	LastApplied  int      `json:"last_applied"`
	LastLogIndex int      `json:"last_log_index"`
	KVSize       int      `json:"kv_size"`
	Peers        []string `json:"peers"`
}

// Status reports the current node.
func (s *Server) Status() Status {
	n := s.raft
	leaderID := n.LeaderID()
	return Status{
		ID:           n.ID(),
		State:        n.State().String(),
		Term:         n.CurrentTerm(),
		LeaderID:     leaderID,
		LeaderAddr:   n.LeaderAddr(),
		LeaderHTTP:   s.httpAddr(leaderID),
		VotedFor:     n.VotedFor(),
		CommitIndex:  n.CommitIndex(),
		LastApplied:  n.LastApplied(),
		LastLogIndex: n.LastLogIndex(),
		KVSize:       s.store.Len(),
		Peers:        n.PeerIDs(),
	}
}

func (s *Server) notLeader() error {
	id := s.raft.LeaderID()
	return &NotLeaderError{
		LeaderID:   id,
		LeaderAddr: s.raft.LeaderAddr(),
		LeaderHTTP: s.httpAddr(id),
	}
}

func (s *Server) httpAddr(id string) string {
	if id == "" || s.httpAddrs == nil {
		return ""
	}
	return s.httpAddrs[id]
}

// Node returns the underlying Raft node.
func (s *Server) Node() *raft.RaftNode { return s.raft }

// Store returns the applied key-value map.
func (s *Server) Store() *Store { return s.store }
