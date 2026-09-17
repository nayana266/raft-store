package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"raftkv/kv"
	"raftkv/raft"
)

// Config describes a static cluster (ids and listen addresses).
type Config struct {
	IDs       []string
	RaftAddrs map[string]string
	HTTPAddrs map[string]string
}

// DevConfig is the three-node layout used by `go run ./cmd -dev`.
func DevConfig() Config {
	return Config{
		IDs: []string{"n1", "n2", "n3"},
		RaftAddrs: map[string]string{
			"n1": "127.0.0.1:19101",
			"n2": "127.0.0.1:19102",
			"n3": "127.0.0.1:19103",
		},
		HTTPAddrs: map[string]string{
			"n1": "127.0.0.1:18101",
			"n2": "127.0.0.1:18102",
			"n3": "127.0.0.1:18103",
		},
	}
}

// Member is one running (or crashed) node.
type Member struct {
	ID      string
	Node    *raft.RaftNode
	KV      *kv.Server
	gs      *grpc.Server
	httpSrv *http.Server
	grpcTr  *raft.GRPCTransport
	Crashed bool
}

// Cluster owns the nodes plus a shared fault injector for chaos.
type Cluster struct {
	mu      sync.Mutex
	cfg     Config
	logger  *slog.Logger
	faults  *raft.Faults
	http    func(*kv.Server) http.Handler
	members map[string]*Member
}

// New builds a cluster but does not start nodes. httpHandler is the per-node mux
// (Get/Put/status). Pass nil and SetHTTPHandler before Start, or use StartDev.
func New(cfg Config, logger *slog.Logger, httpHandler func(*kv.Server) http.Handler) *Cluster {
	if logger == nil {
		logger = slog.Default()
	}
	return &Cluster{
		cfg:     cfg,
		logger:  logger,
		faults:  raft.NewFaults(),
		http:    httpHandler,
		members: make(map[string]*Member),
	}
}

// Start boots every node.
func (c *Cluster) Start() error {
	for _, id := range c.cfg.IDs {
		if err := c.startMember(id); err != nil {
			c.Stop()
			return err
		}
	}
	return nil
}

// Stop crashes every member and clears faults.
func (c *Cluster) Stop() {
	c.mu.Lock()
	ids := append([]string(nil), c.cfg.IDs...)
	c.mu.Unlock()
	for _, id := range ids {
		_ = c.Crash(id)
	}
	c.faults.HealAll()
}

func (c *Cluster) startMember(id string) error {
	c.mu.Lock()
	if m, ok := c.members[id]; ok && !m.Crashed {
		c.mu.Unlock()
		return fmt.Errorf("%s is already running", id)
	}
	handler := c.http
	logger := c.logger
	cfg := c.cfg
	c.mu.Unlock()

	if handler == nil {
		return errors.New("missing HTTP handler")
	}
	raftAddr, ok := cfg.RaftAddrs[id]
	if !ok {
		return fmt.Errorf("no raft address for %s", id)
	}
	httpAddr := cfg.HTTPAddrs[id]

	rCfg := raft.DefaultConfig(id, cfg.RaftAddrs)
	rCfg.Logger = logger
	node := raft.NewNode(rCfg)
	store := kv.NewStore()
	kvSrv := kv.NewServer(node, store)
	kvSrv.SetHTTPAddrs(cfg.HTTPAddrs)

	grpcTr := raft.NewGRPCTransport(cfg.RaftAddrs)
	node.SetTransport(raft.NewFilterTransport(id, grpcTr, c.faults))

	var gs *grpc.Server
	var err error
	for i := 0; i < 25; i++ {
		gs, _, err = raft.ListenAndServeGRPC(raftAddr, node, func(gs *grpc.Server) {
			kv.RegisterGRPC(gs, kvSrv)
		})
		if err == nil {
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	if err != nil {
		grpcTr.Close()
		return fmt.Errorf("grpc listen %s: %w", raftAddr, err)
	}

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler(kvSrv),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", "id", id, "addr", httpAddr, "err", err)
		}
	}()

	node.Start()

	c.mu.Lock()
	c.members[id] = &Member{
		ID:      id,
		Node:    node,
		KV:      kvSrv,
		gs:      gs,
		httpSrv: httpSrv,
		grpcTr:  grpcTr,
		Crashed: false,
	}
	c.mu.Unlock()
	logger.Info("node started", "id", id, "raft", raftAddr, "http", httpAddr)
	return nil
}

// Isolate partitions id from the rest of the cluster. The process stays up
// (a "zombie" leader can still answer HTTP until it times out).
func (c *Cluster) Isolate(id string) error {
	if err := c.mustExist(id); err != nil {
		return err
	}
	c.faults.Isolate(id)
	c.logger.Info("chaos: isolated", "id", id)
	return nil
}

// Heal restores Raft RPCs to and from id.
func (c *Cluster) Heal(id string) error {
	if err := c.mustExist(id); err != nil {
		return err
	}
	c.faults.Heal(id)
	c.logger.Info("chaos: healed", "id", id)
	return nil
}

// Partition cuts the cable between a and b.
func (c *Cluster) Partition(a, b string) error {
	if err := c.mustExist(a); err != nil {
		return err
	}
	if err := c.mustExist(b); err != nil {
		return err
	}
	c.faults.Disconnect(a, b)
	c.logger.Info("chaos: partitioned", "a", a, "b", b)
	return nil
}

// HealAll clears isolations and pairwise cuts. Crashed nodes stay crashed.
func (c *Cluster) HealAll() {
	c.faults.HealAll()
	c.logger.Info("chaos: healed all partitions")
}

// Crash stops Raft, gRPC, and HTTP for id — as if the process was killed.
func (c *Cluster) Crash(id string) error {
	c.mu.Lock()
	m, ok := c.members[id]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("unknown node %s", id)
	}
	if m.Crashed {
		c.mu.Unlock()
		return nil
	}
	m.Crashed = true
	node, gs, httpSrv, grpcTr := m.Node, m.gs, m.httpSrv, m.grpcTr
	c.mu.Unlock()

	if httpSrv != nil {
		_ = httpSrv.Close()
	}
	if gs != nil {
		gs.Stop()
	}
	if node != nil {
		node.Stop()
	}
	if grpcTr != nil {
		grpcTr.Close()
	}
	c.logger.Info("chaos: crashed", "id", id)
	return nil
}

// Restart boots a crashed node with a fresh in-memory log. The leader will
// catch it up via AppendEntries (there is still no disk persistence).
func (c *Cluster) Restart(id string) error {
	c.mu.Lock()
	m, ok := c.members[id]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	if !m.Crashed {
		return fmt.Errorf("%s is still running; crash it first", id)
	}
	c.faults.Heal(id)
	time.Sleep(80 * time.Millisecond)
	return c.startMember(id)
}

func (c *Cluster) mustExist(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.members[id]; !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	return nil
}

// View is a JSON-friendly snapshot of one node for /cluster.
type View struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	LeaderID    string `json:"leader_id"`
	LeaderHTTP  string `json:"leader_http,omitempty"`
	CommitIndex int    `json:"commit_index"`
	KVSize      int    `json:"kv_size"`
	HTTP        string `json:"http"`
	Isolated    bool   `json:"isolated"`
	Crashed     bool   `json:"crashed"`
}

// Snapshot returns every node's current chaos + Raft status.
func (c *Cluster) Snapshot() []View {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]View, 0, len(c.cfg.IDs))
	for _, id := range c.cfg.IDs {
		m := c.members[id]
		v := View{
			ID:       id,
			HTTP:     c.cfg.HTTPAddrs[id],
			Isolated: c.faults.Isolated(id),
		}
		if m == nil || m.Crashed || m.KV == nil {
			v.State = "crashed"
			v.Crashed = true
			out = append(out, v)
			continue
		}
		st := m.KV.Status()
		v.State = st.State
		v.Term = st.Term
		v.LeaderID = st.LeaderID
		v.LeaderHTTP = st.LeaderHTTP
		v.CommitIndex = st.CommitIndex
		v.KVSize = st.KVSize
		out = append(out, v)
	}
	return out
}

// LeaderHTTP returns the HTTP address of the current unique leader, if any.
func (c *Cluster) LeaderHTTP() string {
	for _, v := range c.Snapshot() {
		if v.State == "leader" && !v.Crashed && !v.Isolated {
			return v.HTTP
		}
	}
	// Isolated zombie may still think it is leader; prefer a majority leader.
	for _, v := range c.Snapshot() {
		if v.State == "leader" && !v.Crashed {
			return v.HTTP
		}
	}
	return ""
}
