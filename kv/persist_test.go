package kv

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"raftkv/raft"
)

func TestCommittedPutSurvivesFullRestart(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ids := []string{"n1", "n2", "n3"}
	addrs := map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"}

	start := func(net *raft.MemoryNetwork) ([]*raft.RaftNode, []*Server) {
		var nodes []*raft.RaftNode
		var servers []*Server
		for _, id := range ids {
			n := raft.NewNode(raft.Config{
				ID:                 id,
				PeerAddrs:          addrs,
				ElectionTimeoutMin: 40 * time.Millisecond,
				ElectionTimeoutMax: 80 * time.Millisecond,
				HeartbeatInterval:  15 * time.Millisecond,
				Logger:             logger,
				Storage:            raft.NewFileStorage(filepath.Join(dir, id)),
			})
			net.Add(n)
			srv := NewServer(n, NewStore())
			srv.SetTimeout(2 * time.Second)
			nodes = append(nodes, n)
			servers = append(servers, srv)
			n.Start()
		}
		return nodes, servers
	}

	net := raft.NewMemoryNetwork()
	nodes, servers := start(net)
	leader := leaderOf(t, servers)
	if err := leader.Put("color", "blue"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, n := range nodes {
		n.Stop()
	}

	net2 := raft.NewMemoryNetwork()
	_, servers2 := start(net2)
	t.Cleanup(func() {
		for _, s := range servers2 {
			s.Node().Stop()
		}
	})

	leader2 := leaderOf(t, servers2)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, err := leader2.Get("color")
		if err == nil && ok && v == "blue" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	v, ok, err := leader2.Get("color")
	t.Fatalf("after restart Get = %q ok=%v err=%v, want blue", v, ok, err)
}
