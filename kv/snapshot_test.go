package kv

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"raftkv/raft"
)

func startKVClusterThresh(t *testing.T, thresh int, ids ...string) (*raft.MemoryNetwork, []*Server) {
	t.Helper()
	net := raft.NewMemoryNetwork()
	servers := make([]*Server, 0, len(ids))
	nodes := make([]*raft.RaftNode, 0, len(ids))
	for _, id := range ids {
		store := NewStore()
		cfg := testRaftConfig(id, ids)
		cfg.SnapshotThreshold = thresh
		cfg.Snapshotter = store
		n := raft.NewNode(cfg)
		net.Add(n)
		srv := NewServer(n, store)
		srv.SetTimeout(2 * time.Second)
		servers = append(servers, srv)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return net, servers
}

func TestCompactedPutsStillReadable(t *testing.T) {
	_, servers := startKVClusterThresh(t, 4, "n1", "n2", "n3")
	leader := leaderOf(t, servers)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := leader.Put(k, "v"); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	waitKV(t, 2*time.Second, func() bool { return leader.Node().SnapshotIndex() > 0 })
	if leader.Status().LogLen > 6 {
		t.Fatalf("log did not shrink: %+v", leader.Status())
	}
	v, ok, err := leader.Get("k0")
	if err != nil || !ok || v != "v" {
		t.Fatalf("Get k0 after compact = %q %v %v", v, ok, err)
	}
	v, ok, err = leader.Get("k7")
	if err != nil || !ok || v != "v" {
		t.Fatalf("Get k7 after compact = %q %v %v", v, ok, err)
	}
}

func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	net, servers := startKVClusterThresh(t, 4, "n1", "n2", "n3")
	leader := leaderOf(t, servers)
	var follower *Server
	for _, s := range servers {
		if s.Node().ID() != leader.Node().ID() {
			follower = s
			break
		}
	}
	net.Isolate(follower.Node().ID())
	for i := 0; i < 10; i++ {
		if err := leader.Put(fmt.Sprintf("k%d", i), "yes"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	waitKV(t, 2*time.Second, func() bool { return leader.Node().SnapshotIndex() > 0 })

	net.Heal(follower.Node().ID())
	waitKV(t, 4*time.Second, func() bool {
		v, ok := follower.Store().Get("k0")
		return ok && v == "yes"
	})
	v, ok := follower.Store().Get("k9")
	if !ok || v != "yes" {
		t.Fatalf("follower missing k9 after snapshot catch-up, ok=%v v=%q snap=%d applied=%d",
			ok, v, follower.Node().SnapshotIndex(), follower.Node().LastApplied())
	}
}

func TestSnapshotSurvivesFullRestart(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ids := []string{"n1", "n2", "n3"}
	addrs := map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"}

	start := func(net *raft.MemoryNetwork) ([]*raft.RaftNode, []*Server) {
		var nodes []*raft.RaftNode
		var servers []*Server
		for _, id := range ids {
			store := NewStore()
			n := raft.NewNode(raft.Config{
				ID:                 id,
				PeerAddrs:          addrs,
				ElectionTimeoutMin: 40 * time.Millisecond,
				ElectionTimeoutMax: 80 * time.Millisecond,
				HeartbeatInterval:  15 * time.Millisecond,
				Logger:             logger,
				Storage:            raft.NewFileStorage(filepath.Join(dir, id)),
				Snapshotter:        store,
				SnapshotThreshold:  4,
			})
			net.Add(n)
			srv := NewServer(n, store)
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
	for i := 0; i < 8; i++ {
		if err := leader.Put(fmt.Sprintf("k%d", i), "blue"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	waitKV(t, 2*time.Second, func() bool { return leader.Node().SnapshotIndex() > 0 })
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
	waitKV(t, 3*time.Second, func() bool {
		v, ok, err := leader2.Get("k0")
		return err == nil && ok && v == "blue"
	})
	waitKV(t, 3*time.Second, func() bool {
		v, ok, err := leader2.Get("k7")
		return err == nil && ok && v == "blue"
	})
}

func waitKV(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s", timeout)
}
