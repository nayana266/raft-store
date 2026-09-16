package kv

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"raftkv/raft"
)

func testRaftConfig(id string, peers []string) raft.Config {
	addrs := map[string]string{}
	for _, p := range peers {
		addrs[p] = p
	}
	return raft.Config{
		ID:                 id,
		PeerAddrs:          addrs,
		ElectionTimeoutMin: 40 * time.Millisecond,
		ElectionTimeoutMax: 80 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func startKVCluster(t *testing.T, ids ...string) (*raft.MemoryNetwork, []*Server) {
	t.Helper()
	net := raft.NewMemoryNetwork()
	servers := make([]*Server, 0, len(ids))
	nodes := make([]*raft.RaftNode, 0, len(ids))
	for _, id := range ids {
		n := raft.NewNode(testRaftConfig(id, ids))
		net.Add(n)
		srv := NewServer(n, NewStore())
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

func leaderOf(t *testing.T, servers []*Server) *Server {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var found []*Server
		for _, s := range servers {
			if s.Node().State() == raft.Leader {
				found = append(found, s)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no unique leader")
	return nil
}

func TestPutGetOnLeader(t *testing.T) {
	_, servers := startKVCluster(t, "n1", "n2", "n3")
	leader := leaderOf(t, servers)

	if err := leader.Put("color", "blue"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := leader.Get("color")
	if err != nil || !ok || v != "blue" {
		t.Fatalf("Get = %q %v %v, want blue true nil", v, ok, err)
	}
	_, ok, err = leader.Get("missing")
	if err != nil || ok {
		t.Fatalf("missing Get = ok=%v err=%v", ok, err)
	}
}

func TestFollowerPutAndGetRedirect(t *testing.T) {
	_, servers := startKVCluster(t, "n1", "n2", "n3")
	leader := leaderOf(t, servers)

	var follower *Server
	for _, s := range servers {
		if s.Node().ID() != leader.Node().ID() {
			follower = s
			break
		}
	}

	err := follower.Put("k", "v")
	var nl *NotLeaderError
	if !errors.As(err, &nl) {
		t.Fatalf("Put err = %v, want NotLeaderError", err)
	}
	if nl.LeaderID != leader.Node().ID() {
		t.Fatalf("hint = %q, want %s", nl.LeaderID, leader.Node().ID())
	}

	_, _, err = follower.Get("k")
	if !errors.As(err, &nl) {
		t.Fatalf("Get err = %v, want NotLeaderError", err)
	}
}

func TestPutReplicatedToFollowersAppliedStore(t *testing.T) {
	_, servers := startKVCluster(t, "n1", "n2", "n3")
	leader := leaderOf(t, servers)
	if err := leader.Put("city", "paris"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, s := range servers {
			if v, ok := s.Store().Get("city"); !ok || v != "paris" {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("followers did not apply the committed Put")
}

func TestPutSurvivesLeaderIsolation(t *testing.T) {
	net, servers := startKVCluster(t, "n1", "n2", "n3")
	leader := leaderOf(t, servers)
	if err := leader.Put("durable", "yes"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	net.Isolate(leader.Node().ID())
	var remaining []*Server
	for _, s := range servers {
		if s.Node().ID() != leader.Node().ID() {
			remaining = append(remaining, s)
		}
	}
	newLeader := leaderOf(t, remaining)
	v, ok, err := newLeader.Get("durable")
	if err != nil || !ok || v != "yes" {
		t.Fatalf("after failover Get = %q %v %v", v, ok, err)
	}
}
