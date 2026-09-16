package raft

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(id string, peers ...string) Config {
	addrs := make(map[string]string, len(peers))
	for _, p := range peers {
		addrs[p] = p
	}
	return Config{
		ID:                 id,
		PeerAddrs:          addrs,
		ElectionTimeoutMin: 40 * time.Millisecond,
		ElectionTimeoutMax: 80 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
		Logger:             quietLogger(),
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for condition", timeout)
}

func waitForUniqueLeader(t *testing.T, nodes []*RaftNode) *RaftNode {
	t.Helper()
	var leader *RaftNode
	waitUntil(t, 3*time.Second, func() bool {
		var found []*RaftNode
		for _, n := range nodes {
			if n.State() == Leader {
				found = append(found, n)
			}
		}
		if len(found) == 1 {
			leader = found[0]
			return true
		}
		return false
	})
	return leader
}

func startMemoryCluster(t *testing.T, ids ...string) (*MemoryNetwork, []*RaftNode) {
	t.Helper()
	net := NewMemoryNetwork()
	nodes := make([]*RaftNode, 0, len(ids))
	for _, id := range ids {
		n := NewNode(testConfig(id, ids...))
		net.Add(n)
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
	return net, nodes
}
