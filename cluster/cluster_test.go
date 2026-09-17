package cluster

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"raftkv/kv"
	"raftkv/raft"
)

func TestHandlerIsolateUnknownNode(t *testing.T) {
	c := New(DevConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(s *kv.Server) http.Handler {
		return http.NewServeMux()
	})
	c.members["n1"] = &Member{ID: "n1", Crashed: true}

	srv := httptest.NewServer(Handler(c))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/chaos/isolate/n9", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestHandlerHealAllJSON(t *testing.T) {
	c := New(DevConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(s *kv.Server) http.Handler {
		return http.NewServeMux()
	})
	c.members["n1"] = &Member{ID: "n1", Crashed: true}
	c.members["n2"] = &Member{ID: "n2", Crashed: true}
	c.members["n3"] = &Member{ID: "n3", Crashed: true}

	srv := httptest.NewServer(Handler(c))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/chaos/heal-all", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["ok"] != true {
		t.Fatalf("body = %v", body)
	}
}

func waitLeader(t *testing.T, nodes []*raft.RaftNode) *raft.RaftNode {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var found []*raft.RaftNode
		for _, n := range nodes {
			if n.State() == raft.Leader {
				found = append(found, n)
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

func TestMemoryIsolateThenPutOnRemainingLeader(t *testing.T) {
	// Uses the same Faults Isolate path the HTTP control plane calls.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	net := raft.NewMemoryNetwork()
	ids := []string{"n1", "n2", "n3"}
	addrs := map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"}
	var nodes []*raft.RaftNode
	var servers []*kv.Server
	for _, id := range ids {
		n := raft.NewNode(raft.Config{
			ID:                 id,
			PeerAddrs:          addrs,
			ElectionTimeoutMin: 40 * time.Millisecond,
			ElectionTimeoutMax: 80 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
			Logger:             logger,
		})
		net.Add(n)
		servers = append(servers, kv.NewServer(n, kv.NewStore()))
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

	leaderNode := waitLeader(t, nodes)
	var leader *kv.Server
	for _, s := range servers {
		if s.Node().ID() == leaderNode.ID() {
			leader = s
			break
		}
	}
	if err := leader.Put("color", "blue"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	net.Isolate(leader.Node().ID())
	var remaining []*raft.RaftNode
	var remainingKV []*kv.Server
	for i, n := range nodes {
		if n.ID() != leader.Node().ID() {
			remaining = append(remaining, n)
			remainingKV = append(remainingKV, servers[i])
		}
	}
	newLeaderNode := waitLeader(t, remaining)
	var newLeader *kv.Server
	for _, s := range remainingKV {
		if s.Node().ID() == newLeaderNode.ID() {
			newLeader = s
			break
		}
	}
	v, ok, err := newLeader.Get("color")
	if err != nil || !ok || v != "blue" {
		t.Fatalf("Get after isolate = %q %v %v", v, ok, err)
	}
	if err := newLeader.Put("city", "paris"); err != nil {
		t.Fatalf("Put on new leader: %v", err)
	}
}
