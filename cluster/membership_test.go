package cluster

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"raftkv/kv"
)

func TestAddThenRemoveNode(t *testing.T) {
	cfg := Config{
		IDs: []string{"n1", "n2", "n3"},
		RaftAddrs: map[string]string{
			"n1": "127.0.0.1:19301",
			"n2": "127.0.0.1:19302",
			"n3": "127.0.0.1:19303",
		},
		HTTPAddrs: map[string]string{
			"n1": "127.0.0.1:18301",
			"n2": "127.0.0.1:18302",
			"n3": "127.0.0.1:18303",
		},
		DataDir: t.TempDir(),
	}
	c := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), func(s *kv.Server) http.Handler {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(s.Status())
		})
		return mux
	})
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.leaderNode() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c.leaderNode() == nil {
		t.Fatal("no leader")
	}

	if err := c.Add("n4", "127.0.0.1:19304", "127.0.0.1:18304"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitCluster(t, 3*time.Second, func() bool {
		for _, v := range c.Snapshot() {
			if v.ID == "n4" && !v.Crashed {
				return true
			}
		}
		return false
	})
	found := false
	for _, v := range c.Snapshot() {
		if v.ID == "n4" && !v.Crashed {
			found = true
		}
	}
	if !found {
		t.Fatal("n4 missing from cluster view")
	}

	if err := c.Remove("n4"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	for _, v := range c.Snapshot() {
		if v.ID == "n4" {
			t.Fatal("n4 still listed after remove")
		}
	}
}

func waitCluster(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("timed out")
}
