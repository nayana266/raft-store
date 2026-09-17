package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raftkv/kv"
	"raftkv/raft"
)

func TestFollowerHTTPForwardsPutAndGet(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	addrs := map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"}
	net := raft.NewMemoryNetwork()
	servers := make([]*kv.Server, 0, 3)
	nodes := make([]*raft.RaftNode, 0, 3)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		store := kv.NewStore()
		n := raft.NewNode(raft.Config{
			ID:                 id,
			PeerAddrs:          addrs,
			ElectionTimeoutMin: 40 * time.Millisecond,
			ElectionTimeoutMax: 80 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
			Logger:             logger,
			Snapshotter:        store,
		})
		net.Add(n)
		srv := kv.NewServer(n, store)
		srv.SetTimeout(2 * time.Second)
		servers = append(servers, srv)
		nodes = append(nodes, n)
	}

	httpSrvs := make([]*httptest.Server, 0, 3)
	httpAddrs := map[string]string{}
	for i, srv := range servers {
		hs := httptest.NewServer(httpMux(srv))
		httpSrvs = append(httpSrvs, hs)
		httpAddrs[ids[i]] = strings.TrimPrefix(hs.URL, "http://")
	}
	t.Cleanup(func() {
		for _, hs := range httpSrvs {
			hs.Close()
		}
		for _, n := range nodes {
			n.Stop()
		}
	})
	for _, srv := range servers {
		srv.SetHTTPAddrs(httpAddrs)
	}
	for _, n := range nodes {
		n.Start()
	}

	deadline := time.Now().Add(3 * time.Second)
	var leader, follower *kv.Server
	var followerHTTP string
	for time.Now().Before(deadline) {
		leader, follower = nil, nil
		for i, s := range servers {
			if s.Node().State() == raft.Leader {
				leader = s
			} else if follower == nil {
				follower = s
				followerHTTP = httpSrvs[i].URL
			}
		}
		if leader != nil && follower != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil || follower == nil {
		t.Fatal("no leader/follower")
	}

	putReq, err := http.NewRequest(http.MethodPut, followerHTTP+"/kv/color", strings.NewReader("blue"))
	if err != nil {
		t.Fatal(err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	defer putResp.Body.Close()
	putBody, _ := io.ReadAll(putResp.Body)
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("forwarded Put status %d body %s", putResp.StatusCode, putBody)
	}

	getResp, err := http.Get(followerHTTP + "/kv/color")
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	getBody, _ := io.ReadAll(getResp.Body)
	if getResp.StatusCode != http.StatusOK || !strings.Contains(string(getBody), `"blue"`) {
		t.Fatalf("forwarded Get status %d body %s", getResp.StatusCode, getBody)
	}
}
