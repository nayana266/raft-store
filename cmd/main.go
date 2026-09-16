package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"raftkv/kv"
	"raftkv/raft"
)

func main() {
	var (
		id        = flag.String("id", "n1", "node id")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:19101", "gRPC listen address for Raft and KV RPCs")
		httpAddr  = flag.String("http-addr", "127.0.0.1:18101", "HTTP listen address for Get/Put/status")
		peers     = flag.String("peers", "", "cluster membership as id=host:port,id=host:port (must include self)")
		httpPeers = flag.String("http-peers", "", "optional HTTP advertise map id=host:port,id=host:port for redirects")
		dev       = flag.Bool("dev", false, "start a 3-node in-process cluster on ports 19101-19103 / 18101-18103")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if *dev {
		if err := runDevCluster(logger); err != nil {
			logger.Error("dev cluster failed", "err", err)
			os.Exit(1)
		}
		return
	}

	peerAddrs := parsePeers(*peers)
	if len(peerAddrs) == 0 {
		peerAddrs = map[string]string{*id: *raftAddr}
	}
	if _, ok := peerAddrs[*id]; !ok {
		peerAddrs[*id] = *raftAddr
	}

	httpAddrs := parsePeers(*httpPeers)
	if *httpAddr != "" {
		if httpAddrs == nil {
			httpAddrs = map[string]string{}
		}
		httpAddrs[*id] = *httpAddr
	}

	if err := runNode(*id, *raftAddr, *httpAddr, peerAddrs, httpAddrs, logger); err != nil {
		logger.Error("node failed", "err", err)
		os.Exit(1)
	}
}

func runNode(id, raftAddr, httpAddr string, peerAddrs, httpAddrs map[string]string, logger *slog.Logger) error {
	node, _, gs, httpSrv, err := startNode(id, raftAddr, httpAddr, peerAddrs, httpAddrs, logger)
	if err != nil {
		return err
	}
	logger.Info("node started", "id", id, "raft", raftAddr, "http", httpAddr, "peers", peerKeys(peerAddrs))

	waitForSignal()
	logger.Info("shutting down", "id", id)
	ctxDone := make(chan struct{})
	go func() {
		_ = httpSrv.Close()
		gs.GracefulStop()
		node.Stop()
		close(ctxDone)
	}()
	select {
	case <-ctxDone:
	case <-time.After(3 * time.Second):
	}
	return nil
}

func startNode(id, raftAddr, httpAddr string, peerAddrs, httpAddrs map[string]string, logger *slog.Logger) (*raft.RaftNode, *kv.Server, *grpc.Server, *http.Server, error) {
	cfg := raft.DefaultConfig(id, peerAddrs)
	cfg.Logger = logger
	node := raft.NewNode(cfg)
	store := kv.NewStore()
	kvSrv := kv.NewServer(node, store)
	kvSrv.SetHTTPAddrs(httpAddrs)

	transport := raft.NewGRPCTransport(peerAddrs)
	node.SetTransport(transport)

	gs, _, err := raft.ListenAndServeGRPC(raftAddr, node, func(gs *grpc.Server) {
		kv.RegisterGRPC(gs, kvSrv)
	})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("grpc listen %s: %w", raftAddr, err)
	}

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           httpMux(kvSrv),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", "addr", httpAddr, "err", err)
		}
	}()

	node.Start()
	return node, kvSrv, gs, httpSrv, nil
}

func runDevCluster(logger *slog.Logger) error {
	ids := []string{"n1", "n2", "n3"}
	peerAddrs := map[string]string{
		"n1": "127.0.0.1:19101",
		"n2": "127.0.0.1:19102",
		"n3": "127.0.0.1:19103",
	}
	httpAddrs := map[string]string{
		"n1": "127.0.0.1:18101",
		"n2": "127.0.0.1:18102",
		"n3": "127.0.0.1:18103",
	}

	type running struct {
		node    *raft.RaftNode
		httpSrv *http.Server
		gs      *grpc.Server
	}
	var nodes []running
	for _, id := range ids {
		node, _, gs, httpSrv, err := startNode(id, peerAddrs[id], httpAddrs[id], peerAddrs, httpAddrs, logger)
		if err != nil {
			return err
		}
		nodes = append(nodes, running{node: node, httpSrv: httpSrv, gs: gs})
		logger.Info("dev node started", "id", id, "raft", peerAddrs[id], "http", httpAddrs[id])
	}

	logger.Info("dev cluster ready",
		"hint", "curl -s http://127.0.0.1:18101/status ; curl -s -X PUT http://127.0.0.1:18101/kv/color -d blue")

	waitForSignal()
	for _, n := range nodes {
		_ = n.httpSrv.Close()
		n.gs.GracefulStop()
		n.node.Stop()
	}
	return nil
}

func httpMux(srv *kv.Server) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, srv.Status())
	})
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		v, found, err := srv.Get(key)
		if err != nil {
			writeErr(w, err)
			return
		}
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]any{"key": key, "found": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": v, "found": true})
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := srv.Put(key, string(body)); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key})
	})
	return mux
}

func writeErr(w http.ResponseWriter, err error) {
	var nl *kv.NotLeaderError
	if errors.As(err, &nl) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":          false,
			"error":       "not_leader",
			"leader_id":   nl.LeaderID,
			"leader_addr": nl.LeaderAddr,
			"leader_http": nl.LeaderHTTP,
		})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parsePeers(s string) map[string]string {
	out := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(id)] = strings.TrimSpace(addr)
	}
	return out
}

func peerKeys(m map[string]string) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
