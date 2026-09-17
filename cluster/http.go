package cluster

import (
	"encoding/json"
	"net/http"
)

// Handler is the chaos control plane (port 18280 in -dev).
func Handler(c *Cluster) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"help": "chaos control plane for the -dev cluster",
			"try": []string{
				"curl -s http://127.0.0.1:18280/cluster",
				"curl -s -X POST http://127.0.0.1:18280/chaos/crash/n2",
				"curl -s -X POST http://127.0.0.1:18280/chaos/isolate/n2",
				"curl -s -X POST http://127.0.0.1:18280/chaos/heal/n2",
				"curl -s -X POST http://127.0.0.1:18280/chaos/restart/n2",
				"curl -s -X POST http://127.0.0.1:18280/chaos/partition/n1/n2",
				"curl -s -X POST http://127.0.0.1:18280/chaos/heal-all",
			},
		})
	})
	mux.HandleFunc("GET /cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":          true,
			"nodes":       c.Snapshot(),
			"leader_http": c.LeaderHTTP(),
		})
	})
	mux.HandleFunc("POST /chaos/isolate/{id}", func(w http.ResponseWriter, r *http.Request) {
		chaos(w, c, func() error { return c.Isolate(r.PathValue("id")) })
	})
	mux.HandleFunc("POST /chaos/heal/{id}", func(w http.ResponseWriter, r *http.Request) {
		chaos(w, c, func() error { return c.Heal(r.PathValue("id")) })
	})
	mux.HandleFunc("POST /chaos/crash/{id}", func(w http.ResponseWriter, r *http.Request) {
		chaos(w, c, func() error { return c.Crash(r.PathValue("id")) })
	})
	mux.HandleFunc("POST /chaos/restart/{id}", func(w http.ResponseWriter, r *http.Request) {
		chaos(w, c, func() error { return c.Restart(r.PathValue("id")) })
	})
	mux.HandleFunc("POST /chaos/partition/{a}/{b}", func(w http.ResponseWriter, r *http.Request) {
		chaos(w, c, func() error { return c.Partition(r.PathValue("a"), r.PathValue("b")) })
	})
	mux.HandleFunc("POST /chaos/heal-all", func(w http.ResponseWriter, r *http.Request) {
		c.HealAll()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "nodes": c.Snapshot()})
	})
	return mux
}

func chaos(w http.ResponseWriter, c *Cluster, fn func() error) {
	if err := fn(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "nodes": c.Snapshot()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
