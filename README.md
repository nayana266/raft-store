# raftkv

A small distributed key-value store. Three (or more) Go processes form a cluster, elect a leader with Raft, and only acknowledge a write once a majority of nodes have it in their log.

This phase covers leader election, log replication, a Get/Put API, a chaos control plane, and **on-disk Raft persistence** (term, vote, and log). The key-value map is rebuilt by replaying the log after a restart. There is no snapshotting or membership change yet.

## How it works

Each node is a Raft participant plus an in-memory map.

- **Followers** copy log entries from the leader and reject client reads/writes, pointing the client at the current leader.
- **Candidates** appear when a follower hears no heartbeat before its randomized election timeout. They increment the term and ask for votes.
- **Leaders** are the only nodes that accept `Get`/`Put`. A `Put` is appended to the leader's log and replicated with `AppendEntries`. It is committed when a majority of nodes have that entry, then applied to the map.

If the leader is killed or partitioned away, the remaining majority elects a new one. Entries that already reached a majority survive; entries that did not are not acknowledged.

Inter-node RPCs (`RequestVote`, `AppendEntries`) and the KV service travel over **gRPC**. A small HTTP API sits on top so you can poke the cluster with `curl`.

## Layout

```
raft/     RaftNode, election, log matching, gRPC, faults, file persistence
kv/       in-memory store and Get/Put (leader-only, with redirect)
cluster/  -dev cluster + chaos HTTP API (crash, isolate, partition)
proto/    gRPC definitions
cmd/      process entrypoint
data/     created at runtime: data/n1/state.json, … (gitignored)
```

## Run a cluster

Three terminals is the intended shape:

```bash
go run ./cmd -id n1 -raft-addr 127.0.0.1:19101 -http-addr 127.0.0.1:18101 \
  -peers n1=127.0.0.1:19101,n2=127.0.0.1:19102,n3=127.0.0.1:19103 \
  -http-peers n1=127.0.0.1:18101,n2=127.0.0.1:18102,n3=127.0.0.1:18103

go run ./cmd -id n2 -raft-addr 127.0.0.1:19102 -http-addr 127.0.0.1:18102 \
  -peers n1=127.0.0.1:19101,n2=127.0.0.1:19102,n3=127.0.0.1:19103 \
  -http-peers n1=127.0.0.1:18101,n2=127.0.0.1:18102,n3=127.0.0.1:18103

go run ./cmd -id n3 -raft-addr 127.0.0.1:19103 -http-addr 127.0.0.1:18103 \
  -peers n1=127.0.0.1:19101,n2=127.0.0.1:19102,n3=127.0.0.1:19103 \
  -http-peers n1=127.0.0.1:18101,n2=127.0.0.1:18102,n3=127.0.0.1:18103
```

Or one process that starts all three (handy while developing):

```bash
go run ./cmd -dev
```

## Talk to it

Find the leader (any node answers `/status`):

```bash
curl -s http://127.0.0.1:18101/status | jq
```

Write and read on the leader. If you hit a follower you get `503` with `leader_id` and `leader_http`.

```bash
curl -s -X PUT http://127.0.0.1:18101/kv/color -d blue
curl -s http://127.0.0.1:18101/kv/color
```

Kill the leader process, wait a moment, and `/status` on a survivor should show a new `leader_id`. The key you already put is still there.

## Break it on purpose (`-dev` only)

`go run ./cmd -dev` also starts a **chaos control plane** on port `18280`. This drops Raft RPCs or kills a node without you needing three terminals.

See the whole cluster:

```bash
curl -s http://127.0.0.1:18280/cluster
```

Write a key, then crash the leader and read it from whoever wins next:

```bash
# 1. find the leader
curl -s http://127.0.0.1:18280/cluster

# 2. put on that leader's HTTP port (example: n2 → 18102)
curl -s -X PUT http://127.0.0.1:18102/kv/color -d blue

# 3. kill the leader process
curl -s -X POST http://127.0.0.1:18280/chaos/crash/n2

# 4. wait a beat, then ask the survivors
sleep 1
curl -s http://127.0.0.1:18101/status
curl -s http://127.0.0.1:18103/kv/color   # follow leader_http if this 503s
```

Other knobs:

| Call | What it does |
|---|---|
| `POST /chaos/isolate/n2` | Unplug the network cable. Process stays up (zombie). |
| `POST /chaos/heal/n2` | Plug the cable back in. |
| `POST /chaos/crash/n2` | Kill Raft + HTTP + gRPC for that node. |
| `POST /chaos/restart/n2` | Boot it again from `data/n2/state.json`; it reloads its log. |
| `POST /chaos/partition/n1/n2` | Cut only the n1↔n2 link. |
| `POST /chaos/heal-all` | Clear isolations and pairwise cuts. |

Isolate vs crash: isolate keeps HTTP alive, so a partitioned leader may still accept a Put and then **time out** (no majority). Crash makes `curl` to that port fail with connection refused.

## Persistence

Each node writes `data/<id>/state.json` (term, who it voted for, and the log) before it acknowledges a vote or a log append. The map is **not** saved separately: after a reboot the node replays committed log entries into memory.

```bash
go run ./cmd -dev          # uses ./data/n1, ./data/n2, ./data/n3
# Ctrl+C, then start again — color=blue is still there
```

Single-node process: `-data data/n1` (default `data/<id>`). Wipe the cluster with `rm -rf data`.

## Tests

Election state transitions are unit-tested without sockets. Replication and failover use an in-memory network that can isolate a node:

```bash
go test ./...
go test ./... -race
```

## Not in this phase

Log compaction / snapshots, and dynamic membership. Persistence and chaos are in.
