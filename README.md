# raftkv

A small distributed key-value store. Three (or more) Go processes form a cluster, elect a leader with Raft, and only acknowledge a write once a majority of nodes have it in their log.

This phase covers leader election, log replication, and a Get/Put API. There is no disk persistence, snapshotting, or membership change yet — the log lives in memory.

## How it works

Each node is a Raft participant plus an in-memory map.

- **Followers** copy log entries from the leader and reject client reads/writes, pointing the client at the current leader.
- **Candidates** appear when a follower hears no heartbeat before its randomized election timeout. They increment the term and ask for votes.
- **Leaders** are the only nodes that accept `Get`/`Put`. A `Put` is appended to the leader's log and replicated with `AppendEntries`. It is committed when a majority of nodes have that entry, then applied to the map.

If the leader is killed or partitioned away, the remaining majority elects a new one. Entries that already reached a majority survive; entries that did not are not acknowledged.

Inter-node RPCs (`RequestVote`, `AppendEntries`) and the KV service travel over **gRPC**. A small HTTP API sits on top so you can poke the cluster with `curl`.

## Layout

```
raft/     RaftNode, election, log matching, gRPC transport
kv/       in-memory store and Get/Put (leader-only, with redirect)
proto/    gRPC definitions
cmd/      process entrypoint
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

## Tests

Election state transitions are unit-tested without sockets. Replication and failover use an in-memory network that can isolate a node:

```bash
go test ./...
go test ./... -race
```

## Not in this phase

Disk persistence, log compaction / snapshots, dynamic membership, and a dedicated chaos harness. The in-memory network in `raft/rpc.go` (`Isolate`, `Heal`, `Disconnect`) is the hook those tests will use later.
