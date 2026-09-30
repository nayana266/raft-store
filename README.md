# raftkv

A small distributed key-value store. Three (or more) Go processes form a cluster, elect a leader with Raft, and only acknowledge a write once a majority of nodes have it in their log.

This phase covers leader election, log replication, a Get/Put API, a chaos control plane, on-disk Raft persistence, **log compaction via snapshots**, and **dynamic membership** (add or remove one node at a time).

## Numbers (measured)

These are the figures to put on a resume or LinkedIn. They come from `go run ./cmd/bench` against a live 3-node `go run ./cmd -dev` cluster on localhost. Every Put waits for a **majority commit** and an **fsync** of `data/<id>/state.json` — not an in-memory map.

| What | Number | How to read it |
|---|---|---|
| Nodes | **3** (add a 4th) | Default cluster is n1–n3. `POST /cluster/add/n4` grows it to 4; remove brings it back to 3. |
| Throughput | **~650 committed Puts/sec** concurrent, **~130/sec** sequential | Peak: 16 HTTP clients, 1000 Puts → 661/sec (0 failures). One client, 500 Puts → 131/sec. |
| Fault scenarios | **6** | Isolate, heal, crash, restart-from-disk, pairwise partition, heal-all. Membership add/remove is separate. |

Sustained load is a bit lower because snapshots fire every 32 applied entries and `state.json` grows: 8 clients × 3000 Puts → **541/sec**. Use **~650** as the headline concurrent number, and say **majority + fsync** if someone asks what you measured.

```bash
go run ./cmd -dev          # other terminal
go run ./cmd/bench         # prints the table
```

**57** Go tests cover election, replication, failover, persistence, snapshots, and membership (`go test ./...`).

## How it works

Each node is a Raft participant plus an in-memory map.

- **Followers** copy log entries from the leader and reject client reads/writes, pointing the client at the current leader.
- **Candidates** appear when a follower hears no heartbeat before its randomized election timeout. They increment the term and ask for votes.
- **Leaders** are the only nodes that accept `Get`/`Put`. A `Put` is appended to the leader's log and replicated with `AppendEntries`. It is committed when a majority of nodes have that entry, then applied to the map.

If the leader is killed or partitioned away, the remaining majority elects a new one. Entries that already reached a majority survive; entries that did not are not acknowledged.

After enough entries have been applied (32 by default), a node **snapshots the map** and drops the prefix of the log that snapshot replaces. A follower that was offline long enough to miss those entries is caught up with `InstallSnapshot` instead of replaying the whole log.

Inter-node RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`) and the KV service travel over **gRPC**. A small HTTP API sits on top so you can poke the cluster with `curl`.

A new configuration takes effect as soon as the leader **appends** it to the log (one add or remove at a time). A process started with `-join` will not run for leader until an existing leader replicates membership to it.

## Layout

```
raft/     RaftNode, election, log matching, snapshots, gRPC, faults, file persistence
kv/       in-memory store and Get/Put (leader-only, with redirect)
cluster/  -dev cluster + chaos HTTP API (crash, isolate, partition)
proto/    gRPC definitions
cmd/      process entrypoint
data/     created at runtime: data/<id>/state.json (gitignored)
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
curl -s http://127.0.0.1:18101/status
```

Write and read on any node: a follower forwards Get/Put to the current leader. `/status` is always local (that process’s view), so `18101` can say `"state":"follower"` and still accept the Put.

```bash
curl -s -X PUT http://127.0.0.1:18101/kv/color -d blue
curl -s http://127.0.0.1:18101/kv/color
```

If forwarding is not in the binary you are running, a follower answers `503` with `leader_http`. Retry the same path on that address (for `-dev`, `n3` is `http://127.0.0.1:18103`).

Kill the leader process, wait a moment, and `/status` on a survivor should show a new `leader_id`. The key you already put is still there.

## Snapshots

`/status` reports `snapshot_index` (0 until the first compact) and `log_len` (entries still in memory, not counting the dummy). After ~32 applied commands the log prefix is replaced by a JSON snapshot of the map, written into `state.json` next to the remaining log.

Watch it shrink (run each line; do not paste comments into zsh):

```bash
i=1
while [ "$i" -le 40 ]; do
  curl -s -X PUT http://127.0.0.1:18101/kv/k$i -d v$i
  i=$((i + 1))
done
curl -s http://127.0.0.1:18101/status
```

You should see `snapshot_index` jump off 0 and `log_len` stay small. A crashed follower that missed those writes is sent `InstallSnapshot` when it comes back; `POST /chaos/crash/n3` then `POST /chaos/restart/n3` after the compact is the same idea.

## Break it on purpose (`-dev` only)

`go run ./cmd -dev` also starts a **chaos control plane** on port `18280`. This drops Raft RPCs or kills a node without you needing three terminals.

See the whole cluster:

```bash
curl -s http://127.0.0.1:18280/cluster
```

Write a key, then crash the leader and read it from whoever wins next:

```bash
curl -s http://127.0.0.1:18280/cluster
curl -s -X PUT http://127.0.0.1:18102/kv/color -d blue
curl -s -X POST http://127.0.0.1:18280/chaos/crash/n2
sleep 1
curl -s http://127.0.0.1:18101/status
curl -s http://127.0.0.1:18103/kv/color
```

Other knobs:

| Call | What it does |
|---|---|
| `POST /chaos/isolate/n2` | Unplug the network cable. Process stays up (zombie). |
| `POST /chaos/heal/n2` | Plug the cable back in. |
| `POST /chaos/crash/n2` | Kill Raft + HTTP + gRPC for that node. |
| `POST /chaos/restart/n2` | Boot it again from `data/n2/state.json`; it reloads its snapshot + log. |
| `POST /chaos/partition/n1/n2` | Cut only the n1↔n2 link. |
| `POST /chaos/heal-all` | Clear isolations and pairwise cuts. |
| `POST /cluster/add/n4` | Start n4 on 19104/18104 and append an add-member log entry. |
| `POST /cluster/remove/n4` | Remove n4 from the Raft config, then stop it. The leader cannot remove itself. |

Add a fourth node, write a key, then drop it again (`-dev` only). Use the curl tab, not the `go run` window:

```bash
curl -s -X POST http://127.0.0.1:18280/cluster/add/n4
curl -s http://127.0.0.1:18280/cluster
curl -s -X PUT http://127.0.0.1:18101/kv/after-add -d yes
curl -s http://127.0.0.1:18104/kv/after-add
curl -s -X POST http://127.0.0.1:18280/cluster/remove/n4
```

n4 is a joiner: it does not campaign until the leader has it in the config. After add, `/cluster` lists four peers. After remove, majority is three again.

Isolate vs crash: isolate keeps HTTP alive, so a partitioned leader may still accept a Put and then **time out** (no majority). Crash makes `curl` to that port fail with connection refused.

## Persistence

Each node writes `data/<id>/state.json` (term, who it voted for, the remaining log, and the latest snapshot) before it acknowledges a vote or a log append. After a reboot the node restores the map from the snapshot, then waits for a leader to commit a current-term entry so any suffix after the snapshot can be applied.

```bash
go run ./cmd -dev
```

Ctrl+C, then start again — keys are still there.

Single-node process: `-data data/n1` (default `data/<id>`). Wipe the cluster with `rm -rf data`.

## Tests

Election state transitions are unit-tested without sockets. Replication, failover, persistence, and snapshot catch-up use an in-memory network that can isolate a node:

```bash
go test ./...
go test ./... -race
```

## Not in this phase

Joint consensus for changing several nodes at once, and automatic leader handoff so the current leader can remove itself. One-at-a-time add/remove, snapshots, persistence, and chaos are in.
