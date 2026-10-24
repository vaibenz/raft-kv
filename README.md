# raft-kv

A fault tolerant, replicated key value store built on a from scratch implementation of the **Raft consensus algorithm** in Java 21, with no external dependencies.

Any minority of nodes can crash, restart, or be cut off by a network partition, and the store keeps serving **linearizable** reads and writes without losing a single acknowledged write. A randomized fault injection test checks this continuously.

## What is implemented

* **Leader election** with randomized timeouts, durable votes, and the election restriction (a node only votes for candidates whose log is at least as up to date)
* **PreVote**, so a node that was partitioned away or just restarted cannot force a healthy leader to step down
* **Log replication** with pipelining and fast conflict backtracking (skips back a whole term per round trip)
* **Commit rule** for current term entries, with a no-op appended on election
* **Group commit**: all writes that arrive while an fsync is in progress share the next one
* **Linearizable reads via ReadIndex**, without writing reads to the log, and without ever serving a stale value from a deposed leader
* **Exactly once writes**: client id plus sequence number deduplication, so a retried write that already committed is not applied twice
* **Crash safe storage**: CRC32C checksummed append only log with torn write recovery; term and vote replaced atomically
* **TCP transport** with length prefixed frames, batched writes and reconnects, all on virtual threads
* **HTTP API** with automatic redirects from followers to the leader

## Architecture

```
          clients (HTTP)
               │  PUT / GET / DELETE /kv/{key}       followers answer 307 → leader
               ▼
  ┌───────────────────────────┐   AppendEntries / RequestVote (TCP)   ┌──────────────┐
  │ KvServer (leader)         │ ────────────────────────────────────▶ │ follower n2  │
  │  ├─ RaftNode  (event loop)│ ────────────────────────────────────▶ │ follower n3  │
  │  ├─ KvStateMachine        │ ────────────────────────────────────▶ │ follower n4  │
  │  └─ FileStorage (fsync)   │ ────────────────────────────────────▶ │ follower n5  │
  └───────────────────────────┘                                       └──────────────┘
```

| Package | Responsibility |
|---|---|
| `raftkv.core` | `RaftNode`: the protocol, messages, configuration |
| `raftkv.storage` | `FileStorage` (durable) and `MemoryStorage` (tests, models lost unsynced writes) |
| `raftkv.transport` | `TcpTransport`, binary `Codec`, and `InMemoryNetwork` for simulated faults |
| `raftkv.kv` | `KvStateMachine` and `KvServer` (HTTP API, entry point) |

### Threading model

All Raft state in a node is owned by one event loop thread. Incoming messages, timer expiries and client requests are all queued as tasks on that thread. The protocol is therefore written as plain sequential code with no locks, and two handlers can never interleave. Network I/O and HTTP handling run on separate virtual threads and only enqueue work.

### Life of a write

1. A client `PUT` reaches the leader, which appends the command to its log and schedules a flush.
2. The flush runs after every request already queued has been appended, so one fsync covers the whole batch (group commit).
3. The leader streams the new entries to followers without waiting for earlier batches to be acknowledged (pipelining).
4. Each follower checks that its log matches at `prevLogIndex`, appends, fsyncs, and replies.
5. Once a majority holds an entry from the current term, it is committed, applied to the key value map, and the client's request completes.

### Life of a read

Writing reads into the log would be correct but slow. With ReadIndex, the leader notes its commit index, sends one heartbeat round, and waits for a majority to answer in its current term. That proves no newer leader exists, so once the state machine has applied up to the noted index, the local value is the latest. Reads that arrive together share a single round.

## Testing

```sh
make test                          # 19 tests, about 20 seconds
make chaos CHAOS_SECONDS=120       # longer randomized fault injection run
make chaos SEED=1743191622389      # replay the exact fault schedule of a past run
```

`Cluster` runs five nodes in one process on a simulated network that drops 5% of messages and delays (and so reorders) the rest. While it runs, two of Raft's safety properties are checked on every event:

* **Election Safety**: at most one leader is ever elected per term
* **State Machine Safety**: no two nodes ever apply different commands at the same index

`ChaosTest` adds eight concurrent clients and a nemesis that, every few hundred milliseconds, crashes a node (discarding anything not fsynced), restarts one, isolates the leader, or splits the cluster at random. At the end it heals everything and verifies that **every acknowledged write is present on every replica** and that all replicas are identical. A typical 45 second run injects about 140 faults, forces 20 to 30 leader elections, and acknowledges about 20,000 writes with zero violations.

The tests were also checked against deliberately broken versions of the protocol: removing the election restriction and the current term commit rule makes the chaos test fail.

Scenario tests cover leader failover, minority partitions (the cut off leader cannot commit, and its uncommitted write is discarded on rejoin), stale reads from a deposed leader, a follower catching up after missing 1,500 entries, a whole cluster restart from disk, PreVote, torn writes and checksum corruption in the log file, and an end to end run over real TCP and HTTP.

## Benchmarks

```sh
make bench
```

Five nodes in one JVM on a 2 vCPU cloud VM, real TCP between nodes, 100 byte values, and an fsync on every node before a write is acknowledged:

| Configuration | Committed writes/s | p50 latency | p99 latency |
|---|---|---|---|
| 64 clients, group commit | 16,600 | 2.9 ms | 21.4 ms |
| 64 clients, fsync per write (`-Draftkv.groupCommit=false`) | 2,180 | 28.8 ms | 47.0 ms |
| 1 client | 984 | 0.9 ms | 3.3 ms |

Group commit gives a **7.6x** throughput increase and a 10x lower median latency under load. Because all five nodes share two CPUs and one disk, a real deployment would do better.

## Run a five node cluster

Needs JDK 21 or newer.

```sh
scripts/cluster.sh start                                   # n1..n5, HTTP on ports 8001 to 8005
scripts/cluster.sh status
curl -L -X PUT --data 'hello' http://localhost:8001/kv/greeting
curl -L http://localhost:8003/kv/greeting                  # any node works; followers redirect
scripts/cluster.sh kill n2                                 # kill -9 a node, even the leader
curl -L http://localhost:8004/kv/greeting                  # still served
scripts/cluster.sh restart n2                              # rejoins and catches up
scripts/cluster.sh stop
```

To make writes exactly once across retries, send `X-Client-Id` and an increasing `X-Request-Seq` header.

## Limitations and next steps

* No log compaction or snapshots, so the log and the in memory copy grow without bound
* Cluster membership is fixed at startup (no joint consensus)
* Client sessions for deduplication are never expired
* The whole log is cached in memory; a production version would page older entries from disk

## Reference

Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm*, USENIX ATC 2014, and Ongaro's PhD thesis (PreVote, section 9.6; ReadIndex, section 6.4).
