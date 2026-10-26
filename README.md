# raft-kv

A replicated key value store in Java 21 with no external dependencies: Raft consensus written from scratch, my own LSM tree storage engine underneath it, and a deterministic simulator that tests the whole thing against crashes, partitions and disk failures.

I wanted to understand Raft well enough to break it on purpose. Getting a cluster to survive crashes and partitions in a demo is one thing. Being able to say how sure you are that it never loses an acknowledged write is another. So most of this repo is about testing: every run of the cluster is a pure function of a seed, every client history is checked for linearizability, and every safety rule has a planted bug the tests must catch.

**Where it stands**

* 10,000 simulated runs pass: 74,216 crashes, 3.47 million client operations checked for linearizability, 28 hours of simulated cluster time in 161 seconds.
* All six planted bugs are caught, including the Figure 8 bug from the Raft paper.
* LSM engine: 113,000 random writes/s, 6.6x write amplification, 6.1 µs p50 point reads, about 0.8% bloom filter false positives.
* Cluster: 13,700 committed writes/s across five nodes, with every node fsyncing before a write is acknowledged.

## What's in it

**Consensus**
* Leader election with randomized timeouts, durable votes, the election restriction, and PreVote so a rejoining node cannot depose a healthy leader
* Log replication with pipelining and fast conflict backtracking, the current term commit rule, and a no-op on election
* Group commit: every write that arrives during an fsync shares the next one
* Linearizable reads through ReadIndex, without writing reads to the log
* Log compaction with snapshots, and InstallSnapshot for followers that fall behind the compacted log
* Exactly once writes through client sessions, kept with the data and inside snapshots

**Storage**
* An LSM tree: memtable, sorted string tables with CRC32C on every block, bloom filters, a block cache, and leveled compaction
* The Raft log doubles as the LSM's write ahead log: a flush commits the data and its log position in one atomic manifest update, and a restart replays the log from there
* Crash safe Raft storage: a checksummed log with torn write recovery, and atomic replacement (temp file, fsync, rename, directory fsync) for the term, vote, snapshot and manifest
* Every file operation goes through a small `Disk` interface, so the simulator can run the real storage code on a disk that fails the way real ones do

**Testing**
* Deterministic simulation of the whole cluster on one thread, with a simulated network, a simulated disk with POSIX crash semantics, clock drift, and FoundationDB style buggify
* A linearizability checker (Wing and Gong search with Lowe's memoisation, as in Knossos and Porcupine)
* Mutation testing: six planted protocol and storage bugs that the simulator must catch
* Model based crash tests for the Raft storage and the LSM, plus scenario tests and an end to end test over TCP and HTTP

## Architecture

```
            clients (HTTP)
                 │  GET / PUT / DELETE /kv/{key}         followers answer 307 → leader
                 ▼
   ┌────────────────────────────────┐   AppendEntries, RequestVote,     ┌──────────────┐
   │ KvServer (leader)              │   InstallSnapshot over TCP        │ follower n2  │
   │  ├─ RaftNode (one event loop)  │ ────────────────────────────────▶ │ follower n3  │
   │  ├─ LsmStateMachine            │                                   │ follower n4  │
   │  │    └─ LsmStore (memtable,   │                                   │ follower n5  │
   │  │       SSTables, manifest)   │                                   └──────────────┘
   │  └─ FileStorage (Raft log,     │
   │       term and vote, snapshot) │
   └────────────────────────────────┘
          both on a Disk: RealDisk in production, SimDisk in the simulator
```

| Package | Responsibility |
|---|---|
| `raftkv.core` | `RaftNode` (the protocol), messages, `EventLoop`, `Buggify` and `Mutation` hooks |
| `raftkv.lsm` | `LsmStore`, `SsTable`, `Bloom`, `Manifest`, merging iterator |
| `raftkv.storage` | `FileStorage` (the Raft log, term and vote, snapshot) and `MemoryStorage` |
| `raftkv.io` | `Disk`: the file operations storage needs, with the durability rules spelled out |
| `raftkv.kv` | `LsmStateMachine`, `KvStateMachine` (in memory), `KvServer` (HTTP API, entry point) |
| `raftkv.transport` | `TcpTransport`, binary `Codec`, `InMemoryNetwork` |
| `raftkv.sim` (tests) | the simulator, simulated disk and network, linearizability checker |

The design notes in [docs/DESIGN.md](docs/DESIGN.md) go through the invariants, the crash recovery ordering between the LSM and the Raft log, and how the simulator works.

### Life of a write

1. A client `PUT` reaches the leader, which appends the command to its log and schedules a flush.
2. The flush runs after every request already queued has been appended, so one fsync covers the whole batch.
3. The leader streams the new entries to followers without waiting for earlier batches (pipelining).
4. Each follower checks that its log matches at `prevLogIndex`, appends, fsyncs and replies.
5. Once a majority holds an entry from the current term, it is committed and applied: the LSM state machine checks the client's session, writes to the memtable and records the log position. When the memtable fills, it is flushed to a new table together with that position.

### Life of a read

The leader notes its commit index, sends one heartbeat round and waits for a majority to answer in its current term. That proves no newer leader exists, so once the state machine has applied up to the noted index, the local value is current. Reads that arrive together share one round. The value then comes from the memtable or, at most, one block per level, and the bloom filters rule out most tables without touching disk.

## Deterministic simulation

```sh
make sim SEEDS=1000      # 1000 random clusters
make sim SEED=4711       # replay one run exactly, event for event
make mutations           # check that every planted bug is caught
```

Each seed builds a different cluster: three or five nodes, the LSM or the in memory engine, random timeouts, batch sizes, snapshot frequency and fault rates. Then it runs clients and a nemesis together for 10 seconds of virtual time:

| Faults | |
|---|---|
| Network | drops, delays and reordering, duplicates, partitions into two or three groups, one way link failures |
| Disk | lost unsynced writes, torn writes, lost renames without a directory fsync, I/O errors, crashes in the middle of an I/O call |
| Process | crashes and restarts, crashing a leader moments after it wins, clocks drifting by up to 10% |
| Code (buggify) | append batches of one entry, late flushes, skipped heartbeats, half applied commits, slow ReadIndex rounds, a late no-op from a new leader |

Then it heals everything and checks Election Safety and State Machine Safety (continuously), liveness (every key readable within 10 s), convergence of all replicas, and linearizability of the full client history. A typical run of 10 virtual seconds takes about 16 ms of real time.

### Planted bugs

A test suite that never fails proves nothing, so each safety rule can be switched off and the simulator has to notice:

| Mutation | What goes wrong | Caught |
|---|---|---|
| `read-without-quorum` | a deposed leader serves a stale read | seed 5 |
| `ack-before-sync` | a follower acknowledges an entry it then loses in a crash | seed 1 |
| `no-election-restriction` | a leader without the committed entries overwrites them | seed 2 |
| `skip-dir-fsync` | a crash undoes a rename and a node forgets its vote | seed 1 |
| `skip-sst-fsync` | the LSM commits a table whose data never reached the disk | seed 2 |
| `commit-old-term` | the Figure 8 scenario from the Raft paper | seed 2275 |

The Figure 8 result was the most interesting one. With the leader's no-op still in place, the bug needs a new leader to commit an old entry and then crash before its own no-op reaches a majority. Random faults did not find it in 6,000 seeds. It took a nemesis that crashes freshly elected leaders, plus a buggify site that delays the no-op (legal, since the no-op is only an optimisation), and even then it shows up about once in 3,000 runs. More on that in the [design notes](docs/DESIGN.md).

## Benchmarks

```sh
make bench        # the cluster
make lsm-bench    # the storage engine on its own
```

All numbers are from a 2 vCPU cloud VM.

**Cluster:** five nodes in one JVM, real TCP between them, 100 byte values, LSM engine, and an fsync on every node before a write is acknowledged.

| Configuration | Committed writes/s | p50 latency | p99 latency |
|---|---|---|---|
| 64 clients, group commit | 13,700 | 3.3 ms | 23.6 ms |
| 64 clients, fsync per write (`-Draftkv.groupCommit=false`) | 1,790 | 34.5 ms | 70.2 ms |
| 1 client | 989 | 0.9 ms | 3.3 ms |

Group commit gives about 7.7x more throughput under load. All five nodes share two CPUs and one disk, so a real deployment would do better.

**LSM engine:** one million keys with 100 byte values, inserted in random order, default sizes (4 MB memtable, 2 MB tables).

| | |
|---|---|
| Random writes | 113,000 puts/s, including every flush and compaction |
| Write amplification | 6.6x (bytes written to disk per byte written by the user) |
| Space amplification | 1.10x (127 MB on disk for 116 MB of data) |
| Reads of present keys | p50 6.1 µs, p99 30.8 µs, 0.94 block reads per lookup |
| Reads of absent keys | p50 0.4 µs, 0.011 block reads per lookup |
| Bloom filter, 10 bits per key | 0.84% false positives (measured in `LsmStoreTest`) |

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

Each node keeps its Raft files in `data/<id>/raft` and its LSM tables in `data/<id>/sm`. Pass `--engine memory` for the in memory map instead. To make writes exactly once across retries, send `X-Client-Id` and an increasing `X-Request-Seq` header.

## Testing

```sh
make test       # unit, crash, scenario, chaos, end to end and simulation tests (about 30 s)
make sim        # the deterministic simulator over many seeds
make mutations  # every planted bug must be caught
make chaos      # the older real time chaos test, on real threads
```

## What I'd do next

* Cluster membership changes (single server changes, as in Ongaro's thesis)
* Stream snapshots in chunks instead of one message
* Move compaction to a background thread with write stalls, to remove the latency spikes it causes now
* Prefix compression and block compression in SSTables
* Expire client sessions

## References

* Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm*, USENIX ATC 2014, and Ongaro's PhD thesis (PreVote, section 9.6; ReadIndex, section 6.4)
* Patrick O'Neil and others, *The Log-Structured Merge-Tree*, 1996, and the LevelDB and RocksDB designs
* Will Wilson, *Testing Distributed Systems w/ Deterministic Simulation*, Strange Loop 2014 (FoundationDB)
* Jeannette Wing and Chun Gong, *Testing and Verifying Concurrent Objects*, 1993; Gavin Lowe, *Testing for Linearizability*, 2017
* Thanumalayan Pillai and others, *All File Systems Are Not Created Equal*, OSDI 2014 (on crash consistency)

---

If something here is unclear, broken, or just wrong, feel free to open an issue. I'm always happy to chat about it.
