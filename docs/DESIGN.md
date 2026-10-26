# Design notes

These are the decisions behind raft-kv and the invariants that hold it together. I wrote them down because each one is something I got wrong at least once, or something the simulator caught.

## 1. One event loop per node

All of a node's Raft state is owned by a single `EventLoop`. Messages, timers and client calls become tasks on that loop, so the protocol code is plain sequential Java with no locks, and two handlers can never interleave.

The loop is an interface, not a thread. In production it is `ThreadEventLoop`: one thread and the real clock. In the simulator it is `SimEventLoop`: a slot in a global event queue with a virtual clock. `RaftNode` never calls `System.nanoTime`, never creates a thread, and draws randomness only from the `Random` it is given. That single rule is what makes deterministic simulation possible without changing a line of protocol code between test and production.

## 2. Safety rules and where they live

| Property | Mechanism | Code |
|---|---|---|
| At most one leader per term | Votes are durable before they are sent; one vote per term | `startElection`, `onRequestVote` |
| A new leader has every committed entry | Election restriction: vote only for a log at least as up to date (5.4.1) | `onRequestVote` |
| Committed entries are never lost | Only count replicas for entries of the current term (5.4.2), plus a no-op on election | `advanceCommitIndex`, `becomeLeader` |
| Followers never acknowledge what they could lose | `sync()` before replying to AppendEntries; snapshot install is durable before its reply | `onAppendEntries`, `onInstallSnapshot` |
| Reads are linearizable | ReadIndex: record the commit index, confirm leadership with a majority round, wait until applied | `readBarrier`, `serviceReads` |
| A retried write is applied once | Client id and sequence number sessions, stored with the data and in snapshots | `KvStateMachine`, `LsmStateMachine` |

Four of these rules, plus two storage rules, have a matching mutation (`Mutation.java`) that switches the rule off, and the simulator has to catch each one. See section 6.

## 3. Storage and crash consistency

`Disk` is a deliberately small file system interface that makes the two POSIX durability rules explicit:

1. Data written to a file is durable only after `force()` (fsync).
2. Creating, renaming or deleting a file changes its directory, and that change is durable only after `syncDir()`, even if the file's own data was forced.

Everything that must change atomically (term and vote, the snapshot, the LSM manifest) goes through `writeAtomically`: write a temp file, force it, rename it over the old one, sync the directory. A crash leaves the old version or the new one, never a mix.

The Raft log is an append only file of `[length][term][command][crc32c]` records behind a header that records the log's first index and the term before it. On startup the records are scanned and a torn or corrupt tail is cut off.

Compaction writes the snapshot first and rewrites the log second. A crash between the two leaves a log that starts before the snapshot. On startup the covered prefix is dropped, and if the log disagrees with the snapshot about the term at the snapshot's index, it belongs to an older, conflicting history and is discarded. `StorageCrashTest` checks this by crashing at random I/O calls and comparing what survives with what was promised.

## 4. The LSM tree, and the Raft log as its write ahead log

An LSM needs a write ahead log so a crash does not lose the memtable. A Raft state machine already has one: the Raft log. So `LsmStore` has no WAL of its own. Instead:

* Every applied command tells the store which log position it reflects (`setApplied`).
* A flush writes the memtable as a new level 0 table and commits it, together with that position, in one atomic manifest update.
* After a crash the store reopens at its last flushed position (`durableIndex`), and Raft replays the log from there.

That makes the log the source of truth for anything not yet flushed, so Raft must never discard log entries the state machine still needs. The `StateMachine` interface encodes this:

* `checkpoint(index, term)` is called before Raft compacts its log. The in memory machine returns its whole image, which Raft stores. The LSM flushes and returns nothing, because its tables are the snapshot.
* `restore(image, index, term)` installs a snapshot from the leader, and the LSM makes it durable before returning.
* `durableIndex()` tells Raft how far the machine's own storage is durable.

The ordering rule is: **the state machine always moves first, then the log.** A crash between the two leaves the machine ahead of the log's snapshot, never behind it. On restart `RaftNode.recover()` notices this and finishes the interrupted step by installing a snapshot marker at the machine's durable position. If the order were reversed, a crash could leave a log that has discarded entries the machine never made durable, and that state would be gone for good.

Inside the LSM:

* Level 0 tables may overlap and are searched newest first. Each deeper level is one sorted run, so a lookup touches at most one table per level.
* Compaction merges level 0 into level 1 once it has a few tables. Level `i >= 1` may hold `levelBase * 10^(i-1)` bytes; when it is over, one table (chosen round robin through the key space) is merged into the overlapping tables below. Tombstones are dropped once a merge writes to the deepest level holding data.
* Tables are written and forced before the manifest that names them. Files the manifest does not name are garbage from an interrupted flush or compaction and are deleted on open.
* Every data block, the bloom filter and the block index carry a CRC32C. Corruption surfaces as an error on the read that touches it, never as wrong data.
* Client sessions live in the same store under their own key prefix, so they are flushed, compacted and snapshotted atomically with the data they protect.

## 5. Deterministic simulation

The simulator runs a whole cluster, its clients and a nemesis in one thread, on one virtual clock, from one seed:

* **Network:** drops, delays (and therefore reordering), duplicates, symmetric partitions into two or three groups, and one way link failures.
* **Disk:** each file has a durable image and a list of pending writes. A crash keeps a random prefix of the pending writes and may tear the last one. Directory changes not followed by `syncDir` are lost. Optionally, I/O calls fail with an error, or the process dies in the middle of one.
* **Clocks:** every node's timers drift by up to 10%.
* **Buggify:** code level fault injection after FoundationDB. Six sites in `RaftNode` switch on rare but legal behaviour: append batches of one entry, late flushes, skipped heartbeats, applying half of what was committed, slow ReadIndex rounds, and a new leader appending its no-op late.
* **Nemesis:** crashes (including whoever just became leader, moments after winning), restarts, partitions and network weather changes.

Every parameter (cluster size, timeouts, batch size, snapshot frequency, storage engine, fault rates) is drawn from the seed, so many seeds cover many configurations. After the faulty phase the run heals everything and checks:

1. Election Safety and State Machine Safety, checked continuously as events happen.
2. Liveness: every key becomes readable again within 10 virtual seconds of healing.
3. Convergence: every replica ends with the same log and the same data.
4. Linearizability of the whole client history (reads, writes and compare and set, including writes whose outcome is unknown), using the Wing and Gong search with Lowe's memoisation, checked per key.

A failing seed replays the identical run, event for event. `SimTest.sameSeedReplaysIdentically` checks that by comparing a fingerprint of every delivered message and every applied entry across two runs.

## 6. Mutation testing: proving the tests can fail

| Planted bug | What breaks | First caught at |
|---|---|---|
| `READ_WITHOUT_QUORUM` | a deposed leader serves a stale read | seed 5 |
| `ACK_BEFORE_SYNC` | a crashed follower forgets an entry it acknowledged | seed 1 |
| `NO_ELECTION_RESTRICTION` | a leader without committed entries overwrites them | seed 2 |
| `SKIP_DIR_FSYNC` | a crash undoes a rename, and a node forgets its vote | seed 1 |
| `SKIP_SST_FSYNC` | the LSM manifest names a table whose data never reached disk | seed 2 |
| `COMMIT_OLD_TERM` | the Figure 8 scenario: a "committed" entry is later overwritten | seed 2275 |

The last row was the most interesting result. With the old term commit rule removed but the leader's no-op still appended on election, the bug needs a new leader to commit an old entry *and* crash before its own no-op reaches a majority. The no-op closes almost all of that window, which is defence in depth working as intended.

Random faults did not find it in 6,000 seeds. Two additions did. First, a nemesis that crashes leaders moments after they win. Second, a buggify site that delays the no-op. That is legal behaviour, because the no-op is an optimisation for commit latency and ReadIndex, not a safety rule, and with the commit rule intact the simulator still passes every seed. With both in place the simulator finds Figure 8 at seed 2275, about once in 3,000 runs. That rarity is the real lesson: a bug this narrow would almost never show up in ordinary testing, and it would cost committed data when it did.

## 7. Known limits

* Cluster membership is fixed at startup.
* A snapshot is sent as a single message. A large state should be streamed in chunks.
* Flushes and compactions run inline on the apply thread, which shows up as latency spikes. A background compaction thread with write stalls would fix that.
* SSTable blocks have no prefix compression, and there is no compression at all.
* Client sessions are never expired.
