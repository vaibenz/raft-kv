package raftkv;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import raftkv.core.Message;
import raftkv.core.NotLeaderException;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.kv.KvStateMachine;
import raftkv.transport.Transport;

/** Scenario tests for the core protocol on a five node simulated cluster. */
public class RaftTest {
    private static String value(Cluster c, String node, String key) {
        byte[] v = c.machine(node).get(key);
        return v == null ? null : new String(v);
    }

    @Test
    public void refusesARepeatedPeerOrItselfAsAPeer() {
        for (List<String> bad : List.of(List.of("n2", "n2", "n2", "n2"), List.of("n1", "n2", "n3"))) {
            try {
                new RaftNode("n1", bad, null, null, RaftConfig.fast(), raftkv.core.Listener.NONE, null, null, null);
                throw new AssertionError("accepted peers " + bad);
            } catch (IllegalArgumentException expected) {
                Check.isTrue(expected.getMessage() != null, "message for " + bad);
            }
        }
    }

    @Test
    public void electsExactlyOneLeader() {
        try (Cluster c = new Cluster(5, 1)) {
            RaftNode leader = c.awaitLeader(2000);
            Check.sleep(300); // let followers settle and observe the leader
            long leaders = c.statuses().values().stream().filter(s -> s.role() == RaftNode.Role.LEADER).count();
            Check.equal(1L, leaders, "leaders");
            for (RaftNode.Status s : c.statuses().values())
                Check.equal(leader.id(), s.leaderId(), "every node agrees on the leader");
            c.assertSafe();
        }
    }

    @Test
    public void replicatesWritesToEveryNode() {
        try (Cluster c = new Cluster(5, 2)) {
            c.awaitLeader(2000);
            for (int i = 0; i < 100; i++) c.put("k" + i, "v" + i, 1, i + 1, 2000);
            c.awaitConvergence(3000);
            for (String id : c.ids) Check.equal("v99", value(c, id, "k99"), "replica " + id);
            c.assertSafe();
        }
    }

    @Test
    public void leaderCrashFailsOverWithoutLosingCommittedWrites() {
        try (Cluster c = new Cluster(5, 3)) {
            RaftNode first = c.awaitLeader(2000);
            for (int i = 0; i < 20; i++) c.put("before" + i, "x", 1, i + 1, 2000);
            c.crash(first.id());
            RaftNode second = c.awaitLeader(2000);
            Check.isTrue(!second.id().equals(first.id()), "a different node took over");
            Check.isTrue(second.status().term() > 0, "new term");
            for (int i = 0; i < 20; i++) c.put("after" + i, "y", 1, 100 + i, 2000);
            c.restart(first.id()); // the old leader rejoins as a follower and catches up
            c.awaitConvergence(3000);
            for (String id : c.ids) {
                Check.equal("x", value(c, id, "before19"), "pre-crash write on " + id);
                Check.equal("y", value(c, id, "after19"), "post-crash write on " + id);
            }
            c.assertSafe();
        }
    }

    @Test
    public void minorityPartitionCannotCommitAndRejoinsCleanly() throws Exception {
        try (Cluster c = new Cluster(5, 4)) {
            RaftNode old = c.awaitLeader(2000);
            c.put("stable", "1", 1, 1, 2000);
            String buddy = c.ids.stream().filter(id -> !id.equals(old.id())).findFirst().orElseThrow();
            Set<String> minority = Set.of(old.id(), buddy);
            Set<String> majority = Set.copyOf(c.ids.stream().filter(id -> !minority.contains(id)).toList());
            c.partition(minority, majority);

            // The old leader still believes it leads, but cannot reach a quorum.
            CompletableFuture<byte[]> doomed = old.propose(KvStateMachine.put(9, 1, "lost", "never".getBytes()));
            Check.sleep(500);
            Check.isTrue(!doomed.isDone() || doomed.isCompletedExceptionally(), "minority write must not commit");

            // The majority side elects a new leader and keeps serving writes.
            Check.eventually(() -> {
                RaftNode l = c.leader();
                return l != null && majority.contains(l.id());
            }, 3000, "majority elects a leader");
            c.put("progress", "2", 1, 2, 3000);

            c.net.heal();
            c.awaitConvergence(5000);
            Check.isTrue(doomed.isCompletedExceptionally(), "deposed leader failed the pending write");
            for (String id : c.ids) {
                Check.equal(null, value(c, id, "lost"), "uncommitted minority write discarded on " + id);
                Check.equal("2", value(c, id, "progress"), "majority write present on " + id);
            }
            c.assertSafe();
        }
    }

    @Test
    public void deposedLeaderCannotServeStaleReads() throws Exception {
        try (Cluster c = new Cluster(5, 5)) {
            RaftNode old = c.awaitLeader(2000);
            c.put("k", "old", 1, 1, 2000);
            c.net.partition(Set.of(old.id()), Set.copyOf(c.ids.stream().filter(i -> !i.equals(old.id())).toList()));
            c.put("k", "new", 1, 2, 3000); // committed by the new leader on the majority side

            // The isolated node still has "old" locally and may still think it is leader. ReadIndex must
            // refuse to confirm, so a client never sees the stale value.
            CompletableFuture<Void> read = old.readBarrier();
            try {
                read.get(500, TimeUnit.MILLISECONDS);
                throw new AssertionError("stale leader confirmed a read");
            } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException expected) {
                // ok: either still waiting for a quorum, or rejected because it knows it is not leader
            }
            RaftNode current = c.leader();
            current.readBarrier().get(1, TimeUnit.SECONDS);
            Check.equal("new", value(c, current.id(), "k"), "leader read sees the latest write");
            c.net.heal();
            c.assertSafe();
        }
    }

    @Test
    public void followerCatchesUpAfterLongOutage() {
        try (Cluster c = new Cluster(5, 6)) {
            RaftNode leader = c.awaitLeader(2000);
            String down = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(down);
            for (int i = 0; i < 1500; i++) c.put("k" + i, "v" + i, 1, i + 1, 2000); // spans several batches
            c.restart(down);
            c.awaitConvergence(5000);
            Check.equal("v1499", value(c, down, "k1499"), "lagging node caught up");
            c.assertSafe();
        }
    }

    @Test
    public void appendBatchesStayWithinTheByteBudget() {
        RaftConfig f = RaftConfig.fast();
        int budget = 32 * 1024;
        RaftConfig config = new RaftConfig(f.electionMinMs(), f.electionMaxMs(), f.heartbeatMs(), f.maxBatch(),
                100_000, budget);
        try (Cluster c = new Cluster(3, 12, config)) {
            RaftNode leader = c.awaitLeader(2000);
            String down = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(down);
            String big = "x".repeat(4096);
            for (int i = 0; i < 200; i++) c.put("k" + i, big + i, 1, i + 1, 2000); // 800 KiB behind
            c.restart(down);
            c.awaitConvergence(5000);
            Check.equal(big + 199, value(c, down, "k199"), "lagging node caught up");
            // one entry over the budget may push a batch past it, but never by a whole entry more
            Check.isTrue(c.net.largestFrame() < budget + 2 * 4096, "largest frame " + c.net.largestFrame());
            c.assertSafe();
        }
    }

    @Test
    public void writesFailFastWhenTheNodeStopsOnAStorageFailure() throws Exception {
        raftkv.storage.MemoryStorage real = new raftkv.storage.MemoryStorage();
        java.util.concurrent.atomic.AtomicBoolean broken = new java.util.concurrent.atomic.AtomicBoolean();
        raftkv.storage.Storage storage = (raftkv.storage.Storage) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {raftkv.storage.Storage.class}, (proxy, method, args) -> {
                    if (broken.get() && method.getName().equals("append"))
                        throw new java.io.UncheckedIOException(new java.io.IOException("disk gone"));
                    try {
                        return method.invoke(real, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        try (raftkv.transport.InMemoryNetwork net = new raftkv.transport.InMemoryNetwork(13)) {
            RaftNode node = new RaftNode("n1", List.of(), storage, new KvStateMachine(), RaftConfig.fast(),
                    raftkv.core.Listener.NONE, net.factory());
            node.start();
            try {
                long deadline = System.currentTimeMillis() + 2000;
                while (node.status().role() != RaftNode.Role.LEADER && System.currentTimeMillis() < deadline)
                    Check.sleep(5);
                node.propose(KvStateMachine.put(1, 1, "a", "1".getBytes())).get(2, TimeUnit.SECONDS);
                broken.set(true);
                // The first write hits the failure; the rest are queued behind it when the node stops itself.
                List<CompletableFuture<byte[]>> writes = new java.util.ArrayList<>();
                for (int i = 0; i < 50; i++)
                    writes.add(node.propose(KvStateMachine.put(1, 2 + i, "a", "x".getBytes())));
                for (CompletableFuture<byte[]> w : writes) {
                    try {
                        w.get(1, TimeUnit.SECONDS);
                        throw new AssertionError("a write succeeded on a node that cannot persist");
                    } catch (java.util.concurrent.ExecutionException e) {
                        Check.isTrue(e.getCause() instanceof NotLeaderException, "failed with " + e.getCause());
                    }
                }
                Check.isTrue(!node.isRunning(), "the node stopped itself");
            } finally {
                node.stop();
            }
        }
    }

    private static RaftConfig snapshotEvery(int entries) {
        RaftConfig f = RaftConfig.fast();
        return new RaftConfig(f.electionMinMs(), f.electionMaxMs(), f.heartbeatMs(), f.maxBatch(), entries);
    }

    @Test
    public void laggingFollowerCatchesUpFromASnapshot() {
        try (Cluster c = new Cluster(5, 10, snapshotEvery(50))) {
            RaftNode leader = c.awaitLeader(2000);
            String down = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(down);
            for (int i = 0; i < 400; i++) c.put("k" + (i % 40), "v" + i, 1, i + 1, 2000);
            Check.isTrue(c.leader().status().snapshotIndex() > 300, "leader compacted its log");
            c.restart(down); // the entries it needs are gone from the leader's log: it must get the snapshot
            c.awaitConvergence(5000);
            Check.isTrue(c.node(down).status().snapshotIndex() > 0, "follower installed a snapshot");
            Check.equal(c.machine(leader.id()).dump(), c.machine(down).dump(), "same state as the leader");
            Check.equal("v399", value(c, down, "k39"), "latest value");
            c.assertSafe();
        }
    }

    @Test
    public void restartRestoresFromSnapshotAndReplaysTheRest() {
        try (Cluster c = new Cluster(3, 11, snapshotEvery(25))) {
            c.awaitLeader(2000);
            c.put("dup", "first", 2, 1, 2000); // client 2's only write, soon buried in a snapshot
            for (int i = 0; i < 110; i++) c.put("k" + i, "v" + i, 1, i + 1, 2000);
            for (String id : c.ids) c.crash(id);
            for (String id : c.ids) c.restart(id);
            c.awaitLeader(3000);
            c.put("after", "restart", 1, 1000, 2000);
            c.awaitConvergence(3000);
            for (String id : c.ids) {
                Check.equal("v109", value(c, id, "k109"), "entry after the snapshot replayed on " + id);
                Check.equal("v3", value(c, id, "k3"), "entry inside the snapshot restored on " + id);
            }
            // Client 2 retries its write (same sequence number): its session lives only in the snapshot now, and
            // must still stop the retry from being applied a second time.
            c.put("dup", "second", 2, 1, 2000);
            c.awaitConvergence(3000);
            for (String id : c.ids) Check.equal("first", value(c, id, "dup"), "deduplicated across the snapshot");
            c.assertSafe();
        }
    }

    @Test
    public void wholeClusterRestartRecoversFromStorage() {
        try (Cluster c = new Cluster(5, 7)) {
            c.awaitLeader(2000);
            for (int i = 0; i < 50; i++) c.put("k" + i, "v" + i, 1, i + 1, 2000);
            for (String id : c.ids) c.crash(id);
            for (String id : c.ids) c.restart(id);
            c.awaitLeader(3000);
            c.put("after", "restart", 1, 1000, 2000); // a new term commit also commits the recovered log
            c.awaitConvergence(3000);
            for (String id : c.ids) Check.equal("v49", value(c, id, "k49"), "recovered on " + id);
            c.assertSafe();
        }
    }

    @Test
    public void followersRedirectClients() {
        try (Cluster c = new Cluster(3, 8)) {
            RaftNode leader = c.awaitLeader(2000);
            Check.sleep(100);
            RaftNode follower = c.node(c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow());
            try {
                follower.propose(KvStateMachine.put(0, 0, "k", "v".getBytes())).get(1, TimeUnit.SECONDS);
                throw new AssertionError("follower accepted a write");
            } catch (java.util.concurrent.ExecutionException e) {
                Check.isTrue(e.getCause() instanceof NotLeaderException, "NotLeaderException");
                Check.equal(leader.id(), ((NotLeaderException) e.getCause()).leaderHint(), "hint names the leader");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    @Test
    public void rejoiningNodeDoesNotDisruptAHealthyLeader() {
        try (Cluster c = new Cluster(5, 9)) {
            RaftNode leader = c.awaitLeader(2000);
            c.put("k", "v", 1, 1, 2000);
            long term = leader.status().term();
            String loner = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            Set<String> rest = Set.copyOf(c.ids.stream().filter(id -> !id.equals(loner)).toList());
            c.partition(Set.of(loner), rest);
            Check.sleep(1000); // the loner times out repeatedly, but PreVote stops it from raising its term
            Check.equal(term, c.node(loner).status().term(), "isolated node did not inflate its term");
            c.net.heal();
            c.put("k", "v2", 1, 2, 2000);
            Check.equal(leader.id(), c.leader().id(), "leader kept its job");
            Check.equal(term, c.leader().status().term(), "no extra election");
            c.awaitConvergence(3000);
            c.assertSafe();
        }
    }

    @Test
    public void messagesFromOutsideTheClusterAreIgnored() {
        try (Cluster c = new Cluster(3, 10)) {
            RaftNode leader = c.awaitLeader(2000);
            String left = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            for (String id : c.ids) if (!id.equals(left)) c.crash(id);
            RaftNode alone = c.node(left);
            // A stranger (another cluster, a misconfigured node) grants every vote it sees asked for. With
            // its votes counted, a lone node of three would reach "two of three" and lead a cluster of one.
            Transport stranger = c.net.factory().connect("x1", m -> {});
            long deadline = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < deadline) {
                long term = alone.status().term();
                stranger.send(left, new Message.VoteReply(term, "x1", true, true));
                stranger.send(left, new Message.VoteReply(term, "x1", true, false));
                Check.isTrue(alone.status().role() != RaftNode.Role.LEADER, "a stranger's votes elected a leader");
                Check.sleep(2);
            }
            // Nor can a stranger claiming a higher term take over as its leader.
            long term = alone.status().term();
            stranger.send(left, new Message.AppendEntries(term + 5, "x1", 0, 0, List.of(), 0, 1));
            Check.sleep(50);
            Check.isTrue(alone.status().term() < term + 5, "term raised by a stranger");
            Check.isTrue(!"x1".equals(alone.status().leaderId()), "a stranger became the leader");
            stranger.close();
        }
    }
}
