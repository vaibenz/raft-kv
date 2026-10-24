package raftkv;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import raftkv.core.NotLeaderException;
import raftkv.core.RaftNode;
import raftkv.kv.KvStateMachine;

/** Scenario tests for the core protocol on a five node simulated cluster. */
public class RaftTest {
    private static String value(Cluster c, String node, String key) {
        byte[] v = c.machine(node).get(key);
        return v == null ? null : new String(v);
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
}
