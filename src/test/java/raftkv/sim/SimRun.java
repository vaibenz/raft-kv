package raftkv.sim;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import raftkv.core.Buggify;
import raftkv.core.Listener;
import raftkv.core.NotLeaderException;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.kv.KvStateMachine;
import raftkv.storage.FileStorage;

/**
 * One deterministic simulation run: a cluster, a set of clients and a nemesis, all driven by one seed.
 *
 * <p>Every parameter of the run (cluster size, network weather, timeouts, client mix, which buggify sites are
 * on) is drawn from the seed, so a large number of seeds covers a large space of configurations. Each run:
 * <ol>
 *   <li>runs clients and the nemesis together for {@code duration} of virtual time,</li>
 *   <li>stops the nemesis, heals the network and restarts every node,</li>
 *   <li>requires every key to become readable again (liveness) and every replica to converge,</li>
 *   <li>checks Election Safety and State Machine Safety (continuously, as events happen), and finally</li>
 *   <li>checks the full client history for linearizability.</li>
 * </ol>
 * A failing seed reproduces the identical run, event for event, every time it is replayed.
 */
public final class SimRun {
    public record Result(long seed, String failure, Stats stats, long fingerprint) {
        public boolean ok() {
            return failure == null;
        }
    }

    /** What happened during a run, for the summary line and for aggregation across seeds. */
    public static final class Stats {
        public int nodes, clients, keys;
        public long faults, crashes, partitions, leaders, opsOk, opsUnknown, opsAbandonedReads;
        public long messagesSent, messagesDelivered, virtualMs, steps;
        public long snapshotsTaken, snapshotsInstalled, tornWrites, ioErrors, crashPoints;

        @Override
        public String toString() {
            return String.format("%d nodes, %d clients, %d keys | %d faults (%d crashes, %d mid I/O, %d partitions, "
                            + "%d torn writes, %d I/O errors), %d leaders, %d snapshots taken, %d installed | %d ops "
                            + "checked, %d with unknown outcome | %d msgs | %.1f s virtual, %d events",
                    nodes, clients, keys, faults, crashes, crashPoints, partitions, tornWrites, ioErrors, leaders,
                    snapshotsTaken,
                    snapshotsInstalled, opsOk + opsUnknown, opsUnknown, messagesSent, virtualMs / 1000.0, steps);
        }
    }

    private static final long CLIENT_TIMEOUT_MS = 200, OP_DEADLINE_MS = 2_500;

    private final long seed;
    private final long durationMs;
    private final Simulation sim;
    private final SimNetwork net;
    private final RaftConfig config;
    private final List<String> ids = new ArrayList<>();
    private final Map<String, SimNode> nodes = new HashMap<>();
    private final List<Op> history = new ArrayList<>();
    private final List<String> violations = new ArrayList<>();
    private final Map<Long, String> leaderOfTerm = new HashMap<>();
    private final Map<Long, String> appliedAt = new HashMap<>();
    private final Stats stats = new Stats();
    private final List<String> keys = new ArrayList<>();
    private final double assassinate; // chance that a newly elected leader is crashed moments later
    private int nextOpId;
    private boolean issuing = true;
    private int outstanding;

    /**
     * A machine: a simulated disk that outlives crashes, and a process (storage, state machine, Raft node) that
     * does not. Every start opens the real {@link FileStorage} on whatever the disk kept.
     */
    private final class SimNode {
        final String id;
        final SimDisk disk;
        final double drift;
        RaftNode raft;
        KvStateMachine machine;
        boolean up;

        SimNode(String id, double drift) {
            this.id = id;
            this.drift = drift;
            this.disk = new SimDisk(sim.random);
        }

        void start() {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            FileStorage storage;
            try {
                storage = new FileStorage(disk.root());
            } catch (java.io.UncheckedIOException | SimDisk.Crash e) { // died while opening: stay down
                disk.crash();
                return;
            } catch (SimDisk.InjectedIOException e) { // an injected error while opening: stay down
                disk.crash();
                return;
            } catch (java.io.IOException e) { // the files on disk are unreadable: that is a real bug
                violations.add("storage on " + id + " failed to open after a crash: " + e);
                return;
            }
            machine = new KvStateMachine(); // rebuilt from the snapshot and log, as after a real restart
            raft = new RaftNode(id, peers, storage, machine, config, invariants, net.factory(),
                    new SimEventLoop(sim, drift, this::crash), new Random(sim.random.nextLong()));
            up = true;
            raft.start();
        }

        /** True while the process is running; a node stops itself if its disk reports an error. */
        boolean alive() {
            return up && raft.isRunning();
        }

        void crash() {
            if (!up) return;
            up = false;
            raft.stop();
            disk.crash(); // anything not durable is gone, and the tail of what was in flight may be torn
        }

        /** A node that stopped itself after an I/O error is a dead process: treat it as crashed. */
        void reapIfStopped() {
            if (up && !raft.isRunning()) crash();
        }
    }

    private final Listener invariants = new Listener() {
        @Override
        public void onLeader(String node, long term) {
            stats.leaders++;
            // Many of Raft's subtle cases (Figure 8 among them) need a leader to die just after winning, before
            // its first entry of the new term reaches a majority. Random crashes rarely hit that window.
            if (issuing && sim.random.nextDouble() < assassinate) {
                SimNode victim = nodes.get(node);
                RaftNode raft = victim.raft;
                sim.after(sim.random.nextInt(config.heartbeatMs() * 3), () -> {
                    if (issuing && victim.raft == raft && victim.alive()) {
                        victim.crash();
                        stats.crashes++;
                        sim.after(config.electionMinMs() + sim.random.nextInt(config.electionMaxMs()), () -> {
                            if (!victim.up) victim.start();
                        });
                    }
                });
            }
            String prev = leaderOfTerm.putIfAbsent(term, node);
            if (prev != null && !prev.equals(node))
                violations.add("Election Safety: two leaders in term " + term + ": " + prev + " and " + node);
        }

        @Override
        public void onApply(String node, long index, long term, byte[] command) {
            String fingerprint = term + ":" + Arrays.hashCode(command);
            sim.trace(index * 31 + fingerprint.hashCode());
            String prev = appliedAt.putIfAbsent(index, fingerprint);
            if (prev != null && !prev.equals(fingerprint))
                violations.add("State Machine Safety: index " + index + " applied as " + prev + " and as "
                        + fingerprint + " (on " + node + ")");
        }

        @Override
        public void onSnapshot(String node, long index, long term, boolean installed) {
            if (installed) stats.snapshotsInstalled++;
            else stats.snapshotsTaken++;
        }

        @Override
        public void onError(String node, Throwable error) {
            violations.add("unexpected exception on " + node + ": " + error);
        }
    };

    public SimRun(long seed, long durationMs) {
        this.seed = seed;
        this.durationMs = durationMs;
        this.sim = new Simulation(seed);
        this.net = new SimNetwork(sim);
        Random r = sim.random;

        int heartbeat = 10 + r.nextInt(21);
        int electionMin = heartbeat * (3 + r.nextInt(4));
        int snapshotEvery = r.nextDouble() < 0.25 ? 1_000_000 : 3 + r.nextInt(200);
        config = new RaftConfig(electionMin, electionMin * 2, heartbeat, 1 + r.nextInt(128), snapshotEvery);

        int size = r.nextBoolean() ? 5 : 3;
        for (int i = 1; i <= size; i++) {
            String id = "n" + i;
            ids.add(id);
            nodes.put(id, new SimNode(id, 0.9 + 0.2 * r.nextDouble()));
        }
        net.dropRate = r.nextDouble() < 0.3 ? 0 : 0.15 * r.nextDouble();
        net.duplicateRate = r.nextDouble() < 0.5 ? 0 : 0.05 * r.nextDouble();
        net.minDelayMs = 1 + r.nextInt(2); // a real network never delivers in zero time
        net.maxDelayMs = net.minDelayMs + 1 + r.nextInt(heartbeat * 2);
        double ioErrors = r.nextDouble() < 0.3 ? 0.002 * r.nextDouble() : 0;
        double crashPoints = r.nextDouble() < 0.5 ? 0.003 * r.nextDouble() : 0;
        for (SimNode n : nodes.values()) {
            n.disk.ioErrorRate = ioErrors;
            n.disk.crashPointRate = crashPoints;
        }

        int keyCount = 1 + r.nextInt(4);
        for (int i = 0; i < keyCount; i++) keys.add("k" + i);
        stats.nodes = size;
        stats.keys = keyCount;
        stats.clients = 2 + r.nextInt(6);
        assassinate = r.nextDouble() < 0.5 ? 0 : 0.5 * r.nextDouble();
        Buggify.enable(new Random(seed * 0x9E3779B97F4A7C15L));
    }

    public Result run() {
        sim.setBudget(1_500_000); // a healthy 10 s run takes well under 100,000 events
        try {
            return execute();
        } catch (Simulation.BudgetExceeded e) {
            return finish("message storm or livelock: " + e.getMessage());
        } finally {
            Buggify.disable();
        }
    }

    private Result execute() {
        for (String id : ids) nodes.get(id).start();
        for (int c = 1; c <= stats.clients; c++) {
            Client client = new Client(c);
            sim.after(sim.random.nextInt(50), client::next);
        }
        scheduleNemesis();

        sim.runUntil(durationMs);
        issuing = false;
        healEverything();

        if (!sim.runUntil(() -> outstanding == 0, sim.now() + OP_DEADLINE_MS + 5_000))
            return finish("liveness: " + outstanding + " client operations still running after the cluster healed");
        String failure = finalReads();
        if (failure == null) failure = awaitConvergence();
        if (failure == null && !violations.isEmpty()) failure = violations.get(0);
        if (failure == null) {
            Linearizability.Violation v = Linearizability.check(history);
            if (v != null) failure = v.toString();
        }
        return finish(failure);
    }

    private Result finish(String failure) {
        if (failure == null && !violations.isEmpty()) failure = violations.get(0);
        if (failure != null && !violations.isEmpty() && !failure.equals(violations.get(0)))
            failure = violations.get(0) + " (and then: " + failure + ")"; // the first broken invariant is the cause
        for (SimNode n : nodes.values()) {
            n.crash();
            stats.tornWrites += n.disk.tornWrites;
            stats.ioErrors += n.disk.ioErrors;
            stats.crashPoints += n.disk.crashPoints;
        }
        stats.messagesSent = net.sent;
        stats.messagesDelivered = net.delivered;
        stats.virtualMs = sim.now();
        stats.steps = sim.steps();
        for (Op op : history) if (op.complete()) stats.opsOk++; else stats.opsUnknown++;
        return new Result(seed, failure, stats, sim.fingerprint());
    }

    // ------------------------------------------------------------------ nemesis

    private void scheduleNemesis() {
        sim.after(50 + sim.random.nextInt(350), () -> {
            if (!issuing) return;
            injectFault();
            scheduleNemesis();
        });
    }

    private List<String> upNodes() {
        return ids.stream().filter(id -> nodes.get(id).alive()).toList();
    }

    private String leader() {
        String best = null;
        long bestTerm = -1;
        for (String id : ids) {
            SimNode n = nodes.get(id);
            if (!n.alive()) continue;
            RaftNode.Status s = n.raft.status();
            if (s.role() == RaftNode.Role.LEADER && s.term() > bestTerm) {
                best = id;
                bestTerm = s.term();
            }
        }
        return best;
    }

    private void injectFault() {
        Random r = sim.random;
        stats.faults++;
        for (SimNode n : nodes.values()) n.reapIfStopped();
        List<String> up = upNodes();
        List<String> down = ids.stream().filter(id -> !nodes.get(id).alive()).toList();
        int minority = (ids.size() - 1) / 2;
        switch (r.nextInt(10)) {
            case 0, 1 -> { // crash a node; occasionally allow a majority to be down at once
                if (!up.isEmpty() && (down.size() < minority || r.nextDouble() < 0.1)) {
                    nodes.get(up.get(r.nextInt(up.size()))).crash();
                    stats.crashes++;
                }
            }
            case 2 -> { // crash whoever is leader right now
                String l = leader();
                if (l != null && down.size() < minority + 1) {
                    nodes.get(l).crash();
                    stats.crashes++;
                }
            }
            case 3, 4 -> {
                if (!down.isEmpty()) nodes.get(down.get(r.nextInt(down.size()))).start();
            }
            case 5 -> { // random split into two or three groups
                int groups = 2 + r.nextInt(2);
                List<Set<String>> parts = new ArrayList<>();
                for (int g = 0; g < groups; g++) parts.add(new HashSet<>());
                for (String id : ids) parts.get(r.nextInt(groups)).add(id);
                net.partition(parts);
                stats.partitions++;
            }
            case 6 -> { // isolate the leader
                String l = leader();
                if (l != null) {
                    Set<String> rest = new HashSet<>(ids);
                    rest.remove(l);
                    net.partition(List.of(Set.of(l), rest));
                    stats.partitions++;
                }
            }
            case 7 -> { // one way failures: a few directed links stop working
                for (int i = 0, n = 1 + r.nextInt(3); i < n; i++) {
                    String a = ids.get(r.nextInt(ids.size())), b = ids.get(r.nextInt(ids.size()));
                    if (!a.equals(b)) net.cut(a, b);
                }
                stats.partitions++;
            }
            case 8 -> { // the weather changes
                net.dropRate = r.nextDouble() < 0.5 ? 0 : 0.2 * r.nextDouble();
                net.maxDelayMs = net.minDelayMs + 1 + r.nextInt(config.heartbeatMs() * 3);
            }
            default -> net.heal();
        }
    }

    private void healEverything() {
        net.heal();
        net.dropRate = 0;
        net.duplicateRate = 0;
        for (String id : ids) {
            SimNode n = nodes.get(id);
            n.disk.ioErrorRate = 0;
            n.disk.crashPointRate = 0;
            n.reapIfStopped();
            if (!n.up) n.start();
        }
    }

    // ------------------------------------------------------------------ clients

    private long clientDelay() {
        return 1 + sim.random.nextInt(5);
    }

    private static String str(byte[] v) {
        return v == null ? null : new String(v, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A client that issues one operation at a time. Writes are retried with the same sequence number until they
     * are acknowledged or a deadline passes, so the state machine applies each at most once. A write that runs
     * out of time is recorded with an unknown outcome; a read that runs out of time has no effect and is dropped.
     */
    private final class Client {
        final int id;
        long seq;
        int written;
        String guess; // node believed to be leader
        final Map<String, String> lastSeen = new HashMap<>();

        Client(int id) {
            this.id = id;
        }

        void next() {
            if (!issuing) return;
            Random r = sim.random;
            String key = keys.get(r.nextInt(keys.size()));
            double p = r.nextDouble();
            outstanding++;
            if (p < 0.4) {
                read(new Op(nextOpId++, id, key, Op.Kind.READ, null, null, sim.now()));
            } else if (p < 0.75) {
                String v = "c" + id + "." + (++written);
                Op op = new Op(nextOpId++, id, key, Op.Kind.WRITE, v, null, sim.now());
                history.add(op);
                write(op, KvStateMachine.put(id, ++seq, key, bytes(v)));
            } else {
                String expected = r.nextDouble() < 0.8 ? lastSeen.get(key) : null;
                String v = "c" + id + "." + (++written);
                Op op = new Op(nextOpId++, id, key, Op.Kind.CAS, v, expected, sim.now());
                history.add(op);
                write(op, KvStateMachine.cas(id, ++seq, key, bytes(expected), bytes(v)));
            }
        }

        private void done() {
            outstanding--;
            sim.after(sim.random.nextInt(15), this::next);
        }

        private SimNode pick() {
            List<String> up = upNodes();
            if (up.isEmpty()) return null;
            if (guess != null && nodes.get(guess).alive() && sim.random.nextDouble() < 0.9) return nodes.get(guess);
            return nodes.get(up.get(sim.random.nextInt(up.size())));
        }

        private void redirect(Throwable err) {
            Throwable cause = err instanceof java.util.concurrent.CompletionException ce ? ce.getCause() : err;
            guess = cause instanceof NotLeaderException nle ? nle.leaderHint() : null;
        }

        // ---- writes and compare-and-set

        private final class Pending {
            boolean done;
            int attempts;
        }

        void write(Op op, byte[] cmd) {
            attemptWrite(op, cmd, new Pending(), op.invoked + OP_DEADLINE_MS);
        }

        private void attemptWrite(Op op, byte[] cmd, Pending p, long deadline) {
            if (p.done) return;
            if (sim.now() > deadline) { // give up: the write may or may not have happened
                p.done = true;
                done();
                return;
            }
            SimNode n = pick();
            int attempt = ++p.attempts;
            if (n != null) {
                RaftNode raft = n.raft;
                sim.after(clientDelay(), () -> {
                    if (p.done || !n.alive() || n.raft != raft) return; // the request was lost with the node
                    CompletableFuture<byte[]> f = raft.propose(cmd);
                    f.whenComplete((result, err) -> sim.after(clientDelay(), () -> {
                        if (p.done) return;
                        if (err == null) {
                            p.done = true;
                            op.returned = sim.now();
                            if (op.kind == Op.Kind.CAS) {
                                op.casResult = result.length == 1 && result[0] == 1;
                                if (op.casResult) lastSeen.put(op.key, op.value);
                            } else {
                                lastSeen.put(op.key, op.value);
                            }
                            done();
                        } else if (p.attempts == attempt) {
                            redirect(err);
                            sim.after(1 + sim.random.nextInt(10), () -> attemptWrite(op, cmd, p, deadline));
                        }
                    }));
                });
            }
            sim.after(CLIENT_TIMEOUT_MS, () -> {
                if (!p.done && p.attempts == attempt) attemptWrite(op, cmd, p, deadline);
            });
        }

        // ---- linearizable reads through ReadIndex

        void read(Op op) {
            attemptRead(op, new Pending(), op.invoked + OP_DEADLINE_MS, false);
        }

        void attemptRead(Op op, Pending p, long deadline, boolean mustFinish) {
            if (p.done) return;
            if (sim.now() > deadline && !mustFinish) { // a read that never returned has no effect: drop it
                p.done = true;
                stats.opsAbandonedReads++;
                done();
                return;
            }
            SimNode n = pick();
            int attempt = ++p.attempts;
            if (n != null) {
                RaftNode raft = n.raft;
                KvStateMachine machine = n.machine;
                sim.after(clientDelay(), () -> {
                    if (p.done || !n.alive() || n.raft != raft) return;
                    raft.readBarrier().whenComplete((ignored, err) -> {
                        String value = err == null ? str(machine.get(op.key)) : null; // read at the barrier
                        sim.after(clientDelay(), () -> {
                            if (p.done) return;
                            if (err == null) {
                                p.done = true;
                                op.returned = sim.now();
                                op.readResult = value;
                                lastSeen.put(op.key, value);
                                history.add(op);
                                done();
                            } else if (p.attempts == attempt) {
                                redirect(err);
                                sim.after(1 + sim.random.nextInt(10), () -> attemptRead(op, p, deadline, mustFinish));
                            }
                        });
                    });
                });
            }
            sim.after(CLIENT_TIMEOUT_MS, () -> {
                if (!p.done && p.attempts == attempt) attemptRead(op, p, deadline, mustFinish);
            });
        }
    }

    // ------------------------------------------------------------------ the end of a run

    /** Once healed, every key must become readable again. The final reads also join the checked history. */
    private String finalReads() {
        Client checker = new Client(0);
        for (String key : keys) {
            Op op = new Op(nextOpId++, 0, key, Op.Kind.READ, null, null, sim.now());
            outstanding++;
            issuing = false;
            checker.attemptRead(op, checker.new Pending(), Long.MAX_VALUE, true);
            if (!sim.runUntil(() -> outstanding == 0, sim.now() + 10_000))
                return "liveness: key " + key + " was not readable within 10 s of healing the cluster";
        }
        return null;
    }

    private String awaitConvergence() {
        boolean ok = sim.poll(() -> {
            long target = -1;
            Map<String, String> reference = null;
            for (String id : ids) {
                RaftNode.Status s = nodes.get(id).raft.status();
                if (target < 0) target = s.lastLogIndex();
                if (s.lastLogIndex() != target || s.lastApplied() != target || s.commitIndex() != target) return false;
                Map<String, String> d = nodes.get(id).machine.dump();
                if (reference == null) reference = d;
                else if (!reference.equals(d)) return false;
            }
            return true;
        }, 10, sim.now() + 10_000);
        return ok ? null : "replicas did not converge after healing";
    }
}
