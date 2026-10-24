package raftkv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import raftkv.core.Listener;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.kv.KvStateMachine;
import raftkv.storage.MemoryStorage;
import raftkv.transport.InMemoryNetwork;

/**
 * A whole Raft cluster in one process, on a simulated network, with tools to crash, restart and partition
 * nodes. It also checks two of Raft's safety properties continuously while tests run:
 * <ul>
 *   <li>Election Safety: at most one leader is ever elected in a given term.</li>
 *   <li>State Machine Safety: no two nodes ever apply different commands at the same log index.</li>
 * </ul>
 */
public final class Cluster implements AutoCloseable {
    public final InMemoryNetwork net;
    public final List<String> ids = new ArrayList<>();
    private final Map<String, MemoryStorage> storages = new HashMap<>();
    private final Map<String, RaftNode> live = new ConcurrentHashMap<>();
    private final Map<String, KvStateMachine> machines = new ConcurrentHashMap<>();
    public final List<String> violations = new CopyOnWriteArrayList<>();
    private final Map<Long, String> leaderOfTerm = new ConcurrentHashMap<>();
    private final Map<Long, String> appliedAt = new ConcurrentHashMap<>();

    private final Listener invariants = new Listener() {
        @Override
        public void onLeader(String node, long term) {
            String prev = leaderOfTerm.putIfAbsent(term, node);
            if (prev != null && !prev.equals(node))
                violations.add("two leaders in term " + term + ": " + prev + " and " + node);
        }

        @Override
        public void onApply(String node, long index, long term, byte[] command) {
            String fingerprint = term + ":" + java.util.Arrays.hashCode(command);
            String prev = appliedAt.putIfAbsent(index, fingerprint);
            if (prev != null && !prev.equals(fingerprint))
                violations.add("index " + index + " applied as " + prev + " and " + fingerprint + " on " + node);
        }
    };

    public Cluster(int size, long seed) {
        net = new InMemoryNetwork(seed);
        for (int i = 1; i <= size; i++) {
            String id = "n" + i;
            ids.add(id);
            storages.put(id, new MemoryStorage());
        }
        for (String id : ids) start(id);
    }

    public void start(String id) {
        List<String> peers = new ArrayList<>(ids);
        peers.remove(id);
        KvStateMachine sm = new KvStateMachine(); // rebuilt from the log, as after a real restart
        RaftNode node = new RaftNode(id, peers, storages.get(id), sm, RaftConfig.fast(), invariants, net.factory());
        machines.put(id, sm);
        live.put(id, node);
        node.start();
    }

    public void crash(String id) {
        RaftNode n = live.remove(id);
        if (n == null) return;
        n.stop();
        storages.get(id).simulateCrash(); // anything not fsynced is gone
    }

    public void restart(String id) {
        if (!live.containsKey(id)) start(id);
    }

    public boolean isLive(String id) {
        return live.containsKey(id);
    }

    public RaftNode node(String id) {
        return live.get(id);
    }

    public KvStateMachine machine(String id) {
        return machines.get(id);
    }

    public int electedLeaders() {
        return leaderOfTerm.size();
    }

    /** The live leader with the highest term, or null. */
    public RaftNode leader() {
        RaftNode best = null;
        for (RaftNode n : live.values()) {
            RaftNode.Status s = n.status();
            if (s.role() == RaftNode.Role.LEADER && (best == null || s.term() > best.status().term())) best = n;
        }
        return best;
    }

    public RaftNode awaitLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode l = leader();
            if (l != null) return l;
            Check.sleep(5);
        }
        throw new AssertionError("no leader elected within " + timeoutMs + " ms");
    }

    /** Writes through whichever node is leader, retrying with the same request id until it commits. */
    public void put(String key, String value, long clientId, long seq, long timeoutMs) {
        byte[] cmd = KvStateMachine.put(clientId, seq, key, value.getBytes());
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode l = leader();
            if (l != null) {
                try {
                    l.propose(cmd).get(300, TimeUnit.MILLISECONDS);
                    return;
                } catch (Exception retry) {
                    // not leader any more, or timed out: try again
                }
            }
            Check.sleep(5);
        }
        throw new AssertionError("write of " + key + " did not commit within " + timeoutMs + " ms");
    }

    /** Waits until every live node has applied the same log and holds identical data. */
    public void awaitConvergence(long timeoutMs) {
        Check.eventually(() -> {
            long target = -1;
            Map<String, String> reference = null;
            for (String id : live.keySet()) {
                RaftNode.Status s = live.get(id).status();
                if (target < 0) target = s.lastLogIndex();
                if (s.lastLogIndex() != target || s.lastApplied() != target || s.commitIndex() != target) return false;
                Map<String, String> d = machines.get(id).dump();
                if (reference == null) reference = d;
                else if (!reference.equals(d)) return false;
            }
            return true;
        }, timeoutMs, "replicas did not converge: " + statuses());
    }

    public Map<String, RaftNode.Status> statuses() {
        Map<String, RaftNode.Status> m = new LinkedHashMap<>();
        for (String id : ids) if (live.containsKey(id)) m.put(id, live.get(id).status());
        return m;
    }

    public void partition(Set<String> a, Set<String> b) {
        net.partition(a, b);
    }

    public void assertSafe() {
        Check.isTrue(violations.isEmpty(), "safety violated: " + violations);
    }

    @Override
    public void close() {
        for (String id : new ArrayList<>(live.keySet())) live.remove(id).stop();
        net.close();
    }
}
