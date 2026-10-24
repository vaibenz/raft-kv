package raftkv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import raftkv.core.RaftNode;
import raftkv.kv.KvStateMachine;

/**
 * Randomized fault injection, in the spirit of Jepsen. While clients write continuously, a nemesis crashes
 * and restarts nodes (losing unsynced data), partitions the network (including isolating the leader), and
 * the network drops, delays and reorders messages throughout.
 *
 * <p>Checked continuously: Election Safety and State Machine Safety (see {@link Cluster}).
 * Checked at the end: every write that was acknowledged to a client is present on every node, and all
 * replicas hold identical data. Runs are reproducible from the printed seed.
 */
public class ChaosTest {
    @Test
    public void survivesCrashesPartitionsAndMessageLoss() throws Exception {
        long seed = Long.getLong("seed", System.nanoTime());
        int seconds = Integer.getInteger("chaos.seconds", 15);
        Random rnd = new Random(seed);
        System.out.println("    chaos seed " + seed + ", " + seconds + " s (rerun with -Dseed=" + seed + ")");

        try (Cluster c = new Cluster(5, seed)) {
            c.net.setDropRate(0.05);
            c.net.setDelay(0, 8); // random delays also reorder messages
            c.awaitLeader(3000);

            Map<String, String> acknowledged = new ConcurrentHashMap<>();
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger attempts = new AtomicInteger(), faults = new AtomicInteger();
            List<Thread> clients = new ArrayList<>();
            for (int t = 1; t <= 8; t++) {
                final long clientId = t;
                clients.add(Thread.ofPlatform().start(() -> {
                    for (long seq = 1; !stop.get(); seq++) {
                        String key = "c" + clientId + "-" + seq, value = Long.toHexString(seq * 31 + clientId);
                        byte[] cmd = KvStateMachine.put(clientId, seq, key, value.getBytes());
                        for (int attempt = 0; attempt < 20 && !stop.get(); attempt++) { // same seq on retry
                            attempts.incrementAndGet();
                            RaftNode l = c.leader();
                            if (l == null) {
                                Check.sleep(5);
                                continue;
                            }
                            try {
                                l.propose(cmd).get(250, TimeUnit.MILLISECONDS);
                                acknowledged.put(key, value);
                                break;
                            } catch (Exception e) {
                                Check.sleep(5); // outcome unknown: it may still commit, so retry
                            }
                        }
                    }
                }));
            }

            long end = System.currentTimeMillis() + seconds * 1000L;
            while (System.currentTimeMillis() < end) {
                Check.sleep(150 + rnd.nextInt(350));
                faults.incrementAndGet();
                List<String> down = c.ids.stream().filter(id -> !c.isLive(id)).toList();
                switch (rnd.nextInt(6)) {
                    case 0, 1 -> { // crash a random node, keeping at most two down
                        if (down.size() < 2) {
                            List<String> up = c.ids.stream().filter(c::isLive).toList();
                            c.crash(up.get(rnd.nextInt(up.size())));
                        }
                    }
                    case 2 -> { // bring a node back
                        if (!down.isEmpty()) c.restart(down.get(rnd.nextInt(down.size())));
                    }
                    case 3 -> { // isolate the current leader
                        RaftNode l = c.leader();
                        if (l != null) {
                            Set<String> rest = new HashSet<>(c.ids);
                            rest.remove(l.id());
                            c.partition(Set.of(l.id()), rest);
                        }
                    }
                    case 4 -> { // random split
                        Set<String> a = new HashSet<>(), b = new HashSet<>();
                        for (String id : c.ids) (rnd.nextBoolean() ? a : b).add(id);
                        c.partition(a, b);
                    }
                    default -> c.net.heal();
                }
            }

            stop.set(true);
            for (Thread t : clients) t.join();
            c.net.heal();
            c.net.setDropRate(0);
            for (String id : c.ids) c.restart(id);
            c.awaitLeader(5000);
            c.put("final", "barrier", 99, 1, 5000); // a commit in the final term commits everything before it
            c.awaitConvergence(10_000);
            c.assertSafe();

            int missing = 0;
            for (String id : c.ids) {
                Map<String, String> data = c.machine(id).dump();
                for (Map.Entry<String, String> e : acknowledged.entrySet())
                    if (!e.getValue().equals(data.get(e.getKey()))) missing++;
            }
            System.out.printf("    %d faults injected, %d leaders elected, %d write attempts, %d acknowledged%n",
                    faults.get(), c.electedLeaders(), attempts.get(), acknowledged.size());
            Check.equal(0, missing, "acknowledged writes missing from replicas");
            Check.isTrue(acknowledged.size() > 100, "cluster made progress despite faults");
        }
    }
}
