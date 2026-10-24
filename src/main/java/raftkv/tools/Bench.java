package raftkv.tools;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.kv.KvServer;
import raftkv.kv.KvServer.NodeAddress;
import raftkv.kv.KvStateMachine;

/**
 * Write throughput and latency of a real cluster: TCP between nodes and an fsync on every node before a
 * write is acknowledged. All nodes run in this JVM, on this machine, sharing its CPUs and disk.
 *
 * <pre>java -cp out/main raftkv.tools.Bench [nodes=5] [clients=64] [seconds=10]</pre>
 */
public final class Bench {
    public static void main(String[] args) throws Exception {
        int nodes = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        int clients = args.length > 1 ? Integer.parseInt(args[1]) : 64;
        int seconds = args.length > 2 ? Integer.parseInt(args[2]) : 10;

        Map<String, NodeAddress> cluster = new LinkedHashMap<>();
        for (int i = 1; i <= nodes; i++) cluster.put("n" + i, new NodeAddress("127.0.0.1", freePort(), freePort()));
        Path base = Files.createTempDirectory("raftkv-bench");
        List<KvServer> servers = new ArrayList<>();
        for (String id : cluster.keySet()) {
            KvServer s = new KvServer(id, cluster, base.resolve(id), RaftConfig.defaults());
            servers.add(s);
            s.start();
        }
        RaftNode leader = null;
        while (leader == null) {
            Thread.sleep(20);
            for (KvServer s : servers) if (s.node().status().role() == RaftNode.Role.LEADER) leader = s.node();
        }
        final RaftNode target = leader;
        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'x');

        AtomicBoolean measuring = new AtomicBoolean(), stop = new AtomicBoolean();
        AtomicLong ops = new AtomicLong();
        long[][] latencies = new long[clients][];
        int[] counts = new int[clients];
        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            final int cid = c;
            latencies[c] = new long[2_000_000 / clients + 1024];
            threads.add(Thread.ofVirtual().start(() -> {
                for (long seq = 1; !stop.get(); seq++) {
                    long t0 = System.nanoTime();
                    try {
                        target.propose(KvStateMachine.put(cid + 1, seq, "k" + cid + "-" + seq, value))
                                .get(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        continue;
                    }
                    if (measuring.get()) {
                        ops.incrementAndGet();
                        if (counts[cid] < latencies[cid].length) latencies[cid][counts[cid]++] = System.nanoTime() - t0;
                    }
                }
            }));
        }
        Thread.sleep(2000); // warm up the JIT and the page cache
        measuring.set(true);
        long start = System.nanoTime();
        Thread.sleep(seconds * 1000L);
        measuring.set(false);
        double elapsed = (System.nanoTime() - start) / 1e9;
        stop.set(true);
        for (Thread t : threads) t.join();

        long[] all = new long[Arrays.stream(counts).sum()];
        int k = 0;
        for (int c = 0; c < clients; c++) for (int i = 0; i < counts[c]; i++) all[k++] = latencies[c][i];
        Arrays.sort(all);
        System.out.printf("%d nodes, %d concurrent clients, %d s, 100 byte values, fsync on every node%n",
                nodes, clients, seconds);
        System.out.printf("  throughput   %.0f committed writes/s%n", ops.get() / elapsed);
        System.out.printf("  latency ms   p50 %.2f  p99 %.2f  max %.2f%n", pct(all, 0.50), pct(all, 0.99),
                all.length == 0 ? 0 : all[all.length - 1] / 1e6);
        for (KvServer s : servers) s.close();
    }

    private static double pct(long[] sorted, double p) {
        return sorted.length == 0 ? 0 : sorted[(int) Math.min(sorted.length - 1, p * sorted.length)] / 1e6;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
