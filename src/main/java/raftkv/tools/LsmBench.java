package raftkv.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;
import raftkv.io.RealDisk;
import raftkv.lsm.LsmStore;

/**
 * Throughput, write amplification and read latency of the LSM tree on its own (no Raft), on a real disk.
 *
 * <pre>java -cp out/main raftkv.tools.LsmBench [keys=1000000] [valueBytes=100] [reads=200000]</pre>
 *
 * Keys are inserted in random order, so every level sees overlapping ranges and compaction has real work to do.
 * Reads are uniform over the whole key space, far larger than the block cache, so most hit the file system.
 */
public final class LsmBench {
    public static void main(String[] args) throws Exception {
        int keys = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        int valueBytes = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int reads = args.length > 2 ? Integer.parseInt(args[2]) : 200_000;
        Path dir = Files.createTempDirectory("lsm-bench");
        Random r = new Random(1);

        int[] order = new int[keys];
        for (int i = 0; i < keys; i++) order[i] = i;
        for (int i = keys - 1; i > 0; i--) {
            int j = r.nextInt(i + 1), t = order[i];
            order[i] = order[j];
            order[j] = t;
        }
        byte[] value = new byte[valueBytes];

        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.defaults());
        long t0 = System.nanoTime();
        for (int i = 0; i < keys; i++) {
            r.nextBytes(value);
            store.put(key(order[i]), value.clone());
            if (store.shouldFlush()) {
                store.setApplied(i, 1);
                store.flush();
            }
        }
        store.setApplied(keys, 1);
        store.flush();
        double writeSecs = (System.nanoTime() - t0) / 1e9;
        LsmStore.Stats w = store.stats();
        long userBytes = w.userBytes;
        System.out.printf("inserted %,d keys (%d B values) in random order: %.2f s, %,.0f puts/s%n", keys, valueBytes,
                writeSecs, keys / writeSecs);
        System.out.printf("  %d flushes, %d compactions, write amplification %.1fx%n", w.flushes, w.compactions,
                w.writeAmplification());
        long onDisk;
        try (Stream<Path> files = Files.list(dir)) {
            onDisk = files.mapToLong(p -> p.toFile().length()).sum();
        }
        System.out.printf("  levels: %s%n  %.1f MB of user data, %.1f MB on disk (%.2fx)%n", store.describeLevels(),
                userBytes / 1e6, onDisk / 1e6, (double) onDisk / userBytes);

        long[] hit = new long[reads], miss = new long[reads];
        long blockReadsBefore = store.stats().blockReads;
        for (int i = 0; i < reads; i++) {
            byte[] k = key(r.nextInt(keys));
            long s = System.nanoTime();
            byte[] v = store.get(k);
            hit[i] = System.nanoTime() - s;
            if (v == null) throw new IllegalStateException("lost key " + new String(k, StandardCharsets.UTF_8));
        }
        long hitBlockReads = store.stats().blockReads - blockReadsBefore;
        long missBlocksBefore = store.stats().blockReads, skipsBefore = store.stats().bloomSkips;
        for (int i = 0; i < reads; i++) {
            byte[] k = (String.format("user%012d", r.nextInt(keys)) + "x").getBytes(StandardCharsets.UTF_8); // between keys
            long s = System.nanoTime();
            if (store.get(k) != null) throw new IllegalStateException("phantom key");
            miss[i] = System.nanoTime() - s;
        }
        long missBlockReads = store.stats().blockReads - missBlocksBefore;
        long skips = store.stats().bloomSkips - skipsBefore;
        Arrays.sort(hit);
        Arrays.sort(miss);
        System.out.printf("point reads of present keys: p50 %.1f us, p99 %.1f us, %.2f block reads per lookup%n",
                hit[reads / 2] / 1e3, hit[reads * 99 / 100] / 1e3, (double) hitBlockReads / reads);
        System.out.printf("point reads of absent keys:  p50 %.1f us, p99 %.1f us, %.3f block reads per lookup "
                        + "(bloom filters ruled out %,d table probes)%n", miss[reads / 2] / 1e3,
                miss[reads * 99 / 100] / 1e3, (double) missBlockReads / reads, skips);
        store.close();
        try (Stream<Path> files = Files.list(dir)) {
            files.forEach(p -> p.toFile().delete());
        }
        Files.deleteIfExists(dir);
    }

    private static byte[] key(int i) {
        return String.format("user%012d", i).getBytes(StandardCharsets.UTF_8);
    }
}
