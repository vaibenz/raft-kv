package raftkv.lsm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import raftkv.Check;
import raftkv.Test;
import raftkv.io.RealDisk;

public class LsmStoreTest {
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    private static void checkAgainst(LsmStore store, TreeMap<String, String> model, String where) {
        List<Entry> live = store.liveEntries();
        Check.equal(model.size(), live.size(), where + ": number of live keys");
        int i = 0;
        for (Map.Entry<String, String> e : model.entrySet()) {
            Entry got = live.get(i++);
            Check.equal(e.getKey(), s(got.key()), where + ": key order");
            Check.equal(e.getValue(), s(got.value()), where + ": value of " + e.getKey());
        }
    }

    /**
     * Thousands of random puts, overwrites, deletes and reads against a TreeMap, with sizes so small that flushes and
     * compactions at every level happen constantly. Then the store is reopened from disk and checked again.
     */
    @Test
    public void matchesAModelThroughFlushesCompactionsAndReopen() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        Random r = new Random(42);
        TreeMap<String, String> model = new TreeMap<>();
        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        for (int op = 0; op < 30_000; op++) {
            String key = "key" + r.nextInt(700);
            int kind = r.nextInt(10);
            if (kind < 5) {
                String v = "v" + op + "-" + "x".repeat(r.nextInt(40));
                store.put(b(key), b(v));
                model.put(key, v);
            } else if (kind < 7) {
                store.delete(b(key));
                model.remove(key);
            } else {
                Check.equal(model.get(key), s(store.get(b(key))), "read of " + key + " at op " + op);
            }
            if (store.shouldFlush()) {
                store.setApplied(op, 1);
                store.flush();
            }
        }
        checkAgainst(store, model, "before reopen");
        LsmStore.Stats st = store.stats();
        Check.isTrue(st.flushes > 50 && st.compactions > 20, "flushes and compactions happened: " + st.flushes + ", "
                + st.compactions);
        store.setApplied(99_999, 3);
        store.flush();
        String levels = store.describeLevels();
        store.close();

        LsmStore reopened = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        checkAgainst(reopened, model, "after reopen");
        Check.equal(99_999L, reopened.durableIndex(), "applied index survives");
        Check.equal(3L, reopened.durableTerm(), "applied term survives");
        Check.equal(levels, reopened.describeLevels(), "same tables");
        for (String key : List.of("key1", "key350", "key699", "nope"))
            Check.equal(model.get(key), s(reopened.get(b(key))), "point read after reopen: " + key);
        reopened.close();
    }

    @Test
    public void tombstonesDisappearAtTheBottomLevel() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        for (int i = 0; i < 2000; i++) store.put(b("k" + i), b("value-" + i));
        store.flush();
        for (int i = 0; i < 2000; i++) store.delete(b("k" + i));
        store.flush();
        for (int round = 0; round < 30; round++) { // churn a few other keys until compaction reaches the bottom
            for (int i = 0; i < 100; i++) store.put(b("other" + i), b("x" + round));
            store.flush();
        }
        Check.equal(100, store.liveEntries().size(), "only the other keys are live");
        long stored = 0;
        for (SsTable.Meta m : Manifest.load(new RealDisk(dir)).tables()) stored += m.entries();
        Check.isTrue(stored < 1000, "deleted keys were purged from disk, " + stored + " entries remain");
        store.close();
    }

    @Test
    public void bloomFilterFalsePositiveRateIsAboutOnePercent() {
        int n = 20_000;
        long[] hashes = new long[n];
        for (int i = 0; i < n; i++) hashes[i] = Bloom.hash(b("present-" + i));
        Bloom bloom = Bloom.decode(Bloom.build(hashes, n, 10).encode());
        for (int i = 0; i < n; i++) Check.isTrue(bloom.mightContain(b("present-" + i)), "no false negatives");
        int falsePositives = 0, probes = 200_000;
        for (int i = 0; i < probes; i++) if (bloom.mightContain(b("absent-" + i))) falsePositives++;
        double rate = (double) falsePositives / probes;
        System.out.printf("    bloom filter, 10 bits per key: %.2f%% false positives%n", rate * 100);
        Check.isTrue(rate < 0.015, "false positive rate " + rate);
    }

    @Test
    public void corruptBlockIsDetectedNotReturned() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        for (int i = 0; i < 100; i++) store.put(b(String.format("k%03d", i)), b("value-" + i));
        store.flush();
        store.close();
        Path table;
        try (var files = Files.list(dir)) {
            table = files.filter(p -> p.getFileName().toString().startsWith("sst-")).findFirst().orElseThrow();
        }
        byte[] data = Files.readAllBytes(table);
        data[10] ^= 0x20; // inside the first data block
        Files.write(table, data);
        LsmStore reopened = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        try {
            reopened.get(b("k000"));
            throw new AssertionError("corrupt block was returned as data");
        } catch (java.io.UncheckedIOException expected) {
            Check.isTrue(expected.getMessage().contains("corrupt"), "reports corruption: " + expected.getMessage());
        }
        reopened.close();
    }

    /**
     * A damaged table count must be reported as a corrupt manifest. It used to be trusted before the checksum was
     * checked, so a negative count escaped as IllegalArgumentException and a huge one tried to allocate gigabytes.
     */
    @Test
    public void corruptManifestCountIsDetectedBeforeAllocating() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        RealDisk disk = new RealDisk(dir);
        new Manifest(5, 2, 3, List.of(new SsTable.Meta(1, 0, 100, b("a"), b("z"), 10))).save(disk);
        byte[] good = Files.readAllBytes(dir.resolve(Manifest.FILE));
        Check.equal(1, Manifest.load(disk).tables().size(), "intact manifest loads");
        int countOffset = 4 + 8 + 8 + 8;
        for (int bad : new int[] {-1, Integer.MAX_VALUE, 1 << 20}) {
            byte[] data = good.clone();
            java.nio.ByteBuffer.wrap(data).putInt(countOffset, bad);
            Files.write(dir.resolve(Manifest.FILE), data);
            try {
                Manifest.load(disk);
                throw new AssertionError("manifest with table count " + bad + " was accepted");
            } catch (IOException expected) {
                // ok
            }
        }
        byte[] longer = Arrays.copyOf(good, good.length + 1);
        Files.write(dir.resolve(Manifest.FILE), longer);
        try {
            Manifest.load(disk);
            throw new AssertionError("manifest with a trailing byte was accepted");
        } catch (IOException expected) {
            // ok
        }
    }

    /**
     * The memtable's size must be what it holds, not how many writes it has seen. Rewriting a few keys (as every
     * client's session key is rewritten on each of its commands) used to count every write, so a 2 KB memtable
     * holding three small keys asked to be flushed after a few dozen writes.
     */
    @Test
    public void overwritesDoNotFillTheMemtable() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny()); // 2048 byte memtable
        for (int i = 0; i < 5000; i++) {
            store.put(b("hot" + i % 3), b("v" + i + "-" + "x".repeat(50)));
            if (i % 7 == 0) store.delete(b("hot" + i % 3));
            Check.isTrue(!store.shouldFlush(), "three keys fill the memtable after " + i + " writes");
        }
        store.put(b("big"), b("y".repeat(3000)));
        Check.isTrue(store.shouldFlush(), "a value larger than the memtable fills it");
        store.put(b("big"), b("small"));
        Check.isTrue(!store.shouldFlush(), "overwriting the large value frees its bytes");
        Check.equal(0L, store.stats().flushes, "nothing was flushed");
        store.close();
    }

    /** Each of these used to be accepted: l0Trigger 0 crashed the first flush, levelBaseBytes 0 hung it. */
    @Test
    public void optionsThatCannotWorkAreRefused() {
        int[][] bad = {{0, 256, 4096, 3, 12_288, 10, 32}, {2048, 0, 4096, 3, 12_288, 10, 32},
                {2048, 256, 0, 3, 12_288, 10, 32}, {2048, 256, 4096, 0, 12_288, 10, 32},
                {2048, 256, 4096, -1, 12_288, 10, 32}, {2048, 256, 4096, 3, 0, 10, 32},
                {2048, 256, 4096, 3, 12_288, 0, 32}, {2048, 256, 4096, 3, 12_288, 10, -1}};
        for (int[] o : bad) {
            try {
                new LsmStore.Options(o[0], o[1], o[2], o[3], o[4], o[5], o[6]);
                throw new AssertionError("accepted " + Arrays.toString(o));
            } catch (IllegalArgumentException expected) {
                // refused when built
            }
        }
        new LsmStore.Options(1, 1, 1, 1, 1, 1, 0); // the smallest values that still work
    }

    @Test
    public void restoreReplacesEverythingAtomically() throws IOException {
        Path dir = Files.createTempDirectory("lsm");
        LsmStore store = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        for (int i = 0; i < 500; i++) store.put(b("old" + i), b("x"));
        store.flush();
        List<Entry> image = new ArrayList<>();
        for (int i = 0; i < 300; i++) image.add(new Entry(b(String.format("new%04d", i)), b("y" + i)));
        store.restore(image, 1234, 7);
        Check.equal(300, store.liveEntries().size(), "only the image remains");
        Check.isTrue(store.get(b("old1")) == null, "old data gone");
        store.close();
        LsmStore reopened = LsmStore.open(new RealDisk(dir), LsmStore.Options.tiny());
        Check.equal(1234L, reopened.durableIndex(), "restored position is durable");
        Check.isTrue(Arrays.equals(b("y299"), reopened.get(b("new0299"))), "restored data is durable");
        reopened.close();
    }
}
