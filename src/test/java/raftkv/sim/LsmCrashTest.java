package raftkv.sim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import raftkv.Check;
import raftkv.Test;
import raftkv.lsm.Entry;
import raftkv.lsm.LsmStore;

/**
 * Crash testing of the LSM tree on the simulated disk. Writes, flushes and the compactions they trigger are
 * interrupted at random I/O calls and between operations. After every crash the store is reopened and must hold
 * exactly the contents of a committed flush: the last one that returned, or the one that was interrupted if its
 * manifest update had already landed. Never a mix, never a lost table, never a resurrected deleted key.
 */
public class LsmCrashTest {
    private static TreeMap<String, String> contents(LsmStore store) {
        TreeMap<String, String> m = new TreeMap<>();
        for (Entry e : store.liveEntries())
            m.put(new String(e.key(), StandardCharsets.UTF_8), new String(e.value(), StandardCharsets.UTF_8));
        return m;
    }

    private static void runSeed(long seed) throws IOException {
        Random r = new Random(seed);
        SimDisk disk = new SimDisk(new Random(seed * 131 + 1));
        double crashRate = r.nextBoolean() ? 0.01 : 0.002;
        LsmStore store = LsmStore.open(disk.root(), LsmStore.Options.tiny());
        TreeMap<String, String> model = new TreeMap<>();
        TreeMap<String, String> durable = new TreeMap<>();
        long durableIndex = 0;
        int crashes = 0;
        for (int step = 1; step <= 3000; step++) {
            String key = "k" + r.nextInt(300);
            if (r.nextInt(4) == 0) {
                store.delete(key.getBytes(StandardCharsets.UTF_8));
                model.remove(key);
            } else {
                String v = "v" + step + "." + "z".repeat(r.nextInt(30));
                store.put(key.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
                model.put(key, v);
            }
            boolean crashNow = r.nextInt(400) == 0;
            if (!store.shouldFlush() && r.nextInt(50) != 0 && !crashNow) continue;
            disk.crashPointRate = crashRate;
            try {
                if (!crashNow) {
                    store.setApplied(step, 1);
                    store.flush();
                    durable = new TreeMap<>(model);
                    durableIndex = step;
                    continue;
                }
            } catch (SimDisk.Crash interrupted) {
                // fall through to recovery
            } finally {
                disk.crashPointRate = 0;
            }
            crashes++;
            disk.crash();
            store = LsmStore.open(disk.root(), LsmStore.Options.tiny());
            TreeMap<String, String> got = contents(store);
            String where = "seed " + seed + " step " + step;
            if (store.durableIndex() == step) { // the interrupted flush had committed
                Check.equal(model, got, where + ": contents of the flush that committed just before the crash");
                durable = new TreeMap<>(model);
                durableIndex = step;
            } else {
                Check.equal(durableIndex, store.durableIndex(), where + ": durable position");
                Check.equal(durable, got, where + ": contents of the last committed flush");
            }
            for (Map.Entry<String, String> e : List.copyOf(durable.entrySet()))
                Check.equal(e.getValue(), new String(store.get(e.getKey().getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8), where + ": point read of " + e.getKey());
            model = new TreeMap<>(durable); // writes after the last flush are gone, as Raft expects
        }
        Check.isTrue(crashes > 0, "seed " + seed + " crashed at least once");
        store.close();
    }

    @Test
    public void everyCrashLeavesExactlyACommittedFlush() throws IOException {
        int seeds = Integer.getInteger("lsm.seeds", 60);
        for (long seed = 1; seed <= seeds; seed++) runSeed(seed);
    }
}
