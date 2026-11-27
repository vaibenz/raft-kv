package raftkv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import raftkv.core.StateMachine;
import raftkv.io.RealDisk;
import raftkv.kv.KvStateMachine;
import raftkv.kv.KvStore;
import raftkv.kv.LsmStateMachine;
import raftkv.lsm.LsmStore;

public class KvStateMachineTest {
    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    public void putGetDelete() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, 1, KvStateMachine.put(0, 0, "k", b("v1")));
        Check.equal("v1", new String(kv.get("k")), "get after put");
        byte[] prev = kv.apply(2, 1, KvStateMachine.put(0, 0, "k", b("v2")));
        Check.equal("v1", new String(prev), "put returns previous value");
        kv.apply(3, 1, KvStateMachine.delete(0, 0, "k"));
        Check.isTrue(kv.get("k") == null, "deleted");
    }

    @Test
    public void retriedRequestIsAppliedOnce() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, 1, KvStateMachine.put(42, 1, "k", b("first")));
        kv.apply(2, 1, KvStateMachine.put(7, 1, "k", b("other client")));
        // Client 42 retries its seq 1 after a timeout: it must not overwrite the newer value.
        byte[] r = kv.apply(3, 1, KvStateMachine.put(42, 1, "k", b("first")));
        Check.equal("other client", new String(kv.get("k")), "duplicate was not reapplied");
        Check.equal("", new String(r), "duplicate returns the original result");
        kv.apply(4, 1, KvStateMachine.put(42, 2, "k", b("second")));
        Check.equal("second", new String(kv.get("k")), "next sequence number is applied");
        Check.isTrue(KvStateMachine.isStale(kv.apply(5, 1, KvStateMachine.put(42, 1, "k", b("old")))), "older seq");
        Check.equal("second", new String(kv.get("k")), "stale write not applied");
        Check.isTrue(!KvStateMachine.isStale(kv.apply(6, 1, KvStateMachine.put(0, 0, "s", b("stale request")))),
                "a value that happens to read 'stale request' is not a stale marker");
        Check.isTrue(!KvStateMachine.isStale(kv.apply(7, 1, KvStateMachine.put(0, 0, "s", b("x")))),
                "previous value 'stale request' is returned, not taken for a stale marker");
    }

    @Test
    public void compareAndSet() {
        KvStateMachine kv = new KvStateMachine();
        Check.isTrue(kv.apply(1, 1, KvStateMachine.cas(0, 0, "k", null, b("a")))[0] == 1, "absent -> a");
        Check.isTrue(kv.apply(2, 1, KvStateMachine.cas(0, 0, "k", null, b("b")))[0] == 0, "key no longer absent");
        Check.isTrue(kv.apply(3, 1, KvStateMachine.cas(0, 0, "k", b("x"), b("b")))[0] == 0, "wrong expected value");
        Check.equal("a", new String(kv.get("k")), "failed cas changes nothing");
        Check.isTrue(kv.apply(4, 1, KvStateMachine.cas(0, 0, "k", b("a"), b("")))[0] == 1, "a -> empty value");
        Check.equal("", new String(kv.get("k")), "empty value is present, not absent");
        Check.isTrue(kv.apply(5, 1, KvStateMachine.cas(0, 0, "k", null, b("c")))[0] == 0, "empty is not absent");
    }

    /** A DELETE must report "deleted" for a key that holds an empty value, on both engines. */
    @Test
    public void deleteOfAnEmptyValueIsReportedAsDeleted() throws IOException {
        checkDeleteResults(new KvStateMachine());
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.defaults())) {
            checkDeleteResults(lsm);
        }
    }

    private static <M extends StateMachine & KvStore> void checkDeleteResults(M kv) {
        kv.apply(1, 1, KvStateMachine.put(0, 0, "empty", new byte[0]));
        Check.isTrue(kv.get("empty") != null, "an empty value is present");
        Check.isTrue(kv.apply(2, 1, KvStateMachine.delete(0, 0, "empty")).length > 0, "deleting it is reported");
        Check.isTrue(kv.get("empty") == null, "and it is gone");
        Check.equal(0, kv.apply(3, 1, KvStateMachine.delete(0, 0, "empty")).length, "a second delete finds nothing");
        // A retried delete gets the original answer, even though the key is gone by then.
        kv.apply(4, 1, KvStateMachine.put(0, 0, "k", b("v")));
        Check.isTrue(kv.apply(5, 1, KvStateMachine.delete(9, 1, "k")).length > 0, "delete k");
        Check.isTrue(kv.apply(6, 1, KvStateMachine.delete(9, 1, "k")).length > 0, "retry gets the same answer");
    }

    /**
     * Client sessions travel with the snapshot. A follower that catches up through InstallSnapshot, or a node that
     * restarts from one, must still recognise a retry of a write the snapshot already holds; otherwise the retry is
     * applied a second time and can overwrite a newer value.
     */
    @Test
    public void retriesAreRecognisedAfterSnapshotRestore() throws IOException {
        KvStateMachine mem = new KvStateMachine();
        fillForSnapshot(mem);
        KvStateMachine memCopy = new KvStateMachine();
        memCopy.restore(mem.snapshot(), 3, 1);
        checkRestoredSessions(memCopy);

        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.defaults());
             LsmStateMachine lsmCopy = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.defaults())) {
            fillForSnapshot(lsm);
            lsmCopy.restore(lsm.snapshot(), 3, 1);
            checkRestoredSessions(lsmCopy);
        }
    }

    /**
     * Nodes of one cluster may run different engines, so a follower must be able to install a snapshot taken by a
     * leader on the other engine. The LSM engine used to send its internal keys (a type byte in front of every key,
     * sessions stored as entries), which the in memory engine could not parse, and it read the in memory image as
     * internal keys, so every key and session it held was lost. Keys that sort differently as UTF-16 and as UTF-8
     * are included, because the LSM tables need their keys in UTF-8 byte order.
     */
    @Test
    public void snapshotsMoveBetweenEngines() throws IOException {
        KvStateMachine mem = new KvStateMachine();
        fillForSnapshot(mem);
        mem.apply(4, 1, KvStateMachine.put(0, 0, "\uFF5E", b("fullwidth tilde")));
        mem.apply(5, 1, KvStateMachine.put(-9, 1, "\uD83D\uDE00", b("emoji")));
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.defaults())) {
            lsm.restore(mem.snapshot(), 5, 1);
            Check.equal(mem.dump(), lsm.dump(), "in memory image restored into the LSM engine");
            Check.equal(4, lsm.size(), "key count after restoring an in memory image");
            Check.isTrue(!KvStateMachine.isStale(lsm.apply(6, 1, KvStateMachine.put(-9, 1, "\uD83D\uDE00", b("x")))),
                    "session of a negative client id came across");
            Check.equal("emoji", new String(lsm.get("\uD83D\uDE00"), StandardCharsets.UTF_8), "retry not reapplied");
            checkRestoredSessions(lsm);
        }
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.defaults())) {
            fillForSnapshot(lsm);
            lsm.apply(4, 1, KvStateMachine.put(0, 0, "\uFF5E", b("fullwidth tilde")));
            KvStateMachine copy = new KvStateMachine();
            copy.restore(lsm.snapshot(), 4, 1);
            Check.equal(lsm.dump(), copy.dump(), "LSM image restored into the in memory engine");
            checkRestoredSessions(copy);
        }
    }

    /**
     * A damaged snapshot image must be refused without touching the current state. The in memory engine used to
     * clear its map and sessions before decoding, so an image cut short left it empty while Raft kept applying the
     * log on top as if nothing had happened.
     */
    @Test
    public void damagedSnapshotLeavesTheStateAlone() throws IOException {
        checkDamagedRestore(new KvStateMachine());
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.tiny())) {
            checkDamagedRestore(lsm);
        }
    }

    private static <M extends StateMachine & KvStore> void checkDamagedRestore(M kv) {
        fillForSnapshot(kv);
        byte[] image = kv.snapshot();
        for (int cut : new int[] {2, 6, image.length / 2, image.length - 1}) {
            try {
                kv.restore(java.util.Arrays.copyOf(image, cut), 9, 1);
                throw new AssertionError("an image cut to " + cut + " of " + image.length + " bytes was accepted");
            } catch (RuntimeException expected) {
                // refused
            }
            Check.equal(2, kv.size(), "keys after a refused image of " + cut + " bytes");
            Check.equal("other client", new String(kv.get("k")), "data after a refused image of " + cut + " bytes");
        }
        // A length or count that cannot be right, and bytes after the last session, are refused the same way. A
        // length of 2^31 - 1 used to allocate before the buffer ran out, and died with an OutOfMemoryError.
        java.util.List<byte[]> damaged = new java.util.ArrayList<>();
        damaged.add(java.nio.ByteBuffer.wrap(image.clone()).putInt(4, Integer.MAX_VALUE).array()); // first key length
        damaged.add(java.nio.ByteBuffer.wrap(image.clone()).putInt(4, -1).array());
        damaged.add(java.nio.ByteBuffer.wrap(image.clone()).putInt(0, -1).array()); // key count
        damaged.add(java.util.Arrays.copyOf(image, image.length + 3)); // trailing bytes
        for (byte[] bad : damaged) {
            try {
                kv.restore(bad, 9, 1);
                throw new AssertionError("a damaged image of " + bad.length + " bytes was accepted");
            } catch (IllegalArgumentException expected) {
                // refused
            }
            Check.equal(2, kv.size(), "keys after a refused damaged image");
        }
        checkRestoredSessions(kv);
    }

    private static void fillForSnapshot(StateMachine kv) {
        kv.apply(1, 1, KvStateMachine.put(42, 1, "k", b("first")));
        kv.apply(2, 1, KvStateMachine.put(7, 1, "k", b("other client")));
        kv.apply(3, 1, KvStateMachine.put(42, 2, "j", b("j1")));
    }

    private static <M extends StateMachine & KvStore> void checkRestoredSessions(M kv) {
        Check.equal("other client", new String(kv.get("k")), "data restored");
        byte[] r = kv.apply(4, 1, KvStateMachine.put(42, 2, "j", b("j1")));
        Check.isTrue(!KvStateMachine.isStale(r), "retry of the latest write is not stale");
        Check.equal("", new String(r), "retry gets the original result");
        Check.isTrue(KvStateMachine.isStale(kv.apply(5, 1, KvStateMachine.put(42, 1, "k", b("first")))),
                "an older write from a restored session is stale");
        Check.equal("other client", new String(kv.get("k")), "and is not applied again");
        kv.apply(6, 1, KvStateMachine.put(42, 3, "k", b("third")));
        Check.equal("third", new String(kv.get("k")), "the next sequence number still applies");
    }

    /**
     * size() is a running count on the LSM engine (it used to scan the whole store on every /status). It must
     * agree with the real contents through puts, overwrites, deletes, compare and set, retries, restores and a
     * reopen from disk.
     */
    @Test
    public void keyCountMatchesContentsOnBothEngines() throws IOException {
        checkKeyCount(new KvStateMachine());
        java.nio.file.Path dir = Files.createTempDirectory("raftkv-sm");
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(dir), LsmStore.Options.tiny())) {
            checkKeyCount(lsm);
        }
        try (LsmStateMachine reopened = new LsmStateMachine(new RealDisk(dir), LsmStore.Options.tiny())) {
            Check.equal(reopened.dump().size(), reopened.size(), "count after reopening");
            Check.equal(3, reopened.size(), "keys after reopening");
        }
    }

    private static <M extends StateMachine & KvStore> void checkKeyCount(M kv) {
        long i = 0;
        kv.apply(++i, 1, KvStateMachine.put(0, 0, "a", b("1")));
        kv.apply(++i, 1, KvStateMachine.put(0, 0, "b", b("1")));
        kv.apply(++i, 1, KvStateMachine.put(0, 0, "a", b("2")));            // overwrite: still two
        Check.equal(2, kv.size(), "after an overwrite");
        kv.apply(++i, 1, KvStateMachine.delete(0, 0, "missing"));           // nothing to delete
        kv.apply(++i, 1, KvStateMachine.delete(0, 0, "b"));
        kv.apply(++i, 1, KvStateMachine.delete(0, 0, "b"));                 // already gone
        Check.equal(1, kv.size(), "after deletes");
        kv.apply(++i, 1, KvStateMachine.cas(0, 0, "c", null, b("new")));    // creates c
        kv.apply(++i, 1, KvStateMachine.cas(0, 0, "c", null, b("again")));  // fails, c exists
        kv.apply(++i, 1, KvStateMachine.cas(0, 0, "c", b("new"), b("x")));  // replaces c
        kv.apply(++i, 1, KvStateMachine.cas(0, 0, "d", b("no"), b("x")));   // fails, d absent
        Check.equal(2, kv.size(), "after compare and set");
        kv.apply(++i, 1, KvStateMachine.put(5, 1, "e", b("1")));
        kv.apply(++i, 1, KvStateMachine.put(5, 1, "e", b("1")));            // retry, not applied twice
        kv.apply(++i, 1, KvStateMachine.delete(5, 0, "e"));                 // stale, not applied
        Check.equal(3, kv.size(), "retries and stale writes change nothing");
        for (int k = 0; k < 300; k++) kv.apply(++i, 1, KvStateMachine.put(0, 0, "bulk" + k, b("v")));
        for (int k = 0; k < 300; k++) kv.apply(++i, 1, KvStateMachine.delete(0, 0, "bulk" + k));
        Check.equal(kv.dump().size(), kv.size(), "count matches contents");
        byte[] image = kv.snapshot();
        kv.apply(++i, 1, KvStateMachine.put(0, 0, "later", b("v")));
        kv.restore(image, i, 1);
        Check.equal(3, kv.size(), "count after restore");
    }
    /**
     * The two engines must be interchangeable: every node of a cluster may use either, and a client must not be able
     * to tell which one answered. Random puts, deletes and compare and sets from a few clients, with retries and stale
     * sequence numbers mixed in, go to both; every result, the contents and the key count must agree, also across
     * the flushes and compactions of a tiny LSM tree and a snapshot restored into a fresh copy of each.
     */
    @Test
    public void bothEnginesGiveTheSameResults() throws IOException {
        long seed = Long.getLong("seed", System.nanoTime());
        java.util.Random rnd = new java.util.Random(seed);
        KvStateMachine mem = new KvStateMachine();
        try (LsmStateMachine lsm = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                LsmStore.Options.tiny())) {
            long[] seqs = new long[4]; // client 0 opts out of deduplication; 1 to 3 keep a sequence number
            for (long index = 1; index <= 3000; index++) {
                int client = rnd.nextInt(seqs.length);
                long seq = 0;
                if (client != 0) {
                    int r = rnd.nextInt(10);
                    seq = r == 0 ? seqs[client] : r == 1 ? Math.max(1, seqs[client] - 1 - rnd.nextInt(3)) : ++seqs[client];
                }
                String key = "k" + rnd.nextInt(40);
                byte[] value = rnd.nextInt(8) == 0 ? new byte[0] : b("v" + rnd.nextInt(1000));
                byte[] command = switch (rnd.nextInt(3)) {
                    case 0 -> KvStateMachine.put(client, seq, key, value);
                    case 1 -> KvStateMachine.delete(client, seq, key);
                    default -> {
                        byte[] current = mem.get(key);
                        byte[] expected = rnd.nextBoolean() ? current : rnd.nextBoolean() ? null : b("v" + rnd.nextInt(1000));
                        yield KvStateMachine.cas(client, seq, key, expected, value);
                    }
                };
                byte[] a = mem.apply(index, 1, command), c = lsm.apply(index, 1, command);
                String where = "command " + index + " (seed " + seed + ")";
                Check.equal(KvStateMachine.isStale(a), KvStateMachine.isStale(c), "stale on both engines, " + where);
                if (!KvStateMachine.isStale(a)) Check.isTrue(java.util.Arrays.equals(a, c), "same result, " + where);
                if (index % 500 == 0) {
                    Check.equal(mem.dump(), lsm.dump(), "same contents, " + where);
                    Check.equal(mem.size(), lsm.size(), "same key count, " + where);
                }
            }
            KvStateMachine memCopy = new KvStateMachine();
            memCopy.restore(mem.snapshot(), 3000, 1);
            try (LsmStateMachine lsmCopy = new LsmStateMachine(new RealDisk(Files.createTempDirectory("raftkv-sm")),
                    LsmStore.Options.tiny())) {
                lsmCopy.restore(lsm.snapshot(), 3000, 1);
                Check.equal(mem.dump(), lsmCopy.dump(), "restored LSM copy has the same contents (seed " + seed + ")");
                // The sessions came along too: the same retry is recognised the same way by both copies.
                for (int client = 1; client < seqs.length; client++) {
                    byte[] retry = KvStateMachine.put(client, seqs[client], "retry", b("x"));
                    Check.isTrue(java.util.Arrays.equals(memCopy.apply(3001, 1, retry), lsmCopy.apply(3001, 1, retry)),
                            "a retry after restore gives the same result on both engines (seed " + seed + ")");
                }
                Check.equal(memCopy.dump(), lsmCopy.dump(), "and leaves the same contents (seed " + seed + ")");
            }
        }
    }
}
