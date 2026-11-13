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
}
