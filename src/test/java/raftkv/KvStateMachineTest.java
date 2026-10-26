package raftkv;

import java.nio.charset.StandardCharsets;
import raftkv.kv.KvStateMachine;

public class KvStateMachineTest {
    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    public void putGetDelete() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, KvStateMachine.put(0, 0, "k", b("v1")));
        Check.equal("v1", new String(kv.get("k")), "get after put");
        byte[] prev = kv.apply(2, KvStateMachine.put(0, 0, "k", b("v2")));
        Check.equal("v1", new String(prev), "put returns previous value");
        kv.apply(3, KvStateMachine.delete(0, 0, "k"));
        Check.isTrue(kv.get("k") == null, "deleted");
    }

    @Test
    public void retriedRequestIsAppliedOnce() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, KvStateMachine.put(42, 1, "k", b("first")));
        kv.apply(2, KvStateMachine.put(7, 1, "k", b("other client")));
        // Client 42 retries its seq 1 after a timeout: it must not overwrite the newer value.
        byte[] r = kv.apply(3, KvStateMachine.put(42, 1, "k", b("first")));
        Check.equal("other client", new String(kv.get("k")), "duplicate was not reapplied");
        Check.equal("", new String(r), "duplicate returns the original result");
        kv.apply(4, KvStateMachine.put(42, 2, "k", b("second")));
        Check.equal("second", new String(kv.get("k")), "next sequence number is applied");
    }

    @Test
    public void compareAndSet() {
        KvStateMachine kv = new KvStateMachine();
        Check.isTrue(kv.apply(1, KvStateMachine.cas(0, 0, "k", null, b("a")))[0] == 1, "absent -> a");
        Check.isTrue(kv.apply(2, KvStateMachine.cas(0, 0, "k", null, b("b")))[0] == 0, "key no longer absent");
        Check.isTrue(kv.apply(3, KvStateMachine.cas(0, 0, "k", b("x"), b("b")))[0] == 0, "wrong expected value");
        Check.equal("a", new String(kv.get("k")), "failed cas changes nothing");
        Check.isTrue(kv.apply(4, KvStateMachine.cas(0, 0, "k", b("a"), b("")))[0] == 1, "a -> empty value");
        Check.equal("", new String(kv.get("k")), "empty value is present, not absent");
        Check.isTrue(kv.apply(5, KvStateMachine.cas(0, 0, "k", null, b("c")))[0] == 0, "empty is not absent");
    }
}
