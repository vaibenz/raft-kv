package raftkv.sim;

import java.util.ArrayList;
import java.util.List;
import raftkv.Check;
import raftkv.Test;

/** Hand built histories with known answers, so the checker is itself tested before it is trusted. */
public class LinearizabilityTest {
    private final List<Op> h = new ArrayList<>();

    private Op write(int client, String v, long from, long to) {
        Op op = new Op(h.size(), client, "k", Op.Kind.WRITE, v, null, from);
        op.returned = to;
        h.add(op);
        return op;
    }

    private void read(int client, String result, long from, long to) {
        Op op = new Op(h.size(), client, "k", Op.Kind.READ, null, null, from);
        op.returned = to;
        op.readResult = result;
        h.add(op);
    }

    private void cas(int client, String expected, String v, Boolean ok, long from, long to) {
        Op op = new Op(h.size(), client, "k", Op.Kind.CAS, v, expected, from);
        op.returned = to;
        op.casResult = ok;
        h.add(op);
    }

    private boolean linearizable() {
        return Linearizability.check(h) == null;
    }

    @Test
    public void sequentialHistory() {
        write(1, "a", 0, 10);
        read(2, "a", 20, 30);
        write(1, "b", 40, 50);
        read(2, "b", 60, 70);
        Check.isTrue(linearizable(), "a plain sequential history");
    }

    @Test
    public void staleReadAfterWriteReturnedIsRejected() {
        write(1, "a", 0, 10);
        write(1, "b", 20, 30);
        read(2, "a", 40, 50); // b had already been acknowledged before this read began
        Check.isTrue(!linearizable(), "stale read must be rejected");
    }

    @Test
    public void concurrentReadMayObserveEitherValue() {
        write(1, "a", 0, 10);
        write(1, "b", 20, 60);
        read(2, "a", 30, 40); // overlaps the write of b: may see the old value
        read(3, "b", 45, 50); // or the new one
        Check.isTrue(linearizable(), "reads overlapping a write");
    }

    @Test
    public void readsCannotGoBackInTime() {
        write(1, "a", 0, 10);
        write(1, "b", 20, 100);
        read(2, "b", 30, 40); // the write of b has taken effect by 40
        read(3, "a", 50, 60); // so a later read cannot see a again
        Check.isTrue(!linearizable(), "new-old inversion must be rejected");
    }

    @Test
    public void unknownOutcomeWriteMayOrMayNotHaveHappened() {
        write(1, "a", 0, 10);
        Op lost = write(1, "b", 20, 30);
        lost.returned = Op.NEVER;
        read(2, "a", 40, 50);
        Check.isTrue(linearizable(), "an unknown write that never took effect");
        read(2, "b", 60, 70);
        Check.isTrue(linearizable(), "or one that took effect late");
        read(2, "a", 80, 90);
        Check.isTrue(!linearizable(), "but it cannot take effect and then be undone");
    }

    @Test
    public void compareAndSetResultsMustMatchTheState() {
        write(1, "a", 0, 10);
        cas(2, "a", "b", true, 20, 30);
        cas(3, "a", "c", false, 40, 50);
        read(1, "b", 60, 70);
        Check.isTrue(linearizable(), "cas a->b succeeds, cas a->c fails");
        cas(3, "b", "d", false, 80, 90); // the value is b, so this cas had to succeed
        Check.isTrue(!linearizable(), "a failed cas on a matching value is impossible");
    }

    @Test
    public void absentKeyReadsAsNull() {
        read(1, null, 0, 10);
        cas(2, null, "a", true, 20, 30);
        read(1, "a", 40, 50);
        Check.isTrue(linearizable(), "cas from absent");
    }

    @Test
    public void largeConcurrentHistoryIsCheckedQuickly() {
        // 200 rounds of 4 overlapping clients: the cache keeps this from exploding.
        long t = 0;
        String last = null;
        for (int round = 0; round < 200; round++) {
            String v = "v" + round;
            write(1, v, t, t + 40);
            read(2, last, t + 1, t + 2);      // before the write takes effect
            read(3, v, t + 30, t + 35);       // after
            read(4, v, t + 41, t + 45);
            last = v;
            t += 50;
        }
        long start = System.currentTimeMillis();
        Check.isTrue(linearizable(), "800 operation history");
        Check.isTrue(System.currentTimeMillis() - start < 2000, "checked in under 2 s");
    }
}
