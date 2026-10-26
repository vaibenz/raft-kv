package raftkv.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks that a history of register operations is linearizable: that there is a single order of all operations,
 * consistent with real time (if A returned before B was invoked, A comes first), in which every result matches
 * what a single register would have returned.
 *
 * <p>The search is the Wing and Gong algorithm with Lowe's memoisation, the approach used by Knossos and
 * Porcupine. Calls and returns are kept in one time ordered list. The search repeatedly picks a pending call that
 * could legally take effect next, removes it and its return from the list, and moves on; when it reaches a return
 * whose call has not been linearized, the current order is impossible and it backtracks. A cache of (set of
 * linearized operations, register value) pairs it has already explored stops it from searching the same state
 * twice, which keeps real histories fast despite the exponential worst case.
 *
 * <p>Linearizability is compositional (Herlihy and Wing): a history is linearizable if and only if the
 * sub history of each key is. So each key is checked on its own, which keeps every search small.
 */
public final class Linearizability {
    private Linearizability() {}

    /** The outcome for a whole history: null if linearizable, otherwise the key and the ops that cannot be ordered. */
    public record Violation(String key, List<Op> ops) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("history of key " + key + " is not linearizable:\n");
            for (Op op : ops) sb.append("    ").append(op).append('\n');
            return sb.toString();
        }
    }

    public static Violation check(List<Op> history) {
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        for (Op op : history) byKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
        for (Map.Entry<String, List<Op>> e : byKey.entrySet())
            if (!checkKey(e.getValue())) return new Violation(e.getKey(), e.getValue());
        return null;
    }

    // ------------------------------------------------------------------ the register model

    private record Step(boolean legal, String state) {}

    private static Step step(String state, Op op) {
        return switch (op.kind) {
            case WRITE -> new Step(true, op.value);
            case READ -> new Step(Objects.equals(state, op.readResult), state);
            case CAS -> {
                boolean matches = Objects.equals(state, op.expected);
                if (op.casResult == null) yield new Step(true, matches ? op.value : state); // outcome unknown
                if (op.casResult != matches) yield new Step(false, state);
                yield new Step(true, matches ? op.value : state);
            }
        };
    }

    // ------------------------------------------------------------------ Wing and Gong search

    private static final class Entry {
        final Op op;
        final boolean isCall;
        final int index; // position of op in this key's list, used as the bit in the linearized set
        Entry match;     // call -> its return
        Entry prev, next;

        Entry(Op op, boolean isCall, int index) {
            this.op = op;
            this.isCall = isCall;
            this.index = index;
        }

        long time() {
            return isCall ? op.invoked : op.returned;
        }
    }

    private record CacheKey(BitSet linearized, String state) {}

    private record Frame(Entry call, String stateBefore) {}

    static boolean checkKey(List<Op> ops) {
        List<Entry> entries = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            Entry call = new Entry(ops.get(i), true, i);
            Entry ret = new Entry(ops.get(i), false, i);
            call.match = ret;
            entries.add(call);
            entries.add(ret);
        }
        // Calls sort before returns at the same instant: operations that touch in time are concurrent.
        entries.sort(Comparator.comparingLong(Entry::time).thenComparing(e -> e.isCall ? 0 : 1));

        Entry head = new Entry(null, false, -1);
        Entry prev = head;
        for (Entry e : entries) {
            prev.next = e;
            e.prev = prev;
            prev = e;
        }

        String state = null; // every key starts absent
        BitSet linearized = new BitSet(ops.size());
        Set<CacheKey> cache = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        Entry entry = head.next;
        while (head.next != null) {
            if (entry == null) return false; // only reachable for malformed input
            if (entry.isCall) {
                Step s = step(state, entry.op);
                if (s.legal()) {
                    BitSet next = (BitSet) linearized.clone();
                    next.set(entry.index);
                    if (cache.add(new CacheKey(next, s.state()))) {
                        stack.push(new Frame(entry, state));
                        state = s.state();
                        linearized = next;
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // Reached a return whose call is still pending: nothing chosen so far can work. Backtrack.
                if (stack.isEmpty()) return false;
                Frame f = stack.pop();
                state = f.stateBefore();
                linearized = (BitSet) linearized.clone();
                linearized.clear(f.call().index);
                unlift(f.call());
                entry = f.call().next;
            }
        }
        return true;
    }

    /** Removes a call and its return from the list. */
    private static void lift(Entry call) {
        call.prev.next = call.next;
        if (call.next != null) call.next.prev = call.prev;
        Entry ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) ret.next.prev = ret.prev;
    }

    /** Puts them back, in reverse order of removal. */
    private static void unlift(Entry call) {
        Entry ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) ret.next.prev = ret;
        call.prev.next = call;
        if (call.next != null) call.next.prev = call;
    }
}
