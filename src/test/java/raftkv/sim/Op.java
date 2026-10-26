package raftkv.sim;

import java.util.Objects;

/**
 * One client operation on one key, as observed from outside the cluster: when it was invoked, when (if ever) its
 * result came back, and what that result was.
 *
 * <p>An operation whose outcome is unknown (the client gave up after timeouts, so the write may or may not have
 * been applied) has {@code returned == Long.MAX_VALUE}. The checker may then place it anywhere after its
 * invocation, including after every other operation, which is the same as saying it never happened.
 */
public final class Op {
    public enum Kind { READ, WRITE, CAS }

    public static final long NEVER = Long.MAX_VALUE;

    final int id;
    final int client;
    final String key;
    final Kind kind;
    final String value;    // WRITE: value written; CAS: new value
    final String expected; // CAS: expected value (null means absent)
    final long invoked;
    long returned = NEVER;
    String readResult;     // READ: value observed (null means absent)
    Boolean casResult;     // CAS: whether it succeeded, null if unknown

    Op(int id, int client, String key, Kind kind, String value, String expected, long invoked) {
        this.id = id;
        this.client = client;
        this.key = key;
        this.kind = kind;
        this.value = value;
        this.expected = expected;
        this.invoked = invoked;
    }

    boolean complete() {
        return returned != NEVER;
    }

    @Override
    public String toString() {
        String ret = complete() ? Long.toString(returned) : "?";
        String body = switch (kind) {
            case READ -> "read " + key + " -> " + readResult;
            case WRITE -> "write " + key + " = " + value + (complete() ? "" : " (outcome unknown)");
            case CAS -> "cas " + key + " " + expected + " -> " + value + " : "
                    + (casResult == null ? "unknown" : casResult ? "ok" : "failed");
        };
        return String.format("[%6d, %6s] c%d %s", invoked, ret, client, body);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Op other && other.id == id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
