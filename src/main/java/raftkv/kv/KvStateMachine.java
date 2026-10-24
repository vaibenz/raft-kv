package raftkv.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import raftkv.core.StateMachine;

/**
 * A key value map driven by the Raft log.
 *
 * <p>Each command carries a client id and a per client sequence number. If a client times out and retries a
 * write that actually did commit (for example because the leader crashed after replicating it), the retry is
 * recognised and the original result is returned instead of applying it twice. Together with ReadIndex this
 * gives linearizable, exactly once semantics.
 */
public final class KvStateMachine implements StateMachine {
    private static final byte PUT = 1, DELETE = 2;
    private static final byte[] STALE = "stale request".getBytes(StandardCharsets.UTF_8);

    private record Session(long seq, byte[] result) {}

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final Map<Long, Session> sessions = new HashMap<>(); // only touched by the apply thread

    public static byte[] put(long clientId, long seq, String key, byte[] value) {
        return encode(PUT, clientId, seq, key, value);
    }

    public static byte[] delete(long clientId, long seq, String key) {
        return encode(DELETE, clientId, seq, key, new byte[0]);
    }

    private static byte[] encode(byte op, long clientId, long seq, String key, byte[] value) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + 8 + 8 + 4 + k.length + value.length)
                .put(op).putLong(clientId).putLong(seq).putInt(k.length).put(k).put(value).array();
    }

    @Override
    public byte[] apply(long index, byte[] command) {
        ByteBuffer b = ByteBuffer.wrap(command);
        byte op = b.get();
        long clientId = b.getLong(), seq = b.getLong();
        byte[] k = new byte[b.getInt()];
        b.get(k);
        byte[] value = new byte[b.remaining()];
        b.get(value);

        if (clientId != 0) { // clientId 0 opts out of deduplication
            Session s = sessions.get(clientId);
            if (s != null && seq <= s.seq()) return seq == s.seq() ? s.result() : STALE;
        }
        String key = new String(k, StandardCharsets.UTF_8);
        byte[] previous = switch (op) {
            case PUT -> data.put(key, value);
            case DELETE -> data.remove(key);
            default -> throw new IllegalStateException("unknown op " + op);
        };
        byte[] result = previous == null ? new byte[0] : previous;
        if (clientId != 0) sessions.put(clientId, new Session(seq, result));
        return result;
    }

    /** Reads the local copy. Call only after {@code RaftNode.readBarrier()} for linearizable results. */
    public byte[] get(String key) {
        return data.get(key);
    }

    public int size() {
        return data.size();
    }

    /** A sorted, printable copy of the whole map, used by tests to compare replicas. */
    public Map<String, String> dump() {
        Map<String, String> out = new TreeMap<>();
        data.forEach((k, v) -> out.put(k, new String(v, StandardCharsets.UTF_8)));
        return out;
    }
}
