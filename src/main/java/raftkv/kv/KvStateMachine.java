package raftkv.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
public final class KvStateMachine implements StateMachine, KvStore {
    static final byte PUT = 1, DELETE = 2, CAS = 3;
    static final byte[] STALE = "stale request".getBytes(StandardCharsets.UTF_8);

    /**
     * True if a command was not applied because its client had already sent a newer sequence number. Compared by
     * identity: a PUT returns the previous value, which could hold the same bytes as {@link #STALE}.
     */
    public static boolean isStale(byte[] result) {
        return result == STALE;
    }
    public static final byte[] CAS_OK = {1}, CAS_FAILED = {0};
    /**
     * Result of a DELETE: {@link #DELETED} if the key was there, empty if it was not. Not the old value, which
     * could itself be empty and would make "deleted an empty value" look like "nothing to delete".
     */
    public static final byte[] DELETED = {1};

    /** A decoded command. Shared with {@link LsmStateMachine}, which applies the same commands. */
    record Command(byte op, long clientId, long seq, String key, byte[] value, byte[] expected) {}

    static Command decode(byte[] command) {
        ByteBuffer b = ByteBuffer.wrap(command);
        byte op = b.get();
        long clientId = b.getLong(), seq = b.getLong();
        byte[] k = new byte[b.getInt()];
        b.get(k);
        byte[] expected = null;
        if (op == CAS) {
            int expLen = b.getInt();
            if (expLen >= 0) {
                expected = new byte[expLen];
                b.get(expected);
            }
        }
        byte[] value = new byte[b.remaining()];
        b.get(value);
        return new Command(op, clientId, seq, new String(k, StandardCharsets.UTF_8), value, expected);
    }

    /**
     * Reads one length prefixed field of a snapshot image. The length is checked against what is left before
     * anything is allocated: a damaged length of 2^31 - 1 used to ask for a 2 GiB array and die with an
     * OutOfMemoryError, which is not an exception the caller can refuse the image with.
     */
    static byte[] imageBytes(ByteBuffer b) {
        int len = b.getInt();
        if (len < 0 || len > b.remaining()) throw new IllegalArgumentException("damaged snapshot image: bad length " + len);
        byte[] out = new byte[len];
        b.get(out);
        return out;
    }

    /** Reads an entry count of a snapshot image. A negative one used to read as zero entries. */
    static int imageCount(ByteBuffer b) {
        int n = b.getInt();
        if (n < 0) throw new IllegalArgumentException("damaged snapshot image: negative count " + n);
        return n;
    }

    /** Bytes left after the sessions mean the image is not one this format wrote: refuse it rather than guess. */
    static void imageEnd(ByteBuffer b) {
        if (b.hasRemaining()) throw new IllegalArgumentException("damaged snapshot image: " + b.remaining() + " trailing bytes");
    }

    static byte[] orEmpty(byte[] v) {
        return v == null ? new byte[0] : v;
    }

    static boolean casMatches(byte[] current, byte[] expected) {
        return expected == null ? current == null : current != null && Arrays.equals(current, expected);
    }

    private record Session(long seq, byte[] result) {}

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final Map<Long, Session> sessions = new HashMap<>(); // only touched by the apply thread

    public static byte[] put(long clientId, long seq, String key, byte[] value) {
        return encode(PUT, clientId, seq, key, value);
    }

    public static byte[] delete(long clientId, long seq, String key) {
        return encode(DELETE, clientId, seq, key, new byte[0]);
    }

    /**
     * Compare and set: replaces the value only if the current value equals {@code expected} ({@code null} means
     * the key must be absent). The result is {@link #CAS_OK} or {@link #CAS_FAILED}.
     */
    public static byte[] cas(long clientId, long seq, String key, byte[] expected, byte[] value) {
        int expLen = expected == null ? -1 : expected.length;
        ByteBuffer b = ByteBuffer.allocate(4 + Math.max(expLen, 0) + value.length);
        b.putInt(expLen);
        if (expected != null) b.put(expected);
        b.put(value);
        return encode(CAS, clientId, seq, key, b.array());
    }

    private static byte[] encode(byte op, long clientId, long seq, String key, byte[] value) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + 8 + 8 + 4 + k.length + value.length)
                .put(op).putLong(clientId).putLong(seq).putInt(k.length).put(k).put(value).array();
    }

    @Override
    public byte[] apply(long index, long term, byte[] command) {
        Command c = decode(command);
        if (c.clientId() != 0) { // clientId 0 opts out of deduplication
            Session s = sessions.get(c.clientId());
            if (s != null && c.seq() <= s.seq()) return c.seq() == s.seq() ? s.result() : STALE;
        }
        byte[] result = switch (c.op()) {
            case PUT -> orEmpty(data.put(c.key(), c.value()));
            case DELETE -> data.remove(c.key()) != null ? DELETED : new byte[0];
            case CAS -> {
                if (!casMatches(data.get(c.key()), c.expected())) yield CAS_FAILED;
                data.put(c.key(), c.value());
                yield CAS_OK;
            }
            default -> throw new IllegalStateException("unknown op " + c.op());
        };
        if (c.clientId() != 0) sessions.put(c.clientId(), new Session(c.seq(), result));
        return result;
    }

    /**
     * The map and the client sessions, in key order so that equal states give equal bytes. Sessions must be
     * part of the snapshot: without them a client retrying a write that is already in the snapshot would have it
     * applied a second time. {@link LsmStateMachine} writes and reads the same format, so either engine can install
     * the other's snapshot.
     */
    @Override
    public byte[] snapshot() {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(bytes)) {
            TreeMap<String, byte[]> sorted = new TreeMap<>(data);
            out.writeInt(sorted.size());
            for (Map.Entry<String, byte[]> e : sorted.entrySet()) {
                byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
                out.writeInt(k.length);
                out.write(k);
                out.writeInt(e.getValue().length);
                out.write(e.getValue());
            }
            TreeMap<Long, Session> sortedSessions = new TreeMap<>(sessions);
            out.writeInt(sortedSessions.size());
            for (Map.Entry<Long, Session> e : sortedSessions.entrySet()) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue().seq());
                out.writeInt(e.getValue().result().length);
                out.write(e.getValue().result());
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    @Override
    public byte[] checkpoint(long index, long term) {
        return snapshot(); // in memory: Raft keeps the image
    }

    /**
     * The whole image is decoded before anything is replaced, as {@link LsmStateMachine} does. Clearing first meant a
     * damaged image threw halfway with the map already emptied, and Raft, which logs the error and carries on, kept
     * applying the log on top of the empty map as if the old state were still there.
     */
    @Override
    public void restore(byte[] image, long index, long term) {
        Map<String, byte[]> newData = new HashMap<>();
        Map<Long, Session> newSessions = new HashMap<>();
        if (image.length > 0) {
            ByteBuffer b = ByteBuffer.wrap(image);
            for (int i = 0, n = imageCount(b); i < n; i++) {
                byte[] k = imageBytes(b);
                newData.put(new String(k, StandardCharsets.UTF_8), imageBytes(b));
            }
            for (int i = 0, n = imageCount(b); i < n; i++) {
                long client = b.getLong(), seq = b.getLong();
                newSessions.put(client, new Session(seq, imageBytes(b)));
            }
            imageEnd(b);
        }
        data.clear();
        data.putAll(newData);
        sessions.clear();
        sessions.putAll(newSessions);
    }

    /** Reads the local copy. Call only after {@code RaftNode.readBarrier()} for linearizable results. */
    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    @Override
    public int size() {
        return data.size();
    }

    /** A sorted, printable copy of the whole map, used by tests to compare replicas. */
    @Override
    public Map<String, String> dump() {
        Map<String, String> out = new TreeMap<>();
        data.forEach((k, v) -> out.put(k, new String(v, StandardCharsets.UTF_8)));
        return out;
    }
}
