package raftkv.kv;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import raftkv.core.StateMachine;
import raftkv.io.Disk;
import raftkv.lsm.Entry;
import raftkv.lsm.LsmStore;

/**
 * The key value state machine on the LSM tree, so the data set is durable and no longer has to fit in memory.
 *
 * <p>The Raft log is its write ahead log. Every applied command records its log position in the store, and a
 * flush (when the memtable fills, or when Raft asks for a checkpoint before compacting its log) makes the data and
 * that position durable together. After a crash the store comes back at its last flushed position and Raft
 * replays the log from there.
 *
 * <p>Client sessions for exactly once retries live in the same store, under their own key prefix, so they are
 * flushed, compacted and snapshotted atomically with the data they protect.
 */
public final class LsmStateMachine implements StateMachine, KvStore, AutoCloseable {
    private static final byte SESSION = 0, DATA = 1; // first byte of every internal key

    private final LsmStore store;
    /**
     * Live data keys, kept up to date by every write so that {@link #size()} (shown by /status on every call) is not
     * a scan that reads and buffers every block of every table. Counted once on open and after a restore. Written
     * only by the apply thread, read by HTTP threads.
     */
    private volatile int dataKeys;

    public LsmStateMachine(Disk disk, LsmStore.Options options) throws IOException {
        this.store = LsmStore.open(disk, options);
        this.dataKeys = countDataKeys();
    }

    private int countDataKeys() {
        int n = 0;
        for (Entry e : store.liveEntries()) if (e.key()[0] == DATA) n++;
        return n;
    }

    public LsmStore store() {
        return store;
    }

    private static byte[] dataKey(String key) {
        return dataKey(key.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] dataKey(byte[] k) {
        byte[] out = new byte[k.length + 1];
        out[0] = DATA;
        System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    private static byte[] sessionKey(long clientId) {
        return ByteBuffer.allocate(9).put(SESSION).putLong(clientId).array();
    }

    @Override
    public byte[] apply(long index, long term, byte[] command) {
        KvStateMachine.Command c = KvStateMachine.decode(command);
        byte[] result = applyCommand(c);
        store.setApplied(index, term);
        if (store.shouldFlush()) store.flush();
        return result;
    }

    private byte[] applyCommand(KvStateMachine.Command c) {
        byte[] sk = c.clientId() == 0 ? null : sessionKey(c.clientId());
        if (sk != null) {
            byte[] session = store.get(sk);
            if (session != null) {
                ByteBuffer b = ByteBuffer.wrap(session);
                long seq = b.getLong();
                if (c.seq() <= seq) {
                    if (c.seq() < seq) return KvStateMachine.STALE;
                    byte[] previous = new byte[b.remaining()];
                    b.get(previous);
                    return previous;
                }
            }
        }
        byte[] key = dataKey(c.key());
        byte[] current = store.get(key);
        byte[] result = switch (c.op()) {
            case KvStateMachine.PUT -> {
                if (current == null) dataKeys++;
                store.put(key, c.value());
                yield KvStateMachine.orEmpty(current);
            }
            case KvStateMachine.DELETE -> {
                if (current == null) yield new byte[0];
                store.delete(key);
                dataKeys--;
                yield KvStateMachine.DELETED;
            }
            case KvStateMachine.CAS -> {
                if (!KvStateMachine.casMatches(current, c.expected())) yield KvStateMachine.CAS_FAILED;
                if (current == null) dataKeys++;
                store.put(key, c.value());
                yield KvStateMachine.CAS_OK;
            }
            default -> throw new IllegalStateException("unknown op " + c.op());
        };
        if (sk != null) store.put(sk, ByteBuffer.allocate(8 + result.length).putLong(c.seq()).put(result).array());
        return result;
    }

    @Override
    public byte[] checkpoint(long index, long term) {
        store.setApplied(index, term);
        store.flush();
        return new byte[0]; // the store itself is the snapshot
    }

    /**
     * The data and the client sessions in the same image format as {@link KvStateMachine#snapshot()}: the user's keys
     * and values, then each session as client id, sequence number and result. A cluster may mix the two engines, and
     * a follower must be able to install whatever its leader sends, so the store's internal keys stay internal.
     */
    @Override
    public byte[] snapshot() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            List<Entry> data = new ArrayList<>(), sessions = new ArrayList<>();
            for (Entry e : store.liveEntries()) (e.key()[0] == DATA ? data : sessions).add(e);
            out.writeInt(data.size());
            for (Entry e : data) {
                out.writeInt(e.key().length - 1);
                out.write(e.key(), 1, e.key().length - 1);
                out.writeInt(e.value().length);
                out.write(e.value());
            }
            out.writeInt(sessions.size());
            for (Entry e : sessions) { // value: [long seq][result]
                out.writeLong(ByteBuffer.wrap(e.key(), 1, 8).getLong());
                out.write(e.value(), 0, 8);
                out.writeInt(e.value().length - 8);
                out.write(e.value(), 8, e.value().length - 8);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /**
     * Installs an image in the format of {@link #snapshot()}, from either engine. The whole image is decoded before
     * the store is touched. The in memory engine lists keys in String order, which is not UTF-8 byte order for keys
     * outside the Basic Multilingual Plane, so the entries are sorted into the store's order here.
     */
    @Override
    public void restore(byte[] image, long index, long term) {
        TreeMap<byte[], byte[]> sorted = new TreeMap<>(Entry.KEY_ORDER);
        int keys = 0;
        if (image.length > 0) {
            ByteBuffer b = ByteBuffer.wrap(image);
            for (int i = 0, n = b.getInt(); i < n; i++) {
                byte[] k = new byte[b.getInt()];
                b.get(k);
                byte[] v = new byte[b.getInt()];
                b.get(v);
                if (sorted.put(dataKey(k), v) == null) keys++;
            }
            for (int i = 0, n = b.getInt(); i < n; i++) {
                long client = b.getLong(), seq = b.getLong();
                byte[] r = new byte[b.getInt()];
                b.get(r);
                sorted.put(sessionKey(client), ByteBuffer.allocate(8 + r.length).putLong(seq).put(r).array());
            }
        }
        List<Entry> entries = new ArrayList<>(sorted.size());
        sorted.forEach((k, v) -> entries.add(new Entry(k, v)));
        store.restore(entries, index, term); // durable before Raft records the snapshot in its log
        dataKeys = keys;
    }

    @Override
    public long durableIndex() {
        return store.durableIndex();
    }

    @Override
    public long durableTerm() {
        return store.durableTerm();
    }

    @Override
    public byte[] get(String key) {
        return store.get(dataKey(key));
    }

    @Override
    public int size() {
        return dataKeys;
    }

    @Override
    public Map<String, String> dump() {
        Map<String, String> out = new TreeMap<>();
        for (Entry e : store.liveEntries())
            if (e.key()[0] == DATA)
                out.put(new String(e.key(), 1, e.key().length - 1, StandardCharsets.UTF_8),
                        new String(e.value(), StandardCharsets.UTF_8));
        return out;
    }

    @Override
    public void close() {
        store.close();
    }
}
