package raftkv.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import raftkv.core.LogEntry;
import raftkv.io.Disk;
import raftkv.io.DiskFile;
import raftkv.io.RealDisk;

/**
 * Durable storage in three files on a {@link Disk}.
 *
 * <p>{@code meta}: term and vote. {@code snapshot}: the latest snapshot's index, term and state machine image.
 * Both are replaced atomically (temp file, force, rename, sync directory) and carry a CRC32C.
 *
 * <p>{@code log}: a header {@code [magic][first index][term of the entry before it][crc]} followed by records
 * {@code [int length][long term][command][int crc32c]}. On startup the records are scanned; a torn or corrupt
 * record at the tail (from a crash mid write) fails its checksum and is cut off. The whole log is cached in
 * memory for fast reads.
 *
 * <p>Compaction writes the snapshot first and then rewrites the log without the covered entries. A crash between
 * the two leaves a log that starts before the snapshot; on startup the covered entries are dropped, and if the
 * log disagrees with the snapshot about the term at the snapshot's index it is from an older, conflicting history
 * and is discarded entirely.
 */
public final class FileStorage implements Storage {
    private static final int MAGIC = 0x524b564c; // "RKVL"
    private static final int LOG_HEADER = 4 + 8 + 8 + 4, REC_HEADER = 4 + 8, REC_TRAILER = 4;
    private static final String META = "meta", SNAPSHOT = "snapshot", LOG = "log";

    private final Disk disk;
    private DiskFile logFile;
    private final List<LogEntry> entries = new ArrayList<>(); // base+1 .. base+size
    private final List<Long> offsets = new ArrayList<>();     // file offset of each cached entry
    private long fileSize;
    private long term;
    private String votedFor;
    private long base, baseTerm;
    private byte[] snapshot = new byte[0];
    private boolean dirty;

    public FileStorage(Path dir) throws IOException {
        this(new RealDisk(dir));
    }

    public FileStorage(Disk disk) throws IOException {
        this.disk = disk;
        try {
            loadMeta();
            loadSnapshot();
            loadLog();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static int crc(byte[] data, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(data, off, len);
        return (int) c.getValue();
    }

    private static ByteBuffer withCrc(ByteBuffer b) {
        b.putInt(crc(b.array(), 0, b.position()));
        return b;
    }

    // ------------------------------------------------------------------ loading

    private void loadMeta() throws IOException {
        if (!disk.exists(META)) return;
        ByteBuffer b = ByteBuffer.wrap(disk.readAll(META));
        try {
            long t = b.getLong();
            byte[] vote = new byte[b.getInt()];
            b.get(vote);
            int crc = b.getInt();
            if (crc != crc(b.array(), 0, b.position() - 4)) throw new IOException("corrupt meta file");
            term = t;
            votedFor = vote.length == 0 ? null : new String(vote, StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            throw new IOException("corrupt meta file", e);
        }
    }

    private void loadSnapshot() throws IOException {
        if (!disk.exists(SNAPSHOT)) return;
        ByteBuffer b = ByteBuffer.wrap(disk.readAll(SNAPSHOT));
        try {
            long index = b.getLong(), t = b.getLong();
            byte[] data = new byte[b.getInt()];
            b.get(data);
            int crc = b.getInt();
            if (crc != crc(b.array(), 0, b.position() - 4)) throw new IOException("corrupt snapshot file");
            base = index;
            baseTerm = t;
            snapshot = data;
        } catch (RuntimeException e) {
            throw new IOException("corrupt snapshot file", e);
        }
    }

    private void loadLog() throws IOException {
        if (!disk.exists(LOG)) {
            rewriteLog(List.of());
            return;
        }
        byte[] data = disk.readAll(LOG);
        if (data.length < LOG_HEADER) { // created but never got its header: an empty log
            rewriteLog(List.of());
            return;
        }
        ByteBuffer b = ByteBuffer.wrap(data);
        int magic = b.getInt();
        long firstIndex = b.getLong(), prevTerm = b.getLong();
        if (magic != MAGIC || b.getInt() != crc(data, 0, LOG_HEADER - 4)) throw new IOException("corrupt log header");

        List<LogEntry> found = new ArrayList<>();
        List<Long> foundOffsets = new ArrayList<>();
        long pos = LOG_HEADER;
        while (b.remaining() >= REC_HEADER + REC_TRAILER) {
            int start = b.position();
            int len = b.getInt();
            if (len < 0 || len > b.remaining() - 8 - REC_TRAILER) break; // torn write
            long t = b.getLong();
            byte[] cmd = new byte[len];
            b.get(cmd);
            if (b.getInt() != crc(data, start + 4, 8 + len)) break; // corrupt tail
            found.add(new LogEntry(t, cmd));
            foundOffsets.add((long) start);
            pos = b.position();
        }

        // Line the log up with the snapshot.
        if (firstIndex > base + 1) throw new IOException("log starts at " + firstIndex + ", after snapshot " + base);
        long termAtBase = firstIndex == base + 1 ? prevTerm
                : base - firstIndex < found.size() ? found.get((int) (base - firstIndex)).term() : -1;
        if (termAtBase != baseTerm) { // older, conflicting history (or too short to reach the snapshot)
            rewriteLog(List.of());
            return;
        }
        int skip = (int) (base + 1 - firstIndex);
        if (skip > 0) { // a crash came between saving a snapshot and rewriting the log
            rewriteLog(found.subList(skip, found.size()));
            return;
        }
        entries.addAll(found);
        offsets.addAll(foundOffsets);
        logFile = disk.open(LOG);
        if (pos != data.length) { // discard the incomplete tail so new appends start on a clean boundary
            logFile.truncate(pos);
            logFile.force();
        }
        fileSize = pos;
    }

    /** Replaces the log file with one that starts right after the snapshot and holds {@code keep}. */
    private void rewriteLog(List<LogEntry> keep) {
        List<LogEntry> kept = new ArrayList<>(keep);
        if (logFile != null) logFile.close();
        ByteBuffer header = withCrc(ByteBuffer.allocate(LOG_HEADER).putInt(MAGIC).putLong(base + 1).putLong(baseTerm));
        byte[] body = encode(kept);
        byte[] all = new byte[LOG_HEADER + body.length];
        System.arraycopy(header.array(), 0, all, 0, LOG_HEADER);
        System.arraycopy(body, 0, all, LOG_HEADER, body.length);
        disk.writeAtomically(LOG, all);
        logFile = disk.open(LOG);
        entries.clear();
        offsets.clear();
        long off = LOG_HEADER;
        for (LogEntry e : kept) {
            entries.add(e);
            offsets.add(off);
            off += REC_HEADER + e.command().length + REC_TRAILER;
        }
        fileSize = off;
        dirty = false;
    }

    private static byte[] encode(List<LogEntry> list) {
        int size = 0;
        for (LogEntry e : list) size += REC_HEADER + e.command().length + REC_TRAILER;
        ByteBuffer b = ByteBuffer.allocate(size);
        for (LogEntry e : list) {
            int start = b.position();
            b.putInt(e.command().length).putLong(e.term()).put(e.command());
            b.putInt(crc(b.array(), start + 4, 8 + e.command().length));
        }
        return b.array();
    }

    // ------------------------------------------------------------------ term and vote

    @Override public synchronized long term() { return term; }

    @Override public synchronized String votedFor() { return votedFor; }

    @Override
    public synchronized void setTermAndVote(long newTerm, String newVote) {
        byte[] vote = newVote == null ? new byte[0] : newVote.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + vote.length + 4);
        b.putLong(newTerm).putInt(vote.length).put(vote);
        disk.writeAtomically(META, withCrc(b).array());
        term = newTerm;
        votedFor = newVote;
    }

    // ------------------------------------------------------------------ log

    @Override public synchronized long lastIndex() { return base + entries.size(); }

    private int pos(long index) {
        if (index <= base || index > lastIndex())
            throw new IndexOutOfBoundsException("index " + index + " outside log (" + base + ", " + lastIndex() + "]");
        return (int) (index - base - 1);
    }

    @Override
    public synchronized long termAt(long index) {
        return index == base ? baseTerm : entries.get(pos(index)).term();
    }

    @Override
    public synchronized LogEntry get(long index) {
        return entries.get(pos(index));
    }

    @Override
    public synchronized List<LogEntry> slice(long from, long toExclusive) {
        if (from == toExclusive) return new ArrayList<>();
        return new ArrayList<>(entries.subList(pos(from), pos(toExclusive - 1) + 1));
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        byte[] data = encode(newEntries);
        long off = fileSize;
        for (LogEntry e : newEntries) {
            offsets.add(off);
            entries.add(e);
            off += REC_HEADER + e.command().length + REC_TRAILER;
        }
        logFile.write(data, fileSize);
        fileSize += data.length;
        dirty = true;
    }

    @Override
    public synchronized void truncateFrom(long index) {
        if (index > lastIndex()) return;
        int p = pos(index);
        long off = offsets.get(p);
        logFile.truncate(off);
        entries.subList(p, entries.size()).clear();
        offsets.subList(p, offsets.size()).clear();
        fileSize = off;
        dirty = true;
    }

    @Override
    public synchronized void sync() {
        if (!dirty) return;
        logFile.force();
        dirty = false;
    }

    // ------------------------------------------------------------------ snapshots

    @Override public synchronized long snapshotIndex() { return base; }

    @Override public synchronized long snapshotTerm() { return baseTerm; }

    @Override public synchronized byte[] snapshotData() { return snapshot.clone(); }

    private void saveSnapshot(long index, long t, byte[] data) {
        ByteBuffer b = ByteBuffer.allocate(8 + 8 + 4 + data.length + 4);
        b.putLong(index).putLong(t).putInt(data.length).put(data);
        disk.writeAtomically(SNAPSHOT, withCrc(b).array());
        base = index;
        baseTerm = t;
        snapshot = data.clone();
    }

    @Override
    public synchronized void compact(long index, long t, byte[] data) {
        if (index <= base) return;
        List<LogEntry> keep = new ArrayList<>(entries.subList(pos(index) + 1, entries.size()));
        saveSnapshot(index, t, data); // first the snapshot, then the log (see the class comment)
        rewriteLog(keep);
    }

    @Override
    public synchronized void installSnapshot(long index, long t, byte[] data) {
        List<LogEntry> keep = index > base && index <= lastIndex() && termAt(index) == t
                ? new ArrayList<>(entries.subList(pos(index) + 1, entries.size()))
                : List.of();
        saveSnapshot(index, t, data);
        rewriteLog(keep);
    }

    @Override
    public synchronized void close() {
        try {
            sync();
        } finally {
            logFile.close();
        }
    }
}
