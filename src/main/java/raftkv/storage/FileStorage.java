package raftkv.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import raftkv.core.LogEntry;

/**
 * Durable storage in two files.
 *
 * <p>{@code meta}: term and vote, replaced atomically (write temp file, fsync, rename, fsync directory),
 * so a crash leaves either the old or the new version, never a mix.
 *
 * <p>{@code log}: an append only sequence of records {@code [int length][long term][command][int crc32c]}.
 * On startup the file is scanned; a torn or corrupt record at the tail (from a crash mid write) fails its
 * checksum and is truncated away. The whole log is also cached in memory for fast reads.
 */
public final class FileStorage implements Storage {
    private static final int HEADER = 4 + 8, TRAILER = 4;

    private final Path dir, metaPath;
    private final FileChannel logChannel;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>(); // file offset of each entry
    private long fileSize;
    private long term;
    private String votedFor;
    private boolean dirty;

    public FileStorage(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        this.metaPath = dir.resolve("meta");
        loadMeta();
        this.logChannel = FileChannel.open(dir.resolve("log"), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        loadLog();
    }

    private void loadMeta() throws IOException {
        if (!Files.exists(metaPath)) return;
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(metaPath));
        long t = b.getLong();
        byte[] vote = new byte[b.getInt()];
        b.get(vote);
        int crc = b.getInt();
        if (crc != crc(b.array(), 0, b.position() - 4)) throw new IOException("corrupt meta file in " + dir);
        term = t;
        votedFor = vote.length == 0 ? null : new String(vote, StandardCharsets.UTF_8);
    }

    private void loadLog() throws IOException {
        byte[] data = Files.readAllBytes(dir.resolve("log"));
        ByteBuffer b = ByteBuffer.wrap(data);
        long pos = 0;
        while (b.remaining() >= HEADER + TRAILER) {
            int start = b.position();
            int len = b.getInt();
            if (len < 0 || len > b.remaining() - 8 - TRAILER) break; // torn write
            long t = b.getLong();
            byte[] cmd = new byte[len];
            b.get(cmd);
            int stored = b.getInt();
            if (stored != crc(data, start + 4, 8 + len)) break; // corrupt tail
            entries.add(new LogEntry(t, cmd));
            offsets.add((long) start);
            pos = b.position();
        }
        if (pos != data.length) { // discard the incomplete tail so new appends start on a clean boundary
            logChannel.truncate(pos);
            logChannel.force(true);
        }
        fileSize = pos;
    }

    private static int crc(byte[] data, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(data, off, len);
        return (int) c.getValue();
    }

    @Override public synchronized long term() { return term; }

    @Override public synchronized String votedFor() { return votedFor; }

    @Override
    public synchronized void setTermAndVote(long newTerm, String newVote) {
        byte[] vote = newVote == null ? new byte[0] : newVote.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + vote.length + 4);
        b.putLong(newTerm).putInt(vote.length).put(vote);
        b.putInt(crc(b.array(), 0, b.position()));
        try {
            Path tmp = dir.resolve("meta.tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                b.flip();
                while (b.hasRemaining()) ch.write(b);
                ch.force(true);
            }
            Files.move(tmp, metaPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
                d.force(true); // make the rename itself durable
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        term = newTerm;
        votedFor = newVote;
    }

    @Override public synchronized long lastIndex() { return entries.size(); }

    @Override
    public synchronized long termAt(long index) {
        return index == 0 ? 0 : entries.get((int) index - 1).term();
    }

    @Override
    public synchronized LogEntry get(long index) {
        return entries.get((int) index - 1);
    }

    @Override
    public synchronized List<LogEntry> slice(long from, long toExclusive) {
        return new ArrayList<>(entries.subList((int) from - 1, (int) toExclusive - 1));
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        int size = 0;
        for (LogEntry e : newEntries) size += HEADER + e.command().length + TRAILER;
        ByteBuffer b = ByteBuffer.allocate(size);
        long off = fileSize;
        for (LogEntry e : newEntries) {
            int start = b.position();
            b.putInt(e.command().length).putLong(e.term()).put(e.command());
            b.putInt(crc(b.array(), start + 4, 8 + e.command().length));
            offsets.add(off + start);
            entries.add(e);
        }
        b.flip();
        try {
            long pos = fileSize;
            while (b.hasRemaining()) pos += logChannel.write(b, pos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        fileSize += size;
        dirty = true;
    }

    @Override
    public synchronized void truncateFrom(long index) {
        if (index > entries.size()) return;
        long off = offsets.get((int) index - 1);
        try {
            logChannel.truncate(off);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        entries.subList((int) index - 1, entries.size()).clear();
        offsets.subList((int) index - 1, offsets.size()).clear();
        fileSize = off;
        dirty = true;
    }

    @Override
    public synchronized void sync() {
        if (!dirty) return;
        try {
            logChannel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        dirty = false;
    }

    @Override
    public synchronized void close() {
        try {
            sync();
            logChannel.close();
        } catch (IOException ignored) {
            // closing on shutdown; nothing useful to do
        }
    }
}
