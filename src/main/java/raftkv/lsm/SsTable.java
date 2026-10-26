package raftkv.lsm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.CRC32C;
import raftkv.core.Mutation;
import raftkv.io.Disk;
import raftkv.io.DiskFile;

/**
 * An immutable sorted string table.
 *
 * <pre>
 * [data block]...[data block][bloom filter][block index][footer]
 *
 * data block:  entries [int key length][key][byte kind][int value length][value], then [int crc32c]
 * bloom:       {@link Bloom#encode()}, then [int crc32c]
 * index:       [int blocks] then per block [int key length][last key][long offset][int length], then [int crc32c]
 * footer:      [long index offset][int index length][long bloom offset][int bloom length][long entries]
 *              [int magic][int crc32c of the 36 bytes before it]
 * </pre>
 *
 * Every block carries its own checksum, so corruption is detected on the read that touches it, never returned as
 * data. The index and bloom filter are loaded into memory when the table is opened; a point lookup then costs at
 * most one block read, and usually none for a key the table does not have.
 */
final class SsTable {
    static final int FOOTER = 8 + 4 + 8 + 4 + 8 + 4 + 4;
    private static final int MAGIC = 0x4c534d54; // "LSMT"
    private static final byte PUT = 0, DELETE = 1;

    /** What the manifest records about a table. */
    record Meta(long id, int level, long size, byte[] smallest, byte[] largest, long entries) {
        String fileName() {
            return fileName(id);
        }

        static String fileName(long id) {
            return String.format("sst-%06d.sst", id);
        }

        boolean overlaps(byte[] from, byte[] to) {
            return Entry.KEY_ORDER.compare(largest, from) >= 0 && Entry.KEY_ORDER.compare(smallest, to) <= 0;
        }

        Meta atLevel(int newLevel) {
            return new Meta(id, newLevel, size, smallest, largest, entries);
        }
    }

    static int crc(byte[] data, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(data, off, len);
        return (int) c.getValue();
    }

    private static IOException corrupt(String what, Meta meta) {
        return new IOException("corrupt " + what + " in " + meta.fileName());
    }

    // ------------------------------------------------------------------ writing

    /** Streams sorted entries into a new table file. */
    static final class Builder {
        private final Disk disk;
        private final long id;
        private final int level;
        private final int blockBytes, bitsPerKey;
        private final DiskFile file;
        private final ByteArrayOutputStream block = new ByteArrayOutputStream();
        private final List<byte[]> indexKeys = new ArrayList<>();
        private final List<long[]> indexPositions = new ArrayList<>(); // {offset, length}
        private long[] hashes = new long[1024];
        private long entries, offset;
        private byte[] smallest, lastKey;

        Builder(Disk disk, long id, int level, int blockBytes, int bitsPerKey) {
            this.disk = disk;
            this.id = id;
            this.level = level;
            this.blockBytes = blockBytes;
            this.bitsPerKey = bitsPerKey;
            this.file = disk.open(Meta.fileName(id));
            file.truncate(0);
        }

        void add(Entry e) {
            if (lastKey != null && Entry.KEY_ORDER.compare(lastKey, e.key()) >= 0)
                throw new IllegalArgumentException("keys must be added in strictly increasing order");
            if (smallest == null) smallest = e.key();
            lastKey = e.key();
            if (entries == hashes.length) hashes = java.util.Arrays.copyOf(hashes, hashes.length * 2);
            hashes[(int) entries++] = Bloom.hash(e.key());
            ByteBuffer b = ByteBuffer.allocate(4 + e.key().length + 1 + 4 + (e.value() == null ? 0 : e.value().length));
            b.putInt(e.key().length).put(e.key()).put(e.isTombstone() ? DELETE : PUT);
            b.putInt(e.isTombstone() ? 0 : e.value().length);
            if (!e.isTombstone()) b.put(e.value());
            block.writeBytes(b.array());
            if (block.size() >= blockBytes) flushBlock();
        }

        long entries() {
            return entries;
        }

        /** Bytes written so far plus the block being filled: compaction cuts a new table when this is large. */
        long estimatedSize() {
            return offset + block.size();
        }

        private void flushBlock() {
            if (block.size() == 0) return;
            byte[] body = block.toByteArray();
            byte[] withCrc = ByteBuffer.allocate(body.length + 4).put(body).putInt(crc(body, 0, body.length)).array();
            file.write(withCrc, offset);
            indexKeys.add(lastKey);
            indexPositions.add(new long[] {offset, withCrc.length});
            offset += withCrc.length;
            block.reset();
        }

        private long writeWithCrc(byte[] body) {
            byte[] withCrc = ByteBuffer.allocate(body.length + 4).put(body).putInt(crc(body, 0, body.length)).array();
            long at = offset;
            file.write(withCrc, offset);
            offset += withCrc.length;
            return at;
        }

        /** Finishes the file and forces it to disk. Returns its metadata, or null if no entries were added. */
        Meta finish() {
            if (entries == 0) {
                file.close();
                disk.delete(Meta.fileName(id));
                return null;
            }
            flushBlock();
            byte[] bloom = Bloom.build(hashes, (int) entries, bitsPerKey).encode();
            long bloomOffset = writeWithCrc(bloom);
            int bloomLength = bloom.length + 4;

            int indexSize = 4;
            for (byte[] k : indexKeys) indexSize += 4 + k.length + 8 + 4;
            ByteBuffer index = ByteBuffer.allocate(indexSize).putInt(indexKeys.size());
            for (int i = 0; i < indexKeys.size(); i++) {
                index.putInt(indexKeys.get(i).length).put(indexKeys.get(i));
                index.putLong(indexPositions.get(i)[0]).putInt((int) indexPositions.get(i)[1]);
            }
            long indexOffset = writeWithCrc(index.array());
            int indexLength = indexSize + 4;

            ByteBuffer footer = ByteBuffer.allocate(FOOTER);
            footer.putLong(indexOffset).putInt(indexLength).putLong(bloomOffset).putInt(bloomLength).putLong(entries)
                    .putInt(MAGIC);
            footer.putInt(crc(footer.array(), 0, FOOTER - 4));
            file.write(footer.array(), offset);
            offset += FOOTER;
            if (!Mutation.is(Mutation.SKIP_SST_FSYNC)) file.force(); // durable before the manifest names it
            file.close();
            return new Meta(id, level, offset, smallest, lastKey, entries);
        }
    }

    // ------------------------------------------------------------------ reading

    final Meta meta;
    private final DiskFile file;
    private final Bloom bloom;
    private final byte[][] blockLastKeys;
    private final long[] blockOffsets;
    private final int[] blockLengths;

    private SsTable(Meta meta, DiskFile file, Bloom bloom, byte[][] lastKeys, long[] offsets, int[] lengths) {
        this.meta = meta;
        this.file = file;
        this.bloom = bloom;
        this.blockLastKeys = lastKeys;
        this.blockOffsets = offsets;
        this.blockLengths = lengths;
    }

    static SsTable open(Disk disk, Meta meta) throws IOException {
        if (!disk.exists(meta.fileName())) throw new IOException("missing table file " + meta.fileName());
        DiskFile f = disk.open(meta.fileName());
        try {
            long size = f.size();
            if (size < FOOTER) throw corrupt("footer (file too short)", meta);
            byte[] footer = f.read(size - FOOTER, FOOTER);
            ByteBuffer fb = ByteBuffer.wrap(footer);
            long indexOffset = fb.getLong();
            int indexLength = fb.getInt();
            long bloomOffset = fb.getLong();
            int bloomLength = fb.getInt();
            fb.getLong(); // entries
            if (fb.getInt() != MAGIC || fb.getInt() != crc(footer, 0, FOOTER - 4)) throw corrupt("footer", meta);
            if (indexOffset + indexLength > size || bloomOffset + bloomLength > size) throw corrupt("footer", meta);

            byte[] index = verified(f, indexOffset, indexLength, "block index", meta);
            ByteBuffer ib = ByteBuffer.wrap(index);
            int n = ib.getInt();
            byte[][] keys = new byte[n][];
            long[] offsets = new long[n];
            int[] lengths = new int[n];
            for (int i = 0; i < n; i++) {
                keys[i] = new byte[ib.getInt()];
                ib.get(keys[i]);
                offsets[i] = ib.getLong();
                lengths[i] = ib.getInt();
            }
            Bloom bloom = Bloom.decode(verified(f, bloomOffset, bloomLength, "bloom filter", meta));
            return new SsTable(meta, f, bloom, keys, offsets, lengths);
        } catch (IOException | RuntimeException e) {
            f.close();
            if (e instanceof IOException io) throw io;
            if (e instanceof UncheckedIOException u) throw u.getCause();
            throw new IOException("corrupt table " + meta.fileName(), e);
        }
    }

    /** Reads a region and checks its trailing CRC, returning the body without it. */
    private static byte[] verified(DiskFile f, long offset, int length, String what, Meta meta) throws IOException {
        byte[] raw = f.read(offset, length);
        int body = length - 4;
        int stored = ByteBuffer.wrap(raw, body, 4).getInt();
        if (stored != crc(raw, 0, body)) throw corrupt(what, meta);
        return java.util.Arrays.copyOf(raw, body);
    }

    /** Parsed entries of one block. */
    record Block(byte[][] keys, byte[][] values) {
        int size() {
            return keys.length;
        }
    }

    Block readBlock(int i) throws IOException {
        byte[] body = verified(file, blockOffsets[i], blockLengths[i], "data block " + i, meta);
        List<byte[]> keys = new ArrayList<>(), values = new ArrayList<>();
        ByteBuffer b = ByteBuffer.wrap(body);
        while (b.hasRemaining()) {
            byte[] k = new byte[b.getInt()];
            b.get(k);
            byte kind = b.get();
            byte[] v = new byte[b.getInt()];
            b.get(v);
            keys.add(k);
            values.add(kind == DELETE ? null : v);
        }
        return new Block(keys.toArray(new byte[0][]), values.toArray(new byte[0][]));
    }

    int blockCount() {
        return blockOffsets.length;
    }

    boolean mightContain(byte[] key) {
        return bloom.mightContain(key);
    }

    /** Index of the only block that can hold {@code key}, or -1 if the key is past the end of the table. */
    int blockFor(byte[] key) {
        int lo = 0, hi = blockLastKeys.length - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Entry.KEY_ORDER.compare(blockLastKeys[mid], key) >= 0) {
                found = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return found;
    }

    /** Looks the key up inside a block: the entry (possibly a tombstone) or null. */
    static Entry find(Block block, byte[] key) {
        int lo = 0, hi = block.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = Entry.KEY_ORDER.compare(block.keys()[mid], key);
            if (c == 0) return new Entry(block.keys()[mid], block.values()[mid]);
            if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return null;
    }

    /** Every entry in key order, tombstones included. Block checksum failures surface as UncheckedIOException. */
    Iterator<Entry> iterator() {
        return new Iterator<>() {
            int blockIndex = -1, pos;
            Block current;

            @Override
            public boolean hasNext() {
                while (current == null || pos >= current.size()) {
                    if (++blockIndex >= blockCount()) return false;
                    try {
                        current = readBlock(blockIndex);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    pos = 0;
                }
                return true;
            }

            @Override
            public Entry next() {
                if (!hasNext()) throw new NoSuchElementException();
                Entry e = new Entry(current.keys()[pos], current.values()[pos]);
                pos++;
                return e;
            }
        };
    }

    void close() {
        file.close();
    }
}
