package raftkv.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import raftkv.io.Disk;

/**
 * The single source of truth for which table files make up the store, and the point up to which the store is
 * durable ({@code appliedIndex} and {@code appliedTerm}, which are opaque to the store and mean "Raft log
 * position" to its owner).
 *
 * <p>It is replaced atomically on every flush and compaction, so a crash leaves either the old version or the
 * new one. Table files that are not named in the current manifest (written by a flush or compaction that never
 * committed, or replaced by one that did) are garbage and are deleted on open.
 */
record Manifest(long appliedIndex, long appliedTerm, long nextFileId, List<SsTable.Meta> tables) {
    static final String FILE = "MANIFEST";
    private static final int MAGIC = 0x4d414e49; // "MANI"

    static Manifest empty() {
        return new Manifest(0, 0, 1, List.of());
    }

    void save(Disk disk) {
        int size = 4 + 8 + 8 + 8 + 4;
        for (SsTable.Meta t : tables) size += 8 + 4 + 8 + 4 + t.smallest().length + 4 + t.largest().length + 8;
        ByteBuffer b = ByteBuffer.allocate(size + 4);
        b.putInt(MAGIC).putLong(appliedIndex).putLong(appliedTerm).putLong(nextFileId).putInt(tables.size());
        for (SsTable.Meta t : tables) {
            b.putLong(t.id()).putInt(t.level()).putLong(t.size());
            b.putInt(t.smallest().length).put(t.smallest());
            b.putInt(t.largest().length).put(t.largest());
            b.putLong(t.entries());
        }
        b.putInt(SsTable.crc(b.array(), 0, size));
        disk.writeAtomically(FILE, b.array());
    }

    static Manifest load(Disk disk) throws IOException {
        if (!disk.exists(FILE)) return empty();
        byte[] data = disk.readAll(FILE);
        try {
            ByteBuffer b = ByteBuffer.wrap(data);
            if (b.getInt() != MAGIC) throw new IOException("bad manifest magic");
            long index = b.getLong(), term = b.getLong(), next = b.getLong();
            int n = b.getInt();
            List<SsTable.Meta> tables = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                long id = b.getLong();
                int level = b.getInt();
                long size = b.getLong();
                byte[] smallest = new byte[b.getInt()];
                b.get(smallest);
                byte[] largest = new byte[b.getInt()];
                b.get(largest);
                tables.add(new SsTable.Meta(id, level, size, smallest, largest, b.getLong()));
            }
            int stored = b.getInt();
            if (stored != SsTable.crc(data, 0, b.position() - 4)) throw new IOException("manifest checksum mismatch");
            return new Manifest(index, term, next, List.copyOf(tables));
        } catch (BufferUnderflowException | NegativeArraySizeException e) {
            throw new IOException("truncated manifest", e);
        }
    }
}
