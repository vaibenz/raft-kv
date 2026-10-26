package raftkv.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import raftkv.io.Disk;

/**
 * A log structured merge tree: writes go to an in memory table, which is flushed to an immutable sorted table on
 * disk when full; tables are merged in the background of the write path by leveled compaction.
 *
 * <p><b>Levels.</b> Level 0 holds freshly flushed tables, which may overlap; a lookup checks them newest first.
 * Every deeper level is one sorted run of non overlapping tables, so a lookup checks at most one table per
 * level. Level 0 is merged into level 1 once it has {@code l0Trigger} tables; level {@code i >= 1} is allowed
 * {@code levelBaseBytes * 10^(i-1)} bytes, and when it is over, one of its tables (chosen round robin through the
 * key space) is merged into the overlapping tables of the level below. Tombstones are dropped once a merge writes
 * to the deepest level that holds data, because nothing older can be hiding under them.
 *
 * <p><b>Durability.</b> There is no write ahead log here. The owner (the Raft state machine) already has one: the
 * Raft log. The owner tells the store which log position its contents reflect ({@link #setApplied}), and a
 * {@link #flush} makes the memtable and that position durable together, through one atomic manifest update.
 * After a crash the store reopens at the last flushed position, and the owner replays the log from there.
 *
 * <p><b>Reads.</b> Each table keeps its block index and a bloom filter in memory, so a point lookup costs at most
 * one block read per table it cannot rule out, and decoded blocks are kept in a small LRU cache.
 *
 * <p>All methods are synchronized: the apply thread writes while HTTP threads read. Flushes and compactions run
 * inline on the writer, which keeps the code simple at the cost of write latency spikes.
 */
public final class LsmStore implements AutoCloseable {
    /** Sizing knobs. The tiny settings make every mechanism run constantly, for tests and the simulator. */
    public record Options(int memtableBytes, int blockBytes, long tableBytes, int l0Trigger, long levelBaseBytes,
                          int bitsPerKey, int cacheBlocks) {
        public static Options defaults() {
            return new Options(4 << 20, 4096, 2L << 20, 4, 8L << 20, 10, 2048);
        }

        public static Options tiny() {
            return new Options(2048, 256, 4096, 3, 12_288, 10, 32);
        }
    }

    /** Counters for benchmarks and tests. */
    public static final class Stats {
        public long userBytes, flushes, flushBytes, compactions, compactionBytesRead, compactionBytesWritten;
        public long gets, memtableHits, bloomSkips, bloomFalsePositives, blockReads, cacheHits;

        /** Bytes written to disk per byte the user wrote. */
        public double writeAmplification() {
            return userBytes == 0 ? 0 : (double) (flushBytes + compactionBytesWritten) / userBytes;
        }
    }

    private final Disk disk;
    private final Options options;
    private final TreeMap<byte[], byte[]> memtable = new TreeMap<>(Entry.KEY_ORDER); // null value = tombstone
    private long memtableBytes;
    private Manifest manifest;
    private final List<List<SsTable>> levels = new ArrayList<>();
    private final List<byte[]> compactPointer = new ArrayList<>();
    private final Map<Long, SsTable.Block> cache;
    private long appliedIndex, appliedTerm;
    private final Stats stats = new Stats();

    private LsmStore(Disk disk, Options options) {
        this.disk = disk;
        this.options = options;
        this.cache = new LinkedHashMap<>(64, 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, SsTable.Block> eldest) {
                return size() > options.cacheBlocks();
            }
        };
    }

    public static LsmStore open(Disk disk, Options options) throws IOException {
        LsmStore s = new LsmStore(disk, options);
        try {
            s.manifest = Manifest.load(disk);
            for (SsTable.Meta m : s.manifest.tables()) {
                s.level(m.level()).add(SsTable.open(disk, m));
            }
        } catch (UncheckedIOException e) {
            s.close();
            throw e.getCause();
        } catch (IOException e) {
            s.close();
            throw e;
        }
        s.level(0).sort(Comparator.comparingLong((SsTable t) -> t.meta.id()).reversed()); // newest first
        for (int i = 1; i < s.levels.size(); i++) s.levels.get(i).sort((a, b) -> Entry.KEY_ORDER.compare(a.meta.smallest(), b.meta.smallest()));
        s.appliedIndex = s.manifest.appliedIndex();
        s.appliedTerm = s.manifest.appliedTerm();
        s.deleteGarbage();
        return s;
    }

    /** Table files from flushes or compactions that never committed, or that a committed one replaced. */
    private void deleteGarbage() {
        Set<String> live = new HashSet<>();
        for (SsTable.Meta m : manifest.tables()) live.add(m.fileName());
        for (String f : disk.list())
            if ((f.startsWith("sst-") && !live.contains(f)) || f.endsWith(".tmp")) disk.delete(f);
    }

    private List<SsTable> level(int i) {
        while (levels.size() <= i) {
            levels.add(new ArrayList<>());
            compactPointer.add(null);
        }
        return levels.get(i);
    }

    // ------------------------------------------------------------------ reads

    public synchronized byte[] get(byte[] key) {
        stats.gets++;
        if (memtable.containsKey(key)) {
            stats.memtableHits++;
            return memtable.get(key);
        }
        for (SsTable t : level(0)) { // overlapping: newest first
            Entry e = probe(t, key);
            if (e != null) return e.value();
        }
        for (int i = 1; i < levels.size(); i++) {
            SsTable t = tableFor(levels.get(i), key);
            if (t == null) continue;
            Entry e = probe(t, key);
            if (e != null) return e.value();
        }
        return null;
    }

    /** In a sorted, non overlapping level: the one table whose range could contain the key. */
    private static SsTable tableFor(List<SsTable> run, byte[] key) {
        int lo = 0, hi = run.size() - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Entry.KEY_ORDER.compare(run.get(mid).meta.smallest(), key) <= 0) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (found < 0) return null;
        SsTable t = run.get(found);
        return Entry.KEY_ORDER.compare(t.meta.largest(), key) >= 0 ? t : null;
    }

    private Entry probe(SsTable t, byte[] key) {
        if (Entry.KEY_ORDER.compare(key, t.meta.smallest()) < 0 || Entry.KEY_ORDER.compare(key, t.meta.largest()) > 0)
            return null;
        if (!t.mightContain(key)) {
            stats.bloomSkips++;
            return null;
        }
        int b = t.blockFor(key);
        Entry e = b < 0 ? null : SsTable.find(block(t, b), key);
        if (e == null) stats.bloomFalsePositives++;
        return e;
    }

    private SsTable.Block block(SsTable t, int i) {
        long cacheKey = t.meta.id() * 1_000_000L + i;
        SsTable.Block b = cache.get(cacheKey);
        if (b != null) {
            stats.cacheHits++;
            return b;
        }
        try {
            b = t.readBlock(i);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        stats.blockReads++;
        cache.put(cacheKey, b);
        return b;
    }

    /** Every live key and value in key order: the newest version of each key, tombstones removed. */
    public synchronized List<Entry> liveEntries() {
        List<Entry> out = new ArrayList<>();
        Iterator<Entry> it = merged(true);
        while (it.hasNext()) {
            Entry e = it.next();
            if (!e.isTombstone()) out.add(e);
        }
        return out;
    }

    private Iterator<Entry> merged(boolean includeMemtable) {
        List<Iterator<Entry>> sources = new ArrayList<>();
        if (includeMemtable) {
            List<Entry> mem = new ArrayList<>(memtable.size());
            memtable.forEach((k, v) -> mem.add(new Entry(k, v)));
            sources.add(mem.iterator());
        }
        for (SsTable t : level(0)) sources.add(t.iterator());
        for (int i = 1; i < levels.size(); i++) for (SsTable t : levels.get(i)) sources.add(t.iterator());
        return new MergingIterator(sources);
    }

    // ------------------------------------------------------------------ writes

    public synchronized void put(byte[] key, byte[] value) {
        memtable.put(key, value);
        memtableBytes += key.length + value.length + 32;
        stats.userBytes += key.length + value.length;
    }

    public synchronized void delete(byte[] key) {
        memtable.put(key, null);
        memtableBytes += key.length + 32;
        stats.userBytes += key.length;
    }

    /** Records which position of the owner's log the current contents reflect. Made durable by {@link #flush}. */
    public synchronized void setApplied(long index, long term) {
        appliedIndex = index;
        appliedTerm = term;
    }

    public synchronized boolean shouldFlush() {
        return memtableBytes >= options.memtableBytes();
    }

    public synchronized long durableIndex() {
        return manifest.appliedIndex();
    }

    public synchronized long durableTerm() {
        return manifest.appliedTerm();
    }

    /**
     * Writes the memtable to a new level 0 table and commits it, together with the applied position, in one
     * manifest update. Then compacts if any level is over its limit.
     */
    public synchronized void flush() {
        if (memtable.isEmpty() && appliedIndex == manifest.appliedIndex() && appliedTerm == manifest.appliedTerm())
            return;
        List<SsTable.Meta> tables = new ArrayList<>(manifest.tables());
        long next = manifest.nextFileId();
        SsTable.Meta written = null;
        if (!memtable.isEmpty()) {
            SsTable.Builder b = new SsTable.Builder(disk, next++, 0, options.blockBytes(), options.bitsPerKey());
            memtable.forEach((k, v) -> b.add(new Entry(k, v)));
            written = b.finish();
            tables.add(written);
            stats.flushes++;
            stats.flushBytes += written.size();
        }
        commit(new Manifest(appliedIndex, appliedTerm, next, tables));
        if (written != null) level(0).add(0, openTable(written));
        memtable.clear();
        memtableBytes = 0;
        compactIfNeeded();
    }

    private void commit(Manifest m) {
        m.save(disk);
        manifest = m;
    }

    private SsTable openTable(SsTable.Meta m) {
        try {
            return SsTable.open(disk, m);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------ compaction

    private long maxBytes(int level) {
        long limit = options.levelBaseBytes();
        for (int i = 1; i < level; i++) limit *= 10;
        return limit;
    }

    private static long bytes(List<SsTable> run) {
        long n = 0;
        for (SsTable t : run) n += t.meta.size();
        return n;
    }

    private void compactIfNeeded() {
        while (true) {
            if (level(0).size() >= options.l0Trigger()) {
                compact(0);
                continue;
            }
            int over = -1;
            for (int i = 1; i < levels.size() && over < 0; i++) if (bytes(levels.get(i)) > maxBytes(i)) over = i;
            if (over < 0) return;
            compact(over);
        }
    }

    private void compact(int level) {
        List<SsTable> upper;
        byte[] from, to;
        if (level == 0) {
            upper = new ArrayList<>(level(0)); // newest first
            from = upper.get(0).meta.smallest();
            to = upper.get(0).meta.largest();
            for (SsTable t : upper) {
                if (Entry.KEY_ORDER.compare(t.meta.smallest(), from) < 0) from = t.meta.smallest();
                if (Entry.KEY_ORDER.compare(t.meta.largest(), to) > 0) to = t.meta.largest();
            }
        } else {
            List<SsTable> run = levels.get(level);
            byte[] after = compactPointer.get(level);
            SsTable pick = run.get(0);
            if (after != null)
                for (SsTable t : run)
                    if (Entry.KEY_ORDER.compare(t.meta.smallest(), after) > 0) {
                        pick = t;
                        break;
                    }
            compactPointer.set(level, pick.meta.largest());
            upper = List.of(pick);
            from = pick.meta.smallest();
            to = pick.meta.largest();
        }
        List<SsTable> lower = new ArrayList<>();
        for (SsTable t : level(level + 1)) if (t.meta.overlaps(from, to)) lower.add(t);

        boolean bottom = true; // nothing deeper than the output level can hold an older version
        for (int i = level + 2; i < levels.size(); i++) if (!levels.get(i).isEmpty()) bottom = false;

        List<Iterator<Entry>> sources = new ArrayList<>();
        for (SsTable t : upper) sources.add(t.iterator());
        for (SsTable t : lower) sources.add(t.iterator());
        Iterator<Entry> merged = new MergingIterator(sources);

        long next = manifest.nextFileId();
        List<SsTable.Meta> outputs = new ArrayList<>();
        SsTable.Builder b = null;
        while (merged.hasNext()) {
            Entry e = merged.next();
            if (bottom && e.isTombstone()) continue;
            if (b == null) b = new SsTable.Builder(disk, next++, level + 1, options.blockBytes(), options.bitsPerKey());
            b.add(e);
            if (b.estimatedSize() >= options.tableBytes()) {
                outputs.add(b.finish());
                b = null;
            }
        }
        if (b != null) {
            SsTable.Meta last = b.finish();
            if (last != null) outputs.add(last);
        }

        Set<Long> inputIds = new HashSet<>();
        for (SsTable t : upper) inputIds.add(t.meta.id());
        for (SsTable t : lower) inputIds.add(t.meta.id());
        List<SsTable.Meta> tables = new ArrayList<>();
        for (SsTable.Meta m : manifest.tables()) if (!inputIds.contains(m.id())) tables.add(m);
        tables.addAll(outputs);
        commit(new Manifest(manifest.appliedIndex(), manifest.appliedTerm(), next, tables));

        stats.compactions++;
        stats.compactionBytesRead += bytes(upper) + bytes(lower);
        for (SsTable.Meta m : outputs) stats.compactionBytesWritten += m.size();

        level(level).removeAll(upper);
        List<SsTable> out = level(level + 1);
        out.removeAll(lower);
        for (SsTable.Meta m : outputs) out.add(openTable(m));
        out.sort((x, y) -> Entry.KEY_ORDER.compare(x.meta.smallest(), y.meta.smallest()));
        for (SsTable t : upper) retire(t);
        for (SsTable t : lower) retire(t);
    }

    /** Called only after the manifest that drops the table is durable. */
    private void retire(SsTable t) {
        t.close();
        cache.keySet().removeIf(k -> k / 1_000_000L == t.meta.id());
        disk.delete(t.meta.fileName());
    }

    // ------------------------------------------------------------------ snapshots

    /**
     * Replaces the whole contents with {@code sorted} (strictly increasing keys, no tombstones) as of log position
     * {@code index}, durably. The new tables are written first; one manifest update then swaps them in.
     */
    public synchronized void restore(List<Entry> sorted, long index, long term) {
        long next = manifest.nextFileId();
        List<SsTable.Meta> outputs = new ArrayList<>();
        SsTable.Builder b = null;
        for (Entry e : sorted) {
            if (b == null) b = new SsTable.Builder(disk, next++, 1, options.blockBytes(), options.bitsPerKey());
            b.add(e);
            if (b.estimatedSize() >= options.tableBytes()) {
                outputs.add(b.finish());
                b = null;
            }
        }
        if (b != null) {
            SsTable.Meta last = b.finish();
            if (last != null) outputs.add(last);
        }
        List<SsTable> old = new ArrayList<>();
        for (List<SsTable> run : levels) old.addAll(run);
        commit(new Manifest(index, term, next, outputs));
        for (List<SsTable> run : levels) run.clear();
        for (SsTable.Meta m : outputs) level(1).add(openTable(m));
        for (SsTable t : old) retire(t);
        memtable.clear();
        memtableBytes = 0;
        appliedIndex = index;
        appliedTerm = term;
    }

    // ------------------------------------------------------------------ introspection

    public synchronized Stats stats() {
        return stats;
    }

    public synchronized String describeLevels() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < levels.size(); i++) {
            if (levels.get(i).isEmpty()) continue;
            if (sb.length() > 0) sb.append(", ");
            sb.append(String.format("L%d: %d tables, %.1f KB", i, levels.get(i).size(), bytes(levels.get(i)) / 1024.0));
        }
        return sb.length() == 0 ? "empty" : sb.toString();
    }

    public synchronized int tableCount() {
        int n = 0;
        for (List<SsTable> run : levels) n += run.size();
        return n;
    }

    @Override
    public synchronized void close() {
        for (List<SsTable> run : levels) for (SsTable t : run) t.close();
    }
}
