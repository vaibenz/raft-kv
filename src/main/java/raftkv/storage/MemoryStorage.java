package raftkv.storage;

import java.util.ArrayList;
import java.util.List;
import raftkv.core.LogEntry;

/**
 * In memory storage for tests. It models durability: entries appended but not yet synced are lost by
 * {@link #simulateCrash()}, exactly as they would be if a real process died before fsync. Snapshots are durable
 * as soon as they are saved, matching {@link FileStorage}.
 */
public final class MemoryStorage implements Storage {
    private final List<LogEntry> log = new ArrayList<>(); // entries base+1 .. base+size
    private long base;          // snapshot index
    private long baseTerm;      // snapshot term
    private byte[] snapshot = new byte[0];
    private long term;
    private String votedFor;
    private long syncedLast;    // last index covered by the most recent sync
    private long durablePrefix; // entries up to here were never rewritten since the last sync

    @Override public synchronized long term() { return term; }

    @Override public synchronized String votedFor() { return votedFor; }

    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override public synchronized long lastIndex() { return base + log.size(); }

    private int pos(long index) {
        if (index <= base || index > base + log.size())
            throw new IndexOutOfBoundsException("index " + index + " outside log (" + base + ", " + lastIndex() + "]");
        return (int) (index - base - 1);
    }

    @Override
    public synchronized long termAt(long index) {
        if (index == base) return baseTerm;
        return log.get(pos(index)).term();
    }

    @Override
    public synchronized LogEntry get(long index) {
        return log.get(pos(index));
    }

    @Override
    public synchronized List<LogEntry> slice(long from, long toExclusive) {
        if (from == toExclusive) return new ArrayList<>();
        return new ArrayList<>(log.subList(pos(from), pos(toExclusive - 1) + 1));
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public synchronized void truncateFrom(long index) {
        log.subList(pos(index), log.size()).clear();
        durablePrefix = Math.min(durablePrefix, lastIndex());
    }

    @Override
    public synchronized void sync() {
        syncedLast = lastIndex();
        durablePrefix = lastIndex();
    }

    @Override public synchronized long snapshotIndex() { return base; }

    @Override public synchronized long snapshotTerm() { return baseTerm; }

    @Override public synchronized byte[] snapshotData() { return snapshot.clone(); }

    @Override
    public synchronized void compact(long index, long term, byte[] data) {
        if (index <= base) return;
        log.subList(0, pos(index) + 1).clear();
        base = index;
        baseTerm = term;
        snapshot = data.clone();
        syncedLast = Math.max(syncedLast, base);
        durablePrefix = Math.max(durablePrefix, base);
    }

    @Override
    public synchronized void installSnapshot(long index, long term, byte[] data) {
        if (index > base && index <= lastIndex() && termAt(index) == term) {
            log.subList(0, pos(index) + 1).clear(); // the suffix agrees with the snapshot: keep it
        } else {
            log.clear();
        }
        base = index;
        baseTerm = term;
        snapshot = data.clone();
        syncedLast = Math.max(syncedLast, base);
        durablePrefix = Math.max(durablePrefix, base);
        if (syncedLast > lastIndex()) syncedLast = lastIndex();
        if (durablePrefix > lastIndex()) durablePrefix = lastIndex();
    }

    /** Drops everything that was not synced, as a power loss would. */
    public synchronized void simulateCrash() {
        long keep = Math.max(base, Math.min(syncedLast, durablePrefix));
        if (lastIndex() > keep) log.subList((int) (keep - base), log.size()).clear();
        syncedLast = durablePrefix = lastIndex();
    }

    @Override public void close() {}
}
