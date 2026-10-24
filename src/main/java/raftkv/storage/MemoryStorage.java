package raftkv.storage;

import java.util.ArrayList;
import java.util.List;
import raftkv.core.LogEntry;

/**
 * In memory storage for tests. It models durability: entries appended but not yet synced are lost by
 * {@link #simulateCrash()}, exactly as they would be if a real process died before fsync.
 */
public final class MemoryStorage implements Storage {
    private final List<LogEntry> log = new ArrayList<>();
    private long term;
    private String votedFor;
    private int syncedLength;
    private int durablePrefix; // entries below this index were never rewritten since the last sync

    @Override public synchronized long term() { return term; }

    @Override public synchronized String votedFor() { return votedFor; }

    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override public synchronized long lastIndex() { return log.size(); }

    @Override
    public synchronized long termAt(long index) {
        return index == 0 ? 0 : log.get((int) index - 1).term();
    }

    @Override
    public synchronized LogEntry get(long index) {
        return log.get((int) index - 1);
    }

    @Override
    public synchronized List<LogEntry> slice(long from, long toExclusive) {
        return new ArrayList<>(log.subList((int) from - 1, (int) toExclusive - 1));
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public synchronized void truncateFrom(long index) {
        log.subList((int) index - 1, log.size()).clear();
        durablePrefix = Math.min(durablePrefix, log.size());
    }

    @Override
    public synchronized void sync() {
        syncedLength = log.size();
        durablePrefix = log.size();
    }

    /** Drops everything that was not synced, as a power loss would. */
    public synchronized void simulateCrash() {
        int keep = Math.min(syncedLength, durablePrefix);
        if (log.size() > keep) log.subList(keep, log.size()).clear();
        syncedLength = durablePrefix = log.size();
    }

    @Override public void close() {}
}
