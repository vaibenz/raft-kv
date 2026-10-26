package raftkv.storage;

import java.util.List;
import raftkv.core.LogEntry;

/**
 * Raft's persistent state: the current term, the vote cast in it, the latest snapshot, and the log after it.
 *
 * <p>Term and vote are durable when {@link #setTermAndVote} returns. Log changes are buffered and become
 * durable on {@link #sync()}, which lets the leader group commit many client writes with one fsync. Snapshot
 * operations are durable when they return.
 *
 * <p>Indices are absolute and 1 based. Entries up to {@link #snapshotIndex()} have been compacted into the
 * snapshot and are no longer available individually; only their last term is remembered, so that the
 * consistency check of AppendEntries still works at the boundary.
 */
public interface Storage extends AutoCloseable {
    long term();

    String votedFor();

    void setTermAndVote(long term, String votedFor);

    long lastIndex();

    /** Term of the entry at {@code index}. Valid for {@code snapshotIndex() <= index <= lastIndex()}. */
    long termAt(long index);

    /** Valid for {@code snapshotIndex() < index <= lastIndex()}. */
    LogEntry get(long index);

    /** Entries in [from, toExclusive), as a new list. {@code from} must be after the snapshot. */
    List<LogEntry> slice(long from, long toExclusive);

    void append(List<LogEntry> entries);

    /** Removes the entry at {@code index} and everything after it. {@code index} must be after the snapshot. */
    void truncateFrom(long index);

    void sync();

    /** Index of the last entry covered by the snapshot, or 0 if there is none. */
    long snapshotIndex();

    /** Term of the entry at {@link #snapshotIndex()}, or 0. */
    long snapshotTerm();

    /** The snapshot's state machine image, or an empty array if there is none. */
    byte[] snapshotData();

    /**
     * Saves a snapshot of this node's own state machine taken after applying {@code index}, then discards the
     * log up to and including {@code index}. Entries after it are kept.
     */
    void compact(long index, long term, byte[] data);

    /**
     * Replaces local state with a snapshot received from the leader. If the log holds an entry at {@code index}
     * with the same term, the entries after it are kept (they may still be needed); otherwise the whole log is
     * discarded, because it conflicts with or is older than the snapshot.
     */
    void installSnapshot(long index, long term, byte[] data);

    default long lastTerm() {
        return termAt(lastIndex());
    }

    /** First index still present in the log. */
    default long firstIndex() {
        return snapshotIndex() + 1;
    }

    @Override
    void close();
}
