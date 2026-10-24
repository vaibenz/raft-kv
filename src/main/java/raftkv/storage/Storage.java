package raftkv.storage;

import java.util.List;
import raftkv.core.LogEntry;

/**
 * Raft's persistent state: the current term, the vote cast in it, and the log.
 * Term and vote are durable when {@link #setTermAndVote} returns. Log changes are buffered and become
 * durable on {@link #sync()}, which lets the leader group commit many client writes with one fsync.
 */
public interface Storage extends AutoCloseable {
    long term();

    String votedFor();

    void setTermAndVote(long term, String votedFor);

    long lastIndex();

    /** Term of the entry at {@code index}, or 0 for index 0. */
    long termAt(long index);

    LogEntry get(long index);

    /** Entries in [from, toExclusive), as a new list. */
    List<LogEntry> slice(long from, long toExclusive);

    void append(List<LogEntry> entries);

    /** Removes the entry at {@code index} and everything after it. */
    void truncateFrom(long index);

    void sync();

    default long lastTerm() {
        return termAt(lastIndex());
    }

    @Override
    void close();
}
