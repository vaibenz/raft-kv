package raftkv.core;

import java.util.List;

/** The four RPC messages of Raft, modelled as one-way messages so any transport can carry them. */
public sealed interface Message {
    long term();

    String from();

    /**
     * With {@code preVote} set this is a PreVote probe (Raft thesis section 9.6): {@code term} is the term the
     * sender would use, and receivers answer without changing any of their own state.
     */
    record RequestVote(long term, String from, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements Message {}

    /** For a PreVote reply, {@code term} is the responder's real current term. */
    record VoteReply(long term, String from, boolean granted, boolean preVote) implements Message {}

    /**
     * Replicates entries and doubles as the heartbeat. {@code round} identifies the heartbeat round so
     * the leader can confirm it is still leader before serving a linearizable read (ReadIndex).
     */
    record AppendEntries(long term, String from, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                         long leaderCommit, long round) implements Message {}

    /**
     * On failure, {@code conflictIndex} and {@code conflictTerm} let the leader skip back a whole term at a
     * time instead of one entry per round trip.
     */
    record AppendReply(long term, String from, boolean success, long matchIndex, long conflictIndex,
                       long conflictTerm, long round) implements Message {}

    /**
     * Sent instead of AppendEntries when a follower needs entries the leader has already compacted away
     * (section 7). Carries the whole state machine image as of {@code lastIndex}.
     */
    record InstallSnapshot(long term, String from, long lastIndex, long lastTerm, byte[] data, long round)
            implements Message {}

    record SnapshotReply(long term, String from, long lastIndex, long round) implements Message {}
}
