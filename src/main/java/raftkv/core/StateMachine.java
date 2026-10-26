package raftkv.core;

/**
 * The application that committed commands are applied to, in log order, exactly once per index.
 *
 * <p>A state machine either lives in memory, in which case Raft stores its snapshots for it, or has durable
 * storage of its own (the LSM), in which case the Raft log serves as its write ahead log: the machine reports how
 * far its own storage is durable, and Raft only discards log entries the machine no longer needs.
 */
public interface StateMachine {
    byte[] apply(long index, long term, byte[] command);

    /**
     * Called before Raft discards the log up to {@code index} (the last applied entry). An in memory machine
     * returns an image of its state, which Raft stores as the snapshot. A durable machine makes its state durable
     * up to {@code index} and returns an empty array: its own storage is the snapshot.
     */
    byte[] checkpoint(long index, long term);

    /** An image of the whole current state, sent to a follower that is too far behind to catch up from the log. */
    byte[] snapshot();

    /**
     * Replaces the whole state with an image taken at {@code index}. A durable machine must make it durable before
     * returning; Raft relies on that ordering to recover from a crash in the middle of installing a snapshot.
     */
    void restore(byte[] image, long index, long term);

    /** The log position up to which this machine's own storage is durable. Always 0 for an in memory machine. */
    default long durableIndex() {
        return 0;
    }

    /** The term of the entry at {@link #durableIndex()}. */
    default long durableTerm() {
        return 0;
    }
}
