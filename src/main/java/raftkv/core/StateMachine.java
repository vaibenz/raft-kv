package raftkv.core;

/** The application that committed commands are applied to, in log order, exactly once per index. */
public interface StateMachine {
    byte[] apply(long index, byte[] command);

    /**
     * A serialized image of the whole state, including anything needed to keep applying commands correctly
     * afterwards (such as client sessions for deduplication). Raft stores it and discards the log it covers.
     */
    byte[] snapshot();

    /** Replaces the whole state with a snapshot image. */
    void restore(byte[] snapshot);
}
