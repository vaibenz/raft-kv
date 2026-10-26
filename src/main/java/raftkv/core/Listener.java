package raftkv.core;

/** Observability hooks, used by tests to check Raft's safety properties while the cluster runs. */
public interface Listener {
    Listener NONE = new Listener() {};

    default void onLeader(String nodeId, long term) {}

    default void onApply(String nodeId, long index, long term, byte[] command) {}

    /** A snapshot was taken locally ({@code installed == false}) or installed from the leader. */
    default void onSnapshot(String nodeId, long index, long term, boolean installed) {}

    /** An unexpected exception escaped a handler. The node logs it and carries on; tests treat it as a failure. */
    default void onError(String nodeId, Throwable error) {}
}
