package raftkv.core;

/** Observability hooks, used by tests to check Raft's safety properties while the cluster runs. */
public interface Listener {
    Listener NONE = new Listener() {};

    default void onLeader(String nodeId, long term) {}

    default void onApply(String nodeId, long index, long term, byte[] command) {}
}
