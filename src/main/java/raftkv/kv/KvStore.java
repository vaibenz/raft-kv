package raftkv.kv;

import java.util.Map;

/** Read access to a key value state machine, whichever engine holds the data. */
public interface KvStore {
    /** The local copy of a value. Call after {@code RaftNode.readBarrier()} for a linearizable read. */
    byte[] get(String key);

    int size();

    /** A sorted, printable copy of every key and value, used by tests to compare replicas. */
    Map<String, String> dump();
}
