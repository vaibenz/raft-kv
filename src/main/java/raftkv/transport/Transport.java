package raftkv.transport;

import raftkv.core.Message;

/** Fire and forget message delivery. Raft tolerates loss, duplication and reordering, so no guarantees are made. */
public interface Transport extends AutoCloseable {
    void send(String to, Message message);

    @Override
    void close();
}
