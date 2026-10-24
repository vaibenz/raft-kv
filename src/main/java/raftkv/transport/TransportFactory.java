package raftkv.transport;

import java.util.function.Consumer;
import raftkv.core.Message;

/** Connects a node to the network, delivering inbound messages to {@code inbound}. */
@FunctionalInterface
public interface TransportFactory {
    Transport connect(String nodeId, Consumer<Message> inbound);
}
