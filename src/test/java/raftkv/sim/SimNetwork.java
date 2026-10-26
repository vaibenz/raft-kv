package raftkv.sim;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import raftkv.core.Message;
import raftkv.transport.Codec;
import raftkv.transport.Transport;
import raftkv.transport.TransportFactory;

/**
 * The network inside the simulation. Messages can be dropped, delayed (and so reordered) and duplicated, and any
 * directed link can be cut, which models symmetric partitions as well as one way failures where A hears B but B
 * never hears A. Every message is encoded and decoded through {@link Codec}, so the wire format is exercised and
 * nodes never share objects.
 */
final class SimNetwork {
    private final Simulation sim;
    private final Map<String, Consumer<Message>> inboxes = new HashMap<>();
    private final Set<String> cut = new HashSet<>(); // "a>b" means messages from a to b are lost
    double dropRate;
    double duplicateRate;
    int minDelayMs = 1, maxDelayMs = 10;
    long sent, delivered;

    SimNetwork(Simulation sim) {
        this.sim = sim;
    }

    TransportFactory factory() {
        return (id, inbound) -> {
            inboxes.put(id, inbound);
            return new Transport() {
                @Override public void send(String to, Message m) { deliver(id, to, m); }

                @Override public void close() { inboxes.remove(id, inbound); }
            };
        };
    }

    boolean connected(String from, String to) {
        return !cut.contains(from + ">" + to);
    }

    void cut(String from, String to) {
        cut.add(from + ">" + to);
    }

    /** Splits the nodes into groups that can only talk within themselves. */
    void partition(List<Set<String>> groups) {
        cut.clear();
        for (Set<String> a : groups)
            for (Set<String> b : groups)
                if (a != b) for (String x : a) for (String y : b) cut(x, y);
    }

    void heal() {
        cut.clear();
    }

    private int delay() {
        return minDelayMs + (maxDelayMs > minDelayMs ? sim.random.nextInt(maxDelayMs - minDelayMs + 1) : 0);
    }

    private void deliver(String from, String to, Message m) {
        sent++;
        if (!connected(from, to) || sim.random.nextDouble() < dropRate) return;
        byte[] wire = Codec.encode(m);
        int copies = sim.random.nextDouble() < duplicateRate ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            sim.after(delay(), () -> {
                Consumer<Message> inbox = inboxes.get(to);
                if (inbox == null || !connected(from, to)) return;
                delivered++;
                sim.trace(Arrays.hashCode(wire));
                inbox.accept(Codec.decode(wire));
            });
        }
    }
}
