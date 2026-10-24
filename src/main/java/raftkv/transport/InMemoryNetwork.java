package raftkv.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import raftkv.core.Message;

/**
 * A simulated network for testing many nodes in one process. It can drop, delay and reorder messages and
 * split the cluster into partitions. Every message is round tripped through {@link Codec}, so nodes never
 * share objects and the wire format is exercised by every test.
 */
public final class InMemoryNetwork implements AutoCloseable {
    private final Map<String, Consumer<Message>> inboxes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService wire = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "network");
        t.setDaemon(true);
        return t;
    });
    private final Random random;
    private volatile double dropRate;
    private volatile int minDelayMs, maxDelayMs;
    private volatile Map<String, Integer> groupOf = Map.of(); // empty map = fully connected

    public InMemoryNetwork(long seed) {
        this.random = new Random(seed);
    }

    public TransportFactory factory() {
        return (id, inbound) -> {
            inboxes.put(id, inbound);
            return new Transport() {
                @Override public void send(String to, Message m) { deliver(id, to, m); }

                @Override public void close() { inboxes.remove(id, inbound); }
            };
        };
    }

    public void setDropRate(double rate) { dropRate = rate; }

    public void setDelay(int minMs, int maxMs) {
        minDelayMs = minMs;
        maxDelayMs = maxMs;
    }

    /** Splits the cluster: nodes can only reach nodes in the same group. */
    @SafeVarargs
    public final void partition(Set<String>... groups) {
        Map<String, Integer> g = new HashMap<>();
        for (int i = 0; i < groups.length; i++) for (String id : groups[i]) g.put(id, i);
        groupOf = Map.copyOf(g);
    }

    public void heal() { groupOf = Map.of(); }

    public boolean connected(String a, String b) {
        Map<String, Integer> g = groupOf;
        return g.isEmpty() || (g.containsKey(a) && g.get(a).equals(g.get(b)));
    }

    private void deliver(String from, String to, Message m) {
        if (!connected(from, to)) return;
        int delay;
        synchronized (random) {
            if (random.nextDouble() < dropRate) return;
            delay = maxDelayMs > minDelayMs ? minDelayMs + random.nextInt(maxDelayMs - minDelayMs) : minDelayMs;
        }
        byte[] wireBytes = Codec.encode(m);
        Runnable arrive = () -> {
            Consumer<Message> inbox = inboxes.get(to);
            if (inbox != null && connected(from, to)) inbox.accept(Codec.decode(wireBytes));
        };
        if (delay == 0) arrive.run();
        else wire.schedule(arrive, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        wire.shutdownNow();
    }
}
