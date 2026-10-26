package raftkv.sim;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * A discrete event simulator: one virtual clock and one queue of timed events for the whole cluster.
 *
 * <p>Everything that would normally happen concurrently (node event loops, network deliveries, client requests,
 * the nemesis) is an event in this queue, executed one at a time on the calling thread. Events at the same
 * virtual time run in the order they were scheduled. Since every random choice is drawn from {@link #random}
 * or from generators seeded by it, a run is a deterministic function of its seed. Virtual time costs nothing,
 * so a 30 second scenario with thousands of messages typically runs in tens of milliseconds.
 */
public final class Simulation {
    private record Event(long time, long seq, Runnable task) {}

    private final PriorityQueue<Event> queue =
            new PriorityQueue<>(Comparator.comparingLong(Event::time).thenComparingLong(Event::seq));
    public final Random random;
    private long now;
    private long seq;
    private long steps;
    private long stepsThisInstant;
    private long budget = Long.MAX_VALUE;
    private long trace = 1;

    public Simulation(long seed) {
        this.random = new Random(seed);
    }

    public long now() {
        return now;
    }

    public long steps() {
        return steps;
    }

    /** Thrown when a run uses more events than its budget, which in practice means a message storm or livelock. */
    public static final class BudgetExceeded extends RuntimeException {
        private static final long serialVersionUID = 1L;

        BudgetExceeded(String message) {
            super(message);
        }
    }

    public void setBudget(long maxSteps) {
        budget = maxSteps;
    }

    /** Runs {@code task} after {@code delayMs} of virtual time. */
    public void after(long delayMs, Runnable task) {
        if (delayMs < 0) throw new IllegalArgumentException("negative delay");
        queue.add(new Event(now + delayMs, seq++, task));
    }

    /** Executes one event. Returns false when nothing is left to do. */
    public boolean step() {
        Event e = queue.poll();
        if (e == null) return false;
        if (e.time() == now) {
            if (++stepsThisInstant > 1_000_000)
                throw new BudgetExceeded("livelock: a million events at virtual time " + now);
        } else {
            stepsThisInstant = 0;
        }
        now = e.time();
        if (++steps > budget) throw new BudgetExceeded("more than " + budget + " events by virtual time " + now);
        e.task().run();
        return true;
    }

    /** Runs every event up to and including virtual time {@code t}, then sets the clock to {@code t}. */
    public void runUntil(long t) {
        while (!queue.isEmpty() && queue.peek().time() <= t) step();
        now = Math.max(now, t);
    }

    /** Runs until the condition holds or the virtual deadline passes. Returns whether the condition held. */
    public boolean runUntil(BooleanSupplier condition, long deadline) {
        while (!condition.getAsBoolean()) {
            if (queue.isEmpty() || queue.peek().time() > deadline) {
                now = Math.max(now, deadline);
                return condition.getAsBoolean();
            }
            step();
        }
        return true;
    }

    /** Like {@link #runUntil(BooleanSupplier, long)}, but only evaluates an expensive condition every {@code pollMs}. */
    public boolean poll(BooleanSupplier condition, long pollMs, long deadline) {
        while (!condition.getAsBoolean()) {
            if (now >= deadline) return false;
            runUntil(Math.min(deadline, now + pollMs));
        }
        return true;
    }

    /** Folds an observation into the run's fingerprint. Two runs of one seed must end with equal fingerprints. */
    public void trace(long value) {
        trace = trace * 1_000_003 + value;
    }

    public long fingerprint() {
        return trace * 31 + now;
    }
}
