package raftkv.sim;

import raftkv.core.EventLoop;

/**
 * One node's event loop inside the simulation. Tasks are events in the shared queue, tagged with this loop so a
 * crash ({@link #shutdown}) silently discards everything the node had queued or scheduled.
 *
 * <p>Each node's timers run at a slightly different rate ({@code drift}), the way real oscillators do. Raft
 * must stay safe whatever the clocks do; drift only changes who times out first.
 */
final class SimEventLoop implements EventLoop {
    private final Simulation sim;
    private final double drift;
    private final Runnable onCrash;
    private boolean alive = true;
    private boolean inLoop;

    /** {@code onCrash} runs if a task dies at a simulated crash point; it should crash the whole machine. */
    SimEventLoop(Simulation sim, double drift, Runnable onCrash) {
        this.sim = sim;
        this.drift = drift;
        this.onCrash = onCrash;
    }

    private final class Task implements Cancellable {
        private final Runnable body;
        private boolean cancelled;

        Task(Runnable body) {
            this.body = body;
        }

        void run() {
            if (!alive || cancelled) return;
            inLoop = true;
            try {
                body.run();
            } catch (SimDisk.Crash crash) {
                inLoop = false;
                onCrash.run();
            } finally {
                inLoop = false;
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    @Override
    public long nowMillis() {
        return sim.now();
    }

    @Override
    public boolean execute(Runnable task) {
        if (!alive) return false;
        Task t = new Task(task);
        sim.after(0, t::run);
        return true;
    }

    private long scaled(long delayMs) {
        return Math.max(0, Math.round(delayMs * drift));
    }

    @Override
    public Cancellable schedule(Runnable task, long delayMs) {
        Task t = new Task(task);
        if (alive) sim.after(scaled(delayMs), t::run);
        return t;
    }

    @Override
    public Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs) {
        final class Repeating implements Cancellable {
            boolean cancelled;

            void fire() {
                if (!alive || cancelled) return;
                sim.after(scaled(periodMs), this::fire);
                new Task(task).run();
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
        Repeating r = new Repeating();
        if (alive) sim.after(scaled(initialDelayMs), r::fire);
        return r;
    }

    @Override
    public void shutdown() {
        alive = false;
    }

    @Override
    public boolean inLoop() {
        return inLoop;
    }
}
