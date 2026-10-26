package raftkv.core;

/**
 * Where a node's tasks run and where its sense of time comes from.
 *
 * <p>{@link RaftNode} never touches threads, {@code System.nanoTime} or a scheduler directly. In production this
 * is a single thread with a real clock ({@link ThreadEventLoop}). In the deterministic simulator it is a virtual
 * clock driven by one seeded event queue shared by the whole cluster, so an entire multi node run, including
 * every timeout, message and crash, is a pure function of its seed and can be replayed exactly.
 */
public interface EventLoop {
    /** A scheduled task that can be cancelled before it runs. */
    interface Cancellable {
        void cancel();
    }

    /** Milliseconds on a monotonic clock. Only differences are meaningful. */
    long nowMillis();

    /** Runs the task on the loop, after every task already queued. Returns false if the loop is shut down. */
    boolean execute(Runnable task);

    Cancellable schedule(Runnable task, long delayMs);

    Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs);

    /** Stops the loop. Queued and scheduled tasks are discarded, as they would be if the process died. */
    void shutdown();

    /** True when called from the loop itself. */
    boolean inLoop();
}
