package raftkv.core;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** The production event loop: one daemon thread and the real monotonic clock. */
public final class ThreadEventLoop implements EventLoop {
    private final ScheduledThreadPoolExecutor exec;
    private volatile Thread thread;

    public ThreadEventLoop(String name) {
        exec = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            thread = t;
            return t;
        });
        exec.setRemoveOnCancelPolicy(true);
    }

    @Override
    public long nowMillis() {
        return System.nanoTime() / 1_000_000;
    }

    @Override
    public boolean execute(Runnable task) {
        try {
            exec.execute(task);
            return true;
        } catch (RejectedExecutionException stopped) {
            return false; // the loop is shut down: drop the task, as a dead process would
        }
    }

    @Override
    public Cancellable schedule(Runnable task, long delayMs) {
        try {
            ScheduledFuture<?> f = exec.schedule(task, delayMs, TimeUnit.MILLISECONDS);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException stopped) {
            return () -> {};
        }
    }

    @Override
    public Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs) {
        try {
            ScheduledFuture<?> f = exec.scheduleAtFixedRate(task, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException stopped) {
            return () -> {};
        }
    }

    @Override
    public void shutdown() {
        exec.shutdownNow();
        if (!inLoop()) {
            try {
                exec.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean inLoop() {
        return Thread.currentThread() == thread;
    }
}
