package raftkv;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/** Assertions and polling helpers for tests. */
public final class Check {
    private Check() {}

    public static void isTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void equal(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual))
            throw new AssertionError(message + ": expected <" + expected + "> but was <" + actual + ">");
    }

    /** Polls until the condition holds, failing after {@code timeoutMs}. */
    public static void eventually(BooleanSupplier condition, long timeoutMs, String message) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            sleep(10);
        }
        if (!condition.getAsBoolean()) throw new AssertionError("timed out after " + timeoutMs + " ms: " + message);
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }
}
