package raftkv.core;

import java.util.Random;

/**
 * Fault injection at the code level, after FoundationDB's BUGGIFY.
 *
 * <p>Network and disk faults only reach the states a real failure can produce. Many bugs hide behind
 * behaviour that is perfectly legal but rare in practice: a batch of one entry, a flush that runs late, a
 * heartbeat that never goes out, a state machine that applies half of what was committed. Each call site
 * below names such a behaviour. In production {@link #fire} is always false and costs one field read. In the
 * simulator every site is switched on for a random subset of runs, and then fires with the given probability,
 * so the protocol is exercised along paths a test author would never think to write.
 *
 * <p>Only one simulation runs at a time and it is single threaded, so plain static state is enough.
 */
public final class Buggify {
    private static Random random;   // null when disabled
    private static long enabledSites; // bit per site, chosen once per run

    private Buggify() {}

    public enum Site { TINY_APPEND_BATCH, LATE_FLUSH, SKIPPED_HEARTBEAT, PARTIAL_APPLY, SLOW_READ_ROUND }

    /** Turns buggify on for one simulation run. Each site is enabled with probability one half. */
    public static void enable(Random r) {
        random = r;
        enabledSites = 0;
        for (Site s : Site.values()) if (r.nextBoolean()) enabledSites |= 1L << s.ordinal();
    }

    public static void disable() {
        random = null;
    }

    public static boolean fire(Site site, double probability) {
        Random r = random;
        return r != null && (enabledSites & (1L << site.ordinal())) != 0 && r.nextDouble() < probability;
    }
}
