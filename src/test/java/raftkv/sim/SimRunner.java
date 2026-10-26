package raftkv.sim;

import raftkv.core.Mutation;

/**
 * Runs many simulation seeds and reports any that fail, each with the command that replays it exactly.
 *
 * <pre>
 * make sim SEEDS=1000              # seeds 1..1000
 * make sim SEED=4711               # replay one seed and print its full history on failure
 * make sim SEEDS=500 MUTATION=read-without-quorum   # check that the simulator catches a planted bug
 * </pre>
 */
public final class SimRunner {
    public static void main(String[] args) {
        long single = Long.getLong("seed", -1);
        long first = Long.getLong("sim.start", 1);
        long count = Long.getLong("sim.seeds", 200);
        long duration = Long.getLong("sim.duration", 10_000);
        String mutation = System.getProperty("raftkv.mutation", "");
        if (single >= 0) {
            first = single;
            count = 1;
        }
        System.out.printf("simulating %d seed(s) from %d, %.0f s of virtual time each%s%n", count, first,
                duration / 1000.0, mutation.isEmpty() ? "" : ", mutation " + mutation);

        // Nodes log unexpected exceptions to stderr; the run reports them as failures anyway, so keep the
        // output readable when sweeping many seeds.
        if (count > 1) System.setErr(new java.io.PrintStream(java.io.OutputStream.nullOutputStream()));
        long start = System.currentTimeMillis();
        long failures = 0, faults = 0, crashes = 0, leaders = 0, ops = 0, unknown = 0, messages = 0, virtualMs = 0;
        long firstFailure = -1;
        for (long seed = first; seed < first + count; seed++) {
            SimRun.Result r = new SimRun(seed, duration).run();
            SimRun.Stats s = r.stats();
            faults += s.faults;
            crashes += s.crashes;
            leaders += s.leaders;
            ops += s.opsOk + s.opsUnknown;
            unknown += s.opsUnknown;
            messages += s.messagesSent;
            virtualMs += s.virtualMs;
            if (count == 1) System.out.println("  " + s);
            if (!r.ok()) {
                failures++;
                if (firstFailure < 0) firstFailure = seed;
                String text = r.failure();
                if (count > 1 && text.length() > 400) text = text.substring(0, 400) + "...";
                System.out.printf("  FAIL seed %d: %s%n        replay with: make sim SEED=%d%s%n", seed, text, seed,
                        mutation.isEmpty() ? "" : " MUTATION=" + mutation);
            } else if (count > 1 && (seed - first + 1) % 100 == 0) {
                System.out.printf("  %d seeds done, %d failed%n", seed - first + 1, failures);
            }
        }
        double secs = (System.currentTimeMillis() - start) / 1000.0;
        System.out.printf("%n%d seeds, %d failed | %d faults injected (%d crashes), %d leaders elected, "
                        + "%d operations checked for linearizability (%d with unknown outcome), %d messages "
                        + "| %.0f min of virtual time in %.1f s%n",
                count, failures, faults, crashes, leaders, ops, unknown, messages, virtualMs / 60_000.0, secs);
        if (Mutation.is(Mutation.NONE)) System.exit(failures == 0 ? 0 : 1);
        System.out.println(failures > 0 ? "mutation caught, first at seed " + firstFailure : "mutation NOT caught");
        System.exit(failures > 0 ? 0 : 1);
    }
}
