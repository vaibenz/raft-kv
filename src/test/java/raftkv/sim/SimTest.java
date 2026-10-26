package raftkv.sim;

import raftkv.Check;
import raftkv.Test;
import raftkv.core.Mutation;

/**
 * The deterministic simulator as part of the normal test suite. The long sweeps live in {@code make sim}; these
 * tests keep the essentials fast: the simulator is deterministic, a batch of seeds passes, and every planted
 * protocol bug is caught.
 */
public class SimTest {
    private static final long DURATION = 10_000;

    static long firstFailingSeed(Mutation m, long from, long maxSeeds) {
        Mutation.set(m);
        java.io.PrintStream err = System.err;
        System.setErr(new java.io.PrintStream(java.io.OutputStream.nullOutputStream()));
        try {
            for (long seed = from; seed < from + maxSeeds; seed++)
                if (!new SimRun(seed, DURATION).run().ok()) return seed;
            return -1;
        } finally {
            Mutation.set(Mutation.NONE);
            System.setErr(err);
        }
    }

    @Test
    public void sameSeedReplaysIdentically() {
        for (long seed : new long[] {7, 1234, 99_999}) {
            SimRun.Result a = new SimRun(seed, DURATION).run();
            SimRun.Result b = new SimRun(seed, DURATION).run();
            Check.equal(a.fingerprint(), b.fingerprint(), "fingerprint of seed " + seed);
            Check.equal(a.stats().toString(), b.stats().toString(), "stats of seed " + seed);
            Check.equal(a.failure(), b.failure(), "outcome of seed " + seed);
        }
    }

    @Test
    public void manySeedsAreLinearizable() {
        int seeds = Integer.getInteger("sim.testSeeds", 100);
        for (long seed = 1; seed <= seeds; seed++) {
            SimRun.Result r = new SimRun(seed, DURATION).run();
            Check.isTrue(r.ok(), "seed " + seed + " failed (replay with make sim SEED=" + seed + "): " + r.failure());
        }
    }

    private static void caught(Mutation m, int maxSeeds) {
        long seed = firstFailingSeed(m, 1, maxSeeds);
        Check.isTrue(seed > 0, m + " was not caught within " + maxSeeds + " seeds");
        System.out.printf("    %-24s caught at seed %d%n", m, seed);
    }

    @Test
    public void catchesReadsWithoutQuorum() {
        caught(Mutation.READ_WITHOUT_QUORUM, 100);
    }

    @Test
    public void catchesAcksBeforeSync() {
        caught(Mutation.ACK_BEFORE_SYNC, 100);
    }

    @Test
    public void catchesMissingElectionRestriction() {
        caught(Mutation.NO_ELECTION_RESTRICTION, 100);
    }

    @Test
    public void catchesRenamesWithoutDirectoryFsync() {
        caught(Mutation.SKIP_DIR_FSYNC, 100);
    }

    @Test
    public void catchesTablesCommittedBeforeTheyAreDurable() {
        caught(Mutation.SKIP_SST_FSYNC, 100);
    }

    // COMMIT_OLD_TERM (Figure 8) is also caught, but rarely: the leader's no-op closes most of the window, so it
    // takes a few thousand seeds. It runs in `make mutations` rather than here.
}
