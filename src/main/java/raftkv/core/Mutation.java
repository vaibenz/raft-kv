package raftkv.core;

/**
 * Deliberate protocol bugs, used only to prove the test suite can catch them (mutation testing).
 *
 * <p>A checker that never fails proves nothing. Each mutation removes one rule that Raft depends on. The
 * simulator is run with each mutation switched on and must find a counterexample within a bounded number of
 * seeds; see {@code SimTest}. Selected with {@code -Draftkv.mutation=NAME}; off by default.
 */
public enum Mutation {
    NONE,
    /** Vote for any candidate, ignoring whether its log is up to date (section 5.4.1). */
    NO_ELECTION_RESTRICTION,
    /** Commit entries from earlier terms by counting replicas (the Figure 8 bug, section 5.4.2). */
    COMMIT_OLD_TERM,
    /** Serve ReadIndex reads without confirming leadership with a majority. */
    READ_WITHOUT_QUORUM,
    /** Followers acknowledge entries before they are durable. */
    ACK_BEFORE_SYNC,
    /** Atomic file replacement skips the directory fsync, so a rename (and the term and vote in it) can be lost. */
    SKIP_DIR_FSYNC,
    /** The LSM commits a table to its manifest without forcing the table's own data to disk first. */
    SKIP_SST_FSYNC;

    private static Mutation active = parse(System.getProperty("raftkv.mutation"));

    private static Mutation parse(String s) {
        return s == null || s.isEmpty() ? NONE : valueOf(s.toUpperCase().replace('-', '_'));
    }

    public static boolean is(Mutation m) {
        return active == m;
    }

    /** For tests that switch mutations between runs in one JVM. */
    public static void set(Mutation m) {
        active = m;
    }
}
