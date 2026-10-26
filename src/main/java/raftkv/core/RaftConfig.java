package raftkv.core;

/**
 * Timing, batching and compaction parameters. Election timeouts must be well above the heartbeat interval.
 * A snapshot is taken once {@code snapshotThreshold} entries have been applied since the last one.
 */
public record RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch, int snapshotThreshold) {
    public RaftConfig {
        if (electionMinMs <= heartbeatMs * 2 || electionMaxMs <= electionMinMs || maxBatch < 1 || snapshotThreshold < 1)
            throw new IllegalArgumentException("invalid Raft timing configuration");
    }

    public RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch) {
        this(electionMinMs, electionMaxMs, heartbeatMs, maxBatch, 100_000);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 512);
    }

    /** Shorter timeouts for in-process tests, so failover happens in tens of milliseconds. */
    public static RaftConfig fast() {
        return new RaftConfig(60, 120, 15, 256);
    }
}
