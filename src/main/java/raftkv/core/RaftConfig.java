package raftkv.core;

/**
 * Timing, batching and compaction parameters. Election timeouts must be well above the heartbeat interval.
 * A snapshot is taken once {@code snapshotThreshold} entries have been applied since the last one.
 * An AppendEntries carries at most {@code maxBatch} entries and, past its first entry, at most
 * {@code maxBatchBytes} of commands, so a batch of large values stays far below the transport's frame cap.
 */
public record RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch, int snapshotThreshold,
                         int maxBatchBytes) {
    public static final int DEFAULT_MAX_BATCH_BYTES = 4 << 20;

    public RaftConfig {
        // The heartbeat must be a real interval: 0 or less used to pass (any election timeout is above twice it) and
        // then failed inside the node's timers. The comparison is in long, since heartbeatMs * 2 wrapped negative for
        // a heartbeat over 2^30 ms and let a 100 ms election timeout through, and the first heartbeat is staggered
        // over three intervals, which must fit in an int.
        if (heartbeatMs < 1 || heartbeatMs > Integer.MAX_VALUE / 3 || electionMinMs <= 2L * heartbeatMs
                || electionMaxMs <= electionMinMs || maxBatch < 1 || snapshotThreshold < 1 || maxBatchBytes < 1)
            throw new IllegalArgumentException("invalid Raft timing configuration");
    }

    public RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch, int snapshotThreshold) {
        this(electionMinMs, electionMaxMs, heartbeatMs, maxBatch, snapshotThreshold, DEFAULT_MAX_BATCH_BYTES);
    }

    public RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch) {
        this(electionMinMs, electionMaxMs, heartbeatMs, maxBatch, 100_000);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 512, 10_000);
    }

    /** Shorter timeouts for in-process tests, so failover happens in tens of milliseconds. */
    public static RaftConfig fast() {
        return new RaftConfig(60, 120, 15, 256);
    }
}
