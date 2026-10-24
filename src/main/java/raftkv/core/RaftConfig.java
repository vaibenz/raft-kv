package raftkv.core;

/** Timing and batching parameters. Election timeouts must be well above the heartbeat interval. */
public record RaftConfig(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxBatch) {
    public RaftConfig {
        if (electionMinMs <= heartbeatMs * 2 || electionMaxMs <= electionMinMs || maxBatch < 1)
            throw new IllegalArgumentException("invalid Raft timing configuration");
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 512);
    }

    /** Shorter timeouts for in-process tests, so failover happens in tens of milliseconds. */
    public static RaftConfig fast() {
        return new RaftConfig(60, 120, 15, 256);
    }
}
