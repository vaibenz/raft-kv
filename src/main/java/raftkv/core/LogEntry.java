package raftkv.core;

/** One entry in the replicated log. Its index is its position in the log (1 based). An empty command is a no-op. */
public record LogEntry(long term, byte[] command) {
    public boolean isNoop() {
        return command.length == 0;
    }
}
