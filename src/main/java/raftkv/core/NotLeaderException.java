package raftkv.core;

/** Thrown when a request reaches a node that is not (or is no longer) the leader. */
public final class NotLeaderException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super(leaderHint == null ? "not the leader; leader unknown" : "not the leader; try " + leaderHint);
        this.leaderHint = leaderHint;
    }

    /** The node this node believes is leader, or null if it does not know. */
    public String leaderHint() {
        return leaderHint;
    }
}
