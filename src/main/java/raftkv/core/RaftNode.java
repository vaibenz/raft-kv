package raftkv.core;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import raftkv.core.Message.AppendEntries;
import raftkv.core.Message.AppendReply;
import raftkv.core.Message.RequestVote;
import raftkv.core.Message.VoteReply;
import raftkv.storage.Storage;
import raftkv.transport.Transport;
import raftkv.transport.TransportFactory;

/**
 * One member of a Raft cluster (Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm").
 *
 * <p><b>Threading model.</b> Every piece of Raft state is owned by a single event loop thread. Messages,
 * timers and client calls are all turned into tasks on that thread, so the protocol logic is written as
 * plain sequential code with no locks, and there is no possibility of two handlers interleaving.
 *
 * <p>Implemented: leader election with randomized timeouts and PreVote, log replication with conflict backtracking and
 * pipelining, the commit rule for current term entries, a leader no-op on election, group commit (one fsync
 * per batch of client writes), and linearizable reads via ReadIndex.
 */
public final class RaftNode {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** A thread safe snapshot of the node's state, for monitoring and tests. */
    public record Status(String id, Role role, long term, String leaderId, long commitIndex, long lastApplied,
                         long lastLogIndex) {}

    private record PendingWrite(long term, CompletableFuture<byte[]> future) {}

    private static final class PendingRead {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        long readIndex = -1; // -1 until this leader has committed an entry in its own term
        long round;
    }

    /** Set -Draftkv.groupCommit=false to fsync and replicate every write on its own, for comparison. */
    private static final boolean GROUP_COMMIT = !"false".equals(System.getProperty("raftkv.groupCommit"));

    private final String id;
    private final List<String> peers;
    private final Storage storage;
    private final StateMachine stateMachine;
    private final RaftConfig config;
    private final Listener listener;
    private final TransportFactory transportFactory;
    private final ScheduledExecutorService loop;
    private final Random random;

    private Transport transport;
    private volatile boolean running;
    private volatile Status status;

    // Volatile Raft state (owned by the loop thread)
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;

    // Candidate state
    private final Set<String> votes = new HashSet<>();
    private final Set<String> preVotes = new HashSet<>();
    private boolean preVoting;
    private long lastHeardFromLeader = Long.MIN_VALUE / 2; // monotonic ms

    // Leader state
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, Long> ackedRound = new HashMap<>();
    private long round;       // heartbeat round, for ReadIndex
    private long syncedIndex; // highest index durable on this leader's own disk
    private boolean flushScheduled;
    private final TreeMap<Long, PendingWrite> pendingWrites = new TreeMap<>();
    private final List<PendingRead> pendingReads = new ArrayList<>();

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    public RaftNode(String id, List<String> peers, Storage storage, StateMachine stateMachine, RaftConfig config,
                    Listener listener, TransportFactory transportFactory) {
        this.id = id;
        this.peers = List.copyOf(peers);
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.config = config;
        this.listener = listener;
        this.transportFactory = transportFactory;
        this.random = new Random(id.hashCode() ^ System.nanoTime());
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "raft-" + id);
            t.setDaemon(true);
            return t;
        });
        exec.setRemoveOnCancelPolicy(true);
        this.loop = exec;
        publish();
    }

    // ------------------------------------------------------------------ lifecycle

    public void start() {
        running = true;
        transport = transportFactory.connect(id, this::receive);
        run(this::resetElectionTimer);
    }

    /** Stops the node abruptly, as a crash would: nothing in flight is finished. */
    public void stop() {
        running = false;
        loop.shutdownNow();
        try {
            loop.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (transport != null) transport.close();
        failPending(new NotLeaderException(null));
    }

    public String id() {
        return id;
    }

    public Status status() {
        return status;
    }

    // ------------------------------------------------------------------ client API (any thread)

    /** Appends a command to the replicated log. Completes with the state machine's result once committed. */
    public CompletableFuture<byte[]> propose(byte[] command) {
        if (command.length == 0) throw new IllegalArgumentException("empty commands are reserved for no-ops");
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        submit(f, () -> {
            if (role != Role.LEADER) {
                f.completeExceptionally(new NotLeaderException(leaderId));
                return;
            }
            appendLocal(new LogEntry(storage.term(), command));
            pendingWrites.put(storage.lastIndex(), new PendingWrite(storage.term(), f));
        });
        return f;
    }

    /**
     * ReadIndex: completes when it is safe to read the state machine linearizably. The leader records its
     * commit index, confirms with a majority that it is still the leader, and waits until that index has been
     * applied. This avoids writing reads to the log while never returning stale data from a deposed leader.
     */
    public CompletableFuture<Void> readBarrier() {
        PendingRead read = new PendingRead();
        submit(read.future, () -> {
            if (role != Role.LEADER) {
                read.future.completeExceptionally(new NotLeaderException(leaderId));
                return;
            }
            pendingReads.add(read);
            serviceReads();
        });
        return read.future;
    }

    // ------------------------------------------------------------------ event loop plumbing

    /** Entry point for the transport. Runs on a network thread, so it just enqueues. */
    public void receive(Message m) {
        if (running) run(() -> handle(m));
    }

    private void run(Runnable task) {
        try {
            loop.execute(() -> safely(task));
        } catch (RejectedExecutionException stopped) {
            // node is stopped: drop, as a crashed process would
        }
    }

    private void submit(CompletableFuture<?> f, Runnable task) {
        if (!running) {
            f.completeExceptionally(new NotLeaderException(null));
            return;
        }
        try {
            loop.execute(() -> safely(task));
        } catch (RejectedExecutionException stopped) {
            f.completeExceptionally(new NotLeaderException(null));
        }
    }

    private void safely(Runnable task) {
        if (!running) return;
        try {
            task.run();
        } catch (UncheckedIOException e) {
            System.err.println(id + ": storage failure, stopping: " + e);
            new Thread(this::stop).start(); // fail stop: a node that cannot persist must not vote or ack
        } catch (RuntimeException e) {
            System.err.println(id + ": unexpected error: " + e);
            e.printStackTrace();
        }
        publish();
    }

    private void publish() {
        status = new Status(id, role, storage.term(), leaderId, commitIndex, lastApplied, storage.lastIndex());
    }

    private void send(String to, Message m) {
        transport.send(to, m);
    }

    private static long now() {
        return System.nanoTime() / 1_000_000;
    }

    private int quorum() {
        return (peers.size() + 1) / 2 + 1;
    }

    // ------------------------------------------------------------------ message handling

    private void handle(Message m) {
        // Raft rule: a higher term always wins. A PreVote probe carries a hypothetical term, so it is exempt.
        boolean probe = m instanceof RequestVote rv && rv.preVote();
        if (!probe && m.term() > storage.term()) stepDown(m.term(), null);
        switch (m) {
            case RequestVote rv -> onRequestVote(rv);
            case VoteReply vr -> onVoteReply(vr);
            case AppendEntries ae -> onAppendEntries(ae);
            case AppendReply ar -> onAppendReply(ar);
        }
    }

    private void stepDown(long newTerm, String newLeader) {
        if (newTerm > storage.term()) storage.setTermAndVote(newTerm, null);
        boolean wasLeader = role == Role.LEADER;
        role = Role.FOLLOWER;
        leaderId = newLeader;
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
            heartbeatTimer = null;
        }
        // In flight writes may still commit under the next leader, so clients must retry with the same
        // request id; the state machine deduplicates them.
        if (wasLeader) failPending(new NotLeaderException(null));
        resetElectionTimer();
    }

    private void failPending(RuntimeException e) {
        pendingWrites.values().forEach(p -> p.future.completeExceptionally(e));
        pendingWrites.clear();
        pendingReads.forEach(r -> r.future.completeExceptionally(e));
        pendingReads.clear();
    }

    // ------------------------------------------------------------------ elections

    private void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        int timeout = config.electionMinMs() + random.nextInt(config.electionMaxMs() - config.electionMinMs());
        electionTimer = loop.schedule(() -> safely(this::startPreVote), timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * PreVote: before disrupting the cluster with a new term, ask whether a majority would vote for us. Peers
     * that heard from a live leader recently say no. Without this, a node that was partitioned away or just
     * restarted would bump the term on rejoining and force the healthy leader to step down.
     */
    private void startPreVote() {
        if (role == Role.LEADER) return;
        preVoting = true;
        preVotes.clear();
        preVotes.add(id);
        resetElectionTimer();
        if (preVotes.size() >= quorum()) {
            startElection();
            return;
        }
        RequestVote probe = new RequestVote(storage.term() + 1, id, storage.lastIndex(), storage.lastTerm(), true);
        for (String p : peers) send(p, probe);
    }

    private void startElection() {
        if (role == Role.LEADER) return;
        preVoting = false;
        long term = storage.term() + 1;
        storage.setTermAndVote(term, id); // durable before asking anyone else
        role = Role.CANDIDATE;
        leaderId = null;
        votes.clear();
        votes.add(id);
        resetElectionTimer(); // a split vote times out and retries with a new random timeout
        if (votes.size() >= quorum()) {
            becomeLeader();
            return;
        }
        RequestVote rv = new RequestVote(term, id, storage.lastIndex(), storage.lastTerm(), false);
        for (String p : peers) send(p, rv);
    }

    private void onRequestVote(RequestVote rv) {
        // Election restriction (section 5.4.1): only vote for a candidate whose log is at least as up to
        // date as ours, so any elected leader already holds every committed entry.
        boolean upToDate = rv.lastLogTerm() > storage.lastTerm()
                || (rv.lastLogTerm() == storage.lastTerm() && rv.lastLogIndex() >= storage.lastIndex());
        if (rv.preVote()) {
            boolean leaderAlive = role == Role.LEADER || now() - lastHeardFromLeader < config.electionMinMs();
            boolean grant = rv.term() > storage.term() && upToDate && !leaderAlive;
            send(rv.from(), new VoteReply(storage.term(), id, grant, true));
            return;
        }
        boolean granted = false;
        if (rv.term() == storage.term()) {
            String voted = storage.votedFor();
            if ((voted == null || voted.equals(rv.from())) && upToDate) {
                if (voted == null) storage.setTermAndVote(rv.term(), rv.from());
                granted = true;
                resetElectionTimer();
            }
        }
        send(rv.from(), new VoteReply(storage.term(), id, granted, false));
    }

    private void onVoteReply(VoteReply vr) {
        if (vr.preVote()) {
            if (preVoting && role != Role.LEADER && vr.granted()) {
                preVotes.add(vr.from());
                if (preVotes.size() >= quorum()) startElection();
            }
            return;
        }
        if (role != Role.CANDIDATE || vr.term() != storage.term() || !vr.granted()) return;
        votes.add(vr.from());
        if (votes.size() >= quorum()) becomeLeader();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        preVoting = false;
        if (electionTimer != null) electionTimer.cancel(false);
        storage.sync();
        syncedIndex = storage.lastIndex();
        long next = storage.lastIndex() + 1;
        for (String p : peers) {
            nextIndex.put(p, next);
            matchIndex.put(p, 0L);
            ackedRound.put(p, 0L);
        }
        listener.onLeader(id, storage.term());
        // A leader may only count replicas for entries of its own term (section 5.4.2). Appending a no-op
        // immediately lets earlier entries commit and gives ReadIndex a current term commit to anchor on.
        appendLocal(new LogEntry(storage.term(), new byte[0]));
        heartbeatTimer = loop.scheduleAtFixedRate(() -> safely(this::heartbeat), config.heartbeatMs(),
                config.heartbeatMs(), TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------ replication (leader side)

    private void appendLocal(LogEntry entry) {
        storage.append(List.of(entry));
        if (!GROUP_COMMIT) {
            flush();
            return;
        }
        if (!flushScheduled) { // group commit: every write queued before the flush shares one fsync
            flushScheduled = true;
            run(this::flush);
        }
    }

    private void flush() {
        flushScheduled = false;
        if (role != Role.LEADER) return;
        storage.sync();
        syncedIndex = storage.lastIndex();
        for (String p : peers) sendAppend(p);
        advanceCommitIndex();
    }

    private void heartbeat() {
        if (role != Role.LEADER) return;
        round++;
        for (String p : peers) sendAppend(p);
    }

    private void sendAppend(String peer) {
        long next = nextIndex.get(peer);
        long prev = next - 1;
        long last = Math.min(syncedIndex, prev + config.maxBatch()); // only ship entries that are durable here
        List<LogEntry> entries = last >= next ? storage.slice(next, last + 1) : List.of();
        send(peer, new AppendEntries(storage.term(), id, prev, storage.termAt(prev), entries, commitIndex, round));
        // Pipelining: assume success and keep streaming. A lost or rejected batch is corrected by the reply.
        if (!entries.isEmpty()) nextIndex.put(peer, last + 1);
    }

    private void onAppendReply(AppendReply r) {
        if (role != Role.LEADER || r.term() != storage.term()) return;
        ackedRound.merge(r.from(), r.round(), Math::max);
        if (r.success()) {
            long match = matchIndex.merge(r.from(), r.matchIndex(), Math::max);
            if (nextIndex.get(r.from()) <= match) nextIndex.put(r.from(), match + 1);
            advanceCommitIndex();
            if (nextIndex.get(r.from()) <= syncedIndex) sendAppend(r.from()); // still behind: keep going
        } else {
            long next = r.conflictIndex();
            if (r.conflictTerm() > 0) {
                long lastOfTerm = lastIndexOfTerm(r.conflictTerm());
                if (lastOfTerm > 0) next = lastOfTerm + 1;
            }
            next = Math.max(next, matchIndex.get(r.from()) + 1);
            next = Math.max(1, Math.min(next, storage.lastIndex() + 1));
            nextIndex.put(r.from(), next);
            sendAppend(r.from());
        }
        serviceReads();
    }

    private long lastIndexOfTerm(long term) {
        long i = storage.lastIndex();
        while (i > 0 && storage.termAt(i) > term) i--;
        return i > 0 && storage.termAt(i) == term ? i : 0;
    }

    private void advanceCommitIndex() {
        long term = storage.term();
        for (long n = syncedIndex; n > commitIndex; n--) {
            long t = storage.termAt(n);
            if (t < term) break; // never commit an older term's entry by counting replicas
            int replicas = 1;
            for (String p : peers) if (matchIndex.get(p) >= n) replicas++;
            if (replicas >= quorum()) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
    }

    // ------------------------------------------------------------------ replication (follower side)

    private void onAppendEntries(AppendEntries ae) {
        long term = storage.term();
        if (ae.term() < term) {
            send(ae.from(), new AppendReply(term, id, false, 0, 0, 0, ae.round()));
            return;
        }
        if (role != Role.FOLLOWER) stepDown(term, ae.from());
        leaderId = ae.from();
        preVoting = false;
        lastHeardFromLeader = now();
        resetElectionTimer();

        long prev = ae.prevLogIndex();
        if (prev > storage.lastIndex()) {
            send(ae.from(), new AppendReply(term, id, false, 0, storage.lastIndex() + 1, 0, ae.round()));
            return;
        }
        if (storage.termAt(prev) != ae.prevLogTerm()) {
            long conflictTerm = storage.termAt(prev);
            long first = prev;
            while (first > 1 && storage.termAt(first - 1) == conflictTerm) first--;
            send(ae.from(), new AppendReply(term, id, false, 0, first, conflictTerm, ae.round()));
            return;
        }

        // Skip entries we already have; truncate only at a real conflict. Never truncate on a merely shorter
        // (possibly stale, reordered) message, or we could delete entries that were already acknowledged.
        List<LogEntry> entries = ae.entries();
        int i = 0;
        for (; i < entries.size(); i++) {
            long idx = prev + 1 + i;
            if (idx > storage.lastIndex()) break;
            if (storage.termAt(idx) != entries.get(i).term()) {
                storage.truncateFrom(idx);
                break;
            }
        }
        if (i < entries.size()) storage.append(entries.subList(i, entries.size()));
        storage.sync(); // durable before we acknowledge

        long lastNew = prev + entries.size();
        long newCommit = Math.min(ae.leaderCommit(), lastNew); // only entries known to match the leader's log
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        send(ae.from(), new AppendReply(term, id, true, lastNew, 0, 0, ae.round()));
    }

    // ------------------------------------------------------------------ applying and reads

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = storage.get(lastApplied);
            byte[] result = e.isNoop() ? new byte[0] : stateMachine.apply(lastApplied, e.command());
            listener.onApply(id, lastApplied, e.term(), e.command());
            PendingWrite p = pendingWrites.remove(lastApplied);
            if (p != null) {
                if (p.term() == e.term()) p.future().complete(result);
                else p.future().completeExceptionally(new NotLeaderException(leaderId));
            }
        }
        serviceReads();
    }

    private void serviceReads() {
        if (role != Role.LEADER || pendingReads.isEmpty()) return;
        boolean committedInTerm = storage.termAt(commitIndex) == storage.term();
        boolean needRound = false;
        Iterator<PendingRead> it = pendingReads.iterator();
        while (it.hasNext()) {
            PendingRead r = it.next();
            if (r.readIndex < 0) {
                if (!committedInTerm) continue;
                r.readIndex = commitIndex;
                if (!needRound) round++;
                needRound = true;
                r.round = round; // reads arriving together share one confirmation round
                continue;
            }
            if (lastApplied >= r.readIndex && roundConfirmed(r.round)) {
                r.future.complete(null);
                it.remove();
            }
        }
        if (needRound) {
            for (String p : peers) sendAppend(p);
            if (peers.isEmpty()) serviceReads();
        }
    }

    private boolean roundConfirmed(long r) {
        int acks = 1;
        for (String p : peers) if (ackedRound.get(p) >= r) acks++;
        return acks >= quorum();
    }
}
