package raftkv.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import raftkv.Check;
import raftkv.Test;
import raftkv.core.LogEntry;
import raftkv.storage.FileStorage;

/**
 * Model based crash testing of {@link FileStorage} on the simulated disk.
 *
 * <p>Each seed runs a random sequence of storage operations (appends, syncs, truncations, term and vote changes,
 * local compactions and snapshot installs) and crashes the machine at random moments, including in the middle of
 * an I/O call. After every crash the storage is reopened and checked against what it promised before it
 * crashed:
 * <ul>
 *   <li>the term and vote are exactly the last ones saved;</li>
 *   <li>the snapshot is the last one saved;</li>
 *   <li>every entry made durable by a {@code sync} (and not truncated since) is still there, unchanged.</li>
 * </ul>
 */
public class StorageCrashTest {
    /** What the storage has promised. Indices are absolute. */
    private static final class Model {
        long term;
        String vote;
        long snapIndex, snapTerm;
        byte[] snapData = new byte[0];
        final List<LogEntry> log = new ArrayList<>(); // what the storage holds right now, after the snapshot
        long durableUpTo; // entries up to here (and after the snapshot) must survive a crash

        long lastIndex() {
            return snapIndex + log.size();
        }

        LogEntry at(long index) {
            return log.get((int) (index - snapIndex - 1));
        }
    }

    private static void verify(FileStorage s, Model m, String where) {
        Check.equal(m.term, s.term(), where + ": term");
        Check.equal(m.vote, s.votedFor(), where + ": vote");
        Check.equal(m.snapIndex, s.snapshotIndex(), where + ": snapshot index");
        Check.equal(m.snapTerm, s.snapshotTerm(), where + ": snapshot term");
        Check.isTrue(Arrays.equals(m.snapData, s.snapshotData()), where + ": snapshot data");
        Check.isTrue(s.lastIndex() >= m.durableUpTo,
                where + ": synced entries lost (have " + s.lastIndex() + ", synced " + m.durableUpTo + ")");
        for (long i = m.snapIndex + 1; i <= m.durableUpTo; i++) {
            LogEntry want = m.at(i), got = s.get(i);
            Check.isTrue(want.term() == got.term() && Arrays.equals(want.command(), got.command()),
                    where + ": entry " + i + " changed");
        }
    }

    /** After a crash, the log beyond the durable point may hold anything the disk happened to keep; adopt it. */
    private static void adopt(FileStorage s, Model m) {
        m.log.clear();
        for (long i = s.snapshotIndex() + 1; i <= s.lastIndex(); i++) m.log.add(s.get(i));
        m.durableUpTo = s.lastIndex();
    }

    private static FileStorage reopen(SimDisk disk, Model m, String where) {
        double rate = disk.crashPointRate;
        disk.crashPointRate = 0;
        try {
            FileStorage s = new FileStorage(disk.root());
            verify(s, m, where);
            adopt(s, m);
            return s;
        } catch (IOException e) {
            throw new AssertionError(where + ": storage did not reopen: " + e, e);
        } finally {
            disk.crashPointRate = rate;
        }
    }

    /** The atomic change an interrupted operation was making: after a crash it either landed whole or not at all. */
    private record Attempt(boolean termChange, long term, String vote, long snapAt, long snapTerm, byte[] snapData,
                           boolean keepSuffix) {
        static final Attempt NONE = new Attempt(false, 0, null, -1, 0, null, false);
    }

    private static void runSeed(long seed) throws IOException {
        Random r = new Random(seed);
        SimDisk disk = new SimDisk(new Random(seed * 31 + 7));
        disk.crashPointRate = r.nextDouble() < 0.5 ? 0.02 : 0;
        Model m = new Model();
        FileStorage s = new FileStorage(disk.root());
        int nextCmd = 0;
        for (int step = 0; step < 300; step++) {
            String where = "seed " + seed + " step " + step;
            Attempt attempt = Attempt.NONE;
            try {
                int op = r.nextInt(20);
                if (op < 7) { // append a few entries with non-decreasing terms
                    List<LogEntry> batch = new ArrayList<>();
                    long t = Math.max(m.term, m.log.isEmpty() ? m.snapTerm : m.log.get(m.log.size() - 1).term());
                    for (int i = 0, n = 1 + r.nextInt(5); i < n; i++)
                        batch.add(new LogEntry(t, ("cmd" + nextCmd++).getBytes()));
                    s.append(batch);
                    m.log.addAll(batch);
                } else if (op < 11) {
                    s.sync();
                    m.durableUpTo = m.lastIndex();
                } else if (op < 13 && m.lastIndex() > m.snapIndex) { // truncate a suffix
                    long from = m.snapIndex + 1 + r.nextInt((int) (m.lastIndex() - m.snapIndex));
                    s.truncateFrom(from);
                    m.log.subList((int) (from - m.snapIndex - 1), m.log.size()).clear();
                    m.durableUpTo = Math.min(m.durableUpTo, from - 1);
                } else if (op < 15) {
                    long t = m.term + r.nextInt(2);
                    String v = r.nextBoolean() ? null : "n" + r.nextInt(5);
                    attempt = new Attempt(true, t, v, -1, 0, null, false);
                    s.setTermAndVote(t, v);
                    m.term = t;
                    m.vote = v;
                } else if (op < 18 && m.lastIndex() > m.snapIndex) { // local compaction
                    long at = m.snapIndex + 1 + r.nextInt((int) (m.lastIndex() - m.snapIndex));
                    long t = m.at(at).term();
                    byte[] data = ("state@" + at + "#" + step).getBytes();
                    attempt = new Attempt(false, 0, null, at, t, data, true);
                    s.compact(at, t, data);
                    m.log.subList(0, (int) (at - m.snapIndex)).clear();
                    m.snapIndex = at;
                    m.snapTerm = t;
                    m.snapData = data;
                    m.durableUpTo = m.lastIndex(); // compaction rewrites and forces the remaining log
                } else if (op < 19) { // a snapshot arrives from the leader
                    long at = m.snapIndex + 1 + r.nextInt((int) (m.lastIndex() - m.snapIndex) + 5);
                    boolean matches = at <= m.lastIndex() && r.nextBoolean();
                    long t = matches ? m.at(at).term() : m.term + 1;
                    byte[] data = ("leader@" + at + "#" + step).getBytes();
                    boolean keepSuffix = at <= m.lastIndex() && m.at(at).term() == t;
                    attempt = new Attempt(false, 0, null, at, t, data, keepSuffix);
                    s.installSnapshot(at, t, data);
                    if (keepSuffix) m.log.subList(0, (int) (at - m.snapIndex)).clear();
                    else m.log.clear();
                    m.snapIndex = at;
                    m.snapTerm = t;
                    m.snapData = data;
                    m.durableUpTo = m.lastIndex();
                } else { // power failure between operations
                    disk.crash();
                    s = reopen(disk, m, where + " (crash between operations)");
                }
            } catch (SimDisk.Crash crash) {
                // Died inside an operation. Everything promised before it must survive, and the change it was making
                // must have landed either whole or not at all.
                disk.crash();
                disk.crashPointRate = 0;
                FileStorage fresh = new FileStorage(disk.root());
                disk.crashPointRate = r.nextDouble() < 0.5 ? 0.02 : 0;
                if (attempt.termChange() && fresh.term() == attempt.term()
                        && java.util.Objects.equals(fresh.votedFor(), attempt.vote())) {
                    m.term = attempt.term();
                    m.vote = attempt.vote();
                }
                if (attempt.snapAt() >= 0 && fresh.snapshotIndex() == attempt.snapAt()
                        && Arrays.equals(fresh.snapshotData(), attempt.snapData())) {
                    long at = attempt.snapAt();
                    if (attempt.keepSuffix()) m.log.subList(0, (int) (at - m.snapIndex)).clear();
                    else m.log.clear();
                    m.durableUpTo = attempt.keepSuffix() ? Math.max(at, m.durableUpTo) : at;
                    m.snapIndex = at;
                    m.snapTerm = attempt.snapTerm();
                    m.snapData = attempt.snapData();
                }
                verify(fresh, m, where + " (crash inside an operation)");
                adopt(fresh, m);
                s = fresh;
            }
        }
    }

    @Test
    public void survivesCrashesAtEveryKindOfMoment() throws IOException {
        int seeds = Integer.getInteger("storage.seeds", 500);
        for (long seed = 1; seed <= seeds; seed++) runSeed(seed);
    }
}
