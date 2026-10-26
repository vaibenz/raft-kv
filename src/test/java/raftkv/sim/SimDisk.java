package raftkv.sim;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import raftkv.io.Disk;
import raftkv.io.DiskFile;

/**
 * One machine's disk inside the simulation, with POSIX crash semantics made explicit.
 *
 * <p>Every file has a durable image and a volatile one, plus the list of writes and truncations made since its
 * last {@code force}. Every directory has a durable and a volatile listing. {@link #crash} keeps, for each file,
 * the durable image plus a random prefix of the pending operations, and may tear the last of those part way
 * through; files created, renamed or deleted without a directory sync revert to the durable listing. That is
 * everything a real machine is allowed to lose, and nothing more, so storage code that is correct on a real disk
 * passes here and code that forgets an fsync (of a file or of its directory) eventually fails.
 *
 * <p>Optionally, writes and forces fail with an I/O error at a given rate. A failed force leaves its writes
 * pending, so they may or may not survive the next crash, as on real hardware.
 *
 * <p>Optionally, the process can also die at any I/O call ({@link #crashPointRate}): the call never happens and a
 * {@link Crash} unwinds the node, which the simulator turns into a machine crash. Crashes scheduled between events
 * almost never land between a write and its fsync, or between writing a snapshot and rewriting the log; crash
 * points do, which is where crash consistency bugs live.
 */
final class SimDisk {
    private sealed interface Pending permits Write, Truncate {}

    private record Write(long position, byte[] data) implements Pending {}

    private record Truncate(long size) implements Pending {}

    private static final class Inode {
        byte[] data = new byte[0];
        byte[] durable = new byte[0];
        final List<Pending> pending = new ArrayList<>();

        void apply(Pending p, int tornLength) {
            switch (p) {
                case Write w -> {
                    int len = Math.min(w.data().length, tornLength);
                    long end = w.position() + len;
                    if (end > data.length) data = Arrays.copyOf(data, (int) end);
                    System.arraycopy(w.data(), 0, data, (int) w.position(), len);
                }
                case Truncate t -> data = Arrays.copyOf(data, (int) Math.min(t.size(), data.length));
            }
        }
    }

    private static final class Dir {
        Map<String, Inode> current = new HashMap<>();
        Map<String, Inode> durable = new HashMap<>();
    }

    /** The process died in the middle of an I/O call. An Error, so no ordinary handler swallows it. */
    static final class Crash extends Error {
        private static final long serialVersionUID = 1L;

        Crash() {
            super("simulated crash at an I/O call", null, false, false);
        }
    }

    /** An I/O error the simulator injected on purpose, as opposed to one caused by a bug. */
    static final class InjectedIOException extends IOException {
        private static final long serialVersionUID = 1L;

        InjectedIOException(String message) {
            super(message);
        }
    }

    private final Random random;
    private final Map<String, Dir> dirs = new HashMap<>();
    double ioErrorRate;
    double crashPointRate;
    long crashes, tornWrites, ioErrors, crashPoints;

    SimDisk(Random random) {
        this.random = random;
    }

    Disk root() {
        return view("");
    }

    /** The machine loses power: everything not durable is lost, except a random prefix of pending writes. */
    void crash() {
        crashes++;
        Map<Inode, Boolean> done = new java.util.IdentityHashMap<>();
        for (Dir d : dirs.values()) {
            d.current = new HashMap<>(d.durable);
            for (Inode inode : d.durable.values()) {
                if (done.put(inode, true) != null) continue;
                byte[] base = inode.durable;
                List<Pending> pending = new ArrayList<>(inode.pending);
                inode.data = base.clone();
                inode.pending.clear();
                int keep = pending.isEmpty() ? 0 : random.nextInt(pending.size() + 1);
                for (int i = 0; i < keep; i++) {
                    Pending p = pending.get(i);
                    boolean last = i == keep - 1;
                    int torn = Integer.MAX_VALUE;
                    if (last && p instanceof Write w && w.data().length > 1 && random.nextInt(4) == 0) {
                        torn = random.nextInt(w.data().length);
                        tornWrites++;
                    }
                    inode.apply(p, torn);
                }
                inode.durable = inode.data.clone();
            }
        }
    }

    private void maybeFail(String what) {
        if (crashPointRate > 0 && random.nextDouble() < crashPointRate) {
            crashPoints++;
            throw new Crash();
        }
        if (ioErrorRate > 0 && random.nextDouble() < ioErrorRate) {
            ioErrors++;
            throw new UncheckedIOException(new InjectedIOException("simulated I/O error during " + what));
        }
    }

    private Disk view(String path) {
        Dir dir = dirs.computeIfAbsent(path, p -> new Dir());
        return new Disk() {
            @Override
            public DiskFile open(String name) {
                Inode inode = dir.current.computeIfAbsent(name, n -> new Inode());
                return new SimFile(inode);
            }

            @Override
            public boolean exists(String name) {
                return dir.current.containsKey(name);
            }

            @Override
            public byte[] readAll(String name) {
                Inode inode = dir.current.get(name);
                if (inode == null) throw new UncheckedIOException(new IOException("no such file: " + name));
                return inode.data.clone();
            }

            @Override
            public void rename(String from, String to) {
                maybeFail("rename");
                Inode inode = dir.current.remove(from);
                if (inode == null) throw new UncheckedIOException(new IOException("no such file: " + from));
                dir.current.put(to, inode);
            }

            @Override
            public void delete(String name) {
                dir.current.remove(name);
            }

            @Override
            public List<String> list() {
                return dir.current.keySet().stream().sorted().toList();
            }

            @Override
            public void syncDir() {
                maybeFail("directory sync");
                dir.durable = new HashMap<>(dir.current);
            }

            @Override
            public Disk subdir(String name) {
                return view(path.isEmpty() ? name : path + "/" + name); // directories themselves never vanish
            }
        };
    }

    private final class SimFile implements DiskFile {
        private final Inode inode;

        SimFile(Inode inode) {
            this.inode = inode;
        }

        @Override
        public long size() {
            return inode.data.length;
        }

        @Override
        public byte[] read(long position, int length) {
            if (position + length > inode.data.length)
                throw new UncheckedIOException(new IOException("read past end of file"));
            return Arrays.copyOfRange(inode.data, (int) position, (int) position + length);
        }

        @Override
        public void write(byte[] data, long position) {
            maybeFail("write");
            Write w = new Write(position, data.clone());
            inode.apply(w, Integer.MAX_VALUE);
            inode.pending.add(w);
        }

        @Override
        public void truncate(long size) {
            maybeFail("truncate");
            Truncate t = new Truncate(size);
            inode.apply(t, Integer.MAX_VALUE);
            inode.pending.add(t);
        }

        @Override
        public void force() {
            maybeFail("fsync");
            inode.durable = inode.data.clone();
            inode.pending.clear();
        }

        @Override
        public void close() {}
    }
}
