package raftkv.io;

import java.util.List;

/**
 * The small slice of a file system that the storage code needs, with the durability rules made explicit.
 *
 * <p>The rules are the POSIX ones that storage engines get wrong:
 * <ul>
 *   <li>Data written to a file is durable only after {@link DiskFile#force()}. A crash may keep any prefix of
 *       the unsynced writes, and the last of those may be torn.</li>
 *   <li>Creating, renaming or deleting a file changes its directory. That change is durable only after
 *       {@link #syncDir()}, even if the file's own contents were forced.</li>
 * </ul>
 * {@link RealDisk} maps this onto NIO. The simulator's disk keeps the durable and volatile state apart and, on a
 * crash, throws away exactly what a real machine could lose, so every persistence path is tested against it.
 *
 * <p>Paths are file names inside one directory; storage components that need several directories get several
 * disks via {@link #subdir}.
 */
public interface Disk {
    /** Opens a file for reading and writing, creating it (in the volatile directory) if needed. */
    DiskFile open(String name);

    boolean exists(String name);

    /** The whole current contents of a file. */
    byte[] readAll(String name);

    /** Atomically replaces {@code to} with {@code from}. Durable after {@link #syncDir()}. */
    void rename(String from, String to);

    void delete(String name);

    List<String> list();

    /** Makes creations, renames and deletions in this directory durable. */
    void syncDir();

    /** A disk rooted at a subdirectory, created if needed. */
    Disk subdir(String name);

    /**
     * Writes a whole file so that after a crash it holds either the old contents or the new, never a mix:
     * write a temporary file, force it, rename it into place, then sync the directory.
     */
    default void writeAtomically(String name, byte[] data) {
        String tmp = name + ".tmp";
        try (DiskFile f = open(tmp)) {
            f.truncate(0);
            f.write(data, 0);
            f.force();
        }
        rename(tmp, name);
        if (!raftkv.core.Mutation.is(raftkv.core.Mutation.SKIP_DIR_FSYNC)) syncDir();
    }
}
