package raftkv.io;

/** An open file. All methods throw {@link java.io.UncheckedIOException} on I/O errors. */
public interface DiskFile extends AutoCloseable {
    long size();

    /** Reads exactly {@code length} bytes at {@code position}. */
    byte[] read(long position, int length);

    void write(byte[] data, long position);

    void truncate(long size);

    /** Makes everything written to this file so far durable (fdatasync). */
    void force();

    @Override
    void close();
}
