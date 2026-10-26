package raftkv.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

/** {@link Disk} on a real directory, through NIO. */
public final class RealDisk implements Disk {
    private final Path dir;

    public RealDisk(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path path() {
        return dir;
    }

    @Override
    public DiskFile open(String name) {
        try {
            FileChannel ch = FileChannel.open(dir.resolve(name), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            return new RealFile(ch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean exists(String name) {
        return Files.exists(dir.resolve(name));
    }

    @Override
    public byte[] readAll(String name) {
        try {
            return Files.readAllBytes(dir.resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void rename(String from, String to) {
        try {
            Files.move(dir.resolve(from), dir.resolve(to), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void delete(String name) {
        try {
            Files.deleteIfExists(dir.resolve(name));
        } catch (NoSuchFileException ignored) {
            // already gone
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<String> list() {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void syncDir() {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Disk subdir(String name) {
        RealDisk d = new RealDisk(dir.resolve(name));
        syncDir(); // the new directory's own entry
        return d;
    }

    private record RealFile(FileChannel ch) implements DiskFile {
        @Override
        public long size() {
            try {
                return ch.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public byte[] read(long position, int length) {
            ByteBuffer b = ByteBuffer.allocate(length);
            try {
                while (b.hasRemaining()) {
                    int n = ch.read(b, position + b.position());
                    if (n < 0) throw new IOException("unexpected end of file");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return b.array();
        }

        @Override
        public void write(byte[] data, long position) {
            ByteBuffer b = ByteBuffer.wrap(data);
            try {
                while (b.hasRemaining()) ch.write(b, position + b.position());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void truncate(long size) {
            try {
                ch.truncate(size);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void force() {
            try {
                ch.force(false);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            try {
                ch.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
