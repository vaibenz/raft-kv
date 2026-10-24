package raftkv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import raftkv.core.LogEntry;
import raftkv.storage.FileStorage;

public class FileStorageTest {
    private static LogEntry e(long term, String cmd) { return new LogEntry(term, cmd.getBytes()); }

    @Test
    public void persistsTermVoteAndLogAcrossRestart() throws IOException {
        Path dir = Files.createTempDirectory("raftkv");
        try (FileStorage s = new FileStorage(dir)) {
            s.setTermAndVote(5, "n2");
            s.append(List.of(e(1, "a"), e(1, "b"), e(5, "c")));
            s.sync();
        }
        try (FileStorage s = new FileStorage(dir)) {
            Check.equal(5L, s.term(), "term");
            Check.equal("n2", s.votedFor(), "vote");
            Check.equal(3L, s.lastIndex(), "log length");
            Check.equal("c", new String(s.get(3).command()), "last entry");
            Check.equal(5L, s.lastTerm(), "last term");
        }
    }

    @Test
    public void truncationIsDurable() throws IOException {
        Path dir = Files.createTempDirectory("raftkv");
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(1, "c")));
            s.truncateFrom(2);
            s.append(List.of(e(2, "x")));
            s.sync();
        }
        try (FileStorage s = new FileStorage(dir)) {
            Check.equal(2L, s.lastIndex(), "length after truncate and append");
            Check.equal("x", new String(s.get(2).command()), "replacement entry");
        }
    }

    @Test
    public void tornTailWriteIsDiscarded() throws IOException {
        Path dir = Files.createTempDirectory("raftkv");
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(e(1, "a"), e(1, "b")));
            s.sync();
        }
        // Simulate a crash in the middle of writing a third record.
        try (FileChannel ch = FileChannel.open(dir.resolve("log"), StandardOpenOption.APPEND)) {
            ch.write(java.nio.ByteBuffer.wrap(new byte[] {0, 0, 0, 5, 0, 0, 0}));
        }
        try (FileStorage s = new FileStorage(dir)) {
            Check.equal(2L, s.lastIndex(), "partial record dropped");
            s.append(List.of(e(2, "c")));
            s.sync();
        }
        try (FileStorage s = new FileStorage(dir)) {
            Check.equal(3L, s.lastIndex(), "appends continue cleanly after recovery");
            Check.equal("c", new String(s.get(3).command()), "new entry intact");
        }
    }

    @Test
    public void corruptRecordIsDetectedByChecksum() throws IOException {
        Path dir = Files.createTempDirectory("raftkv");
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(e(1, "aaaa"), e(1, "bbbb")));
            s.sync();
        }
        byte[] data = Files.readAllBytes(dir.resolve("log"));
        data[data.length - 6] ^= 0x40; // flip a bit inside the second record's payload
        Files.write(dir.resolve("log"), data);
        try (FileStorage s = new FileStorage(dir)) {
            Check.equal(1L, s.lastIndex(), "corrupt record rejected");
        }
    }
}
