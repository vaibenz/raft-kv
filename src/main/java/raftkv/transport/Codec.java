package raftkv.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import raftkv.core.LogEntry;
import raftkv.core.Message;
import raftkv.core.Message.AppendEntries;
import raftkv.core.Message.AppendReply;
import raftkv.core.Message.RequestVote;
import raftkv.core.Message.VoteReply;

/** Compact binary encoding of Raft messages: a type byte followed by the fields in order. */
public final class Codec {
    private static final byte REQUEST_VOTE = 1, VOTE_REPLY = 2, APPEND = 3, APPEND_REPLY = 4;

    private Codec() {}

    public static byte[] encode(Message m) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            switch (m) {
                case RequestVote r -> {
                    out.writeByte(REQUEST_VOTE);
                    out.writeLong(r.term());
                    out.writeUTF(r.from());
                    out.writeLong(r.lastLogIndex());
                    out.writeLong(r.lastLogTerm());
                    out.writeBoolean(r.preVote());
                }
                case VoteReply r -> {
                    out.writeByte(VOTE_REPLY);
                    out.writeLong(r.term());
                    out.writeUTF(r.from());
                    out.writeBoolean(r.granted());
                    out.writeBoolean(r.preVote());
                }
                case AppendEntries a -> {
                    out.writeByte(APPEND);
                    out.writeLong(a.term());
                    out.writeUTF(a.from());
                    out.writeLong(a.prevLogIndex());
                    out.writeLong(a.prevLogTerm());
                    out.writeLong(a.leaderCommit());
                    out.writeLong(a.round());
                    out.writeInt(a.entries().size());
                    for (LogEntry e : a.entries()) {
                        out.writeLong(e.term());
                        out.writeInt(e.command().length);
                        out.write(e.command());
                    }
                }
                case AppendReply r -> {
                    out.writeByte(APPEND_REPLY);
                    out.writeLong(r.term());
                    out.writeUTF(r.from());
                    out.writeBoolean(r.success());
                    out.writeLong(r.matchIndex());
                    out.writeLong(r.conflictIndex());
                    out.writeLong(r.conflictTerm());
                    out.writeLong(r.round());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    public static Message decode(byte[] data) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            byte type = in.readByte();
            return switch (type) {
                case REQUEST_VOTE -> new RequestVote(in.readLong(), in.readUTF(), in.readLong(), in.readLong(),
                        in.readBoolean());
                case VOTE_REPLY -> new VoteReply(in.readLong(), in.readUTF(), in.readBoolean(), in.readBoolean());
                case APPEND -> {
                    long term = in.readLong();
                    String from = in.readUTF();
                    long prevIndex = in.readLong(), prevTerm = in.readLong(), commit = in.readLong(), round = in.readLong();
                    int n = in.readInt();
                    if (n < 0) throw new IOException("negative entry count");
                    List<LogEntry> entries = new ArrayList<>(Math.min(n, 4096));
                    for (int i = 0; i < n; i++) {
                        long t = in.readLong();
                        int len = in.readInt();
                        if (len < 0 || len > in.available()) throw new IOException("bad entry length");
                        byte[] cmd = new byte[len];
                        in.readFully(cmd);
                        entries.add(new LogEntry(t, cmd));
                    }
                    yield new AppendEntries(term, from, prevIndex, prevTerm, entries, commit, round);
                }
                case APPEND_REPLY -> new AppendReply(in.readLong(), in.readUTF(), in.readBoolean(), in.readLong(),
                        in.readLong(), in.readLong(), in.readLong());
                default -> throw new IOException("unknown message type " + type);
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
