package raftkv;

import java.util.Arrays;
import java.util.List;
import raftkv.core.LogEntry;
import raftkv.core.Message;
import raftkv.transport.Codec;

public class CodecTest {
    @Test
    public void roundTripsEveryMessageType() {
        List<Message> messages = List.of(
                new Message.RequestVote(7, "n1", 42, 6, false),
                new Message.RequestVote(8, "n1", 42, 6, true),
                new Message.VoteReply(7, "n2", true, false),
                new Message.VoteReply(7, "n2", false, true),
                new Message.AppendEntries(9, "n3", 10, 8,
                        List.of(new LogEntry(9, "a".getBytes()), new LogEntry(9, new byte[0])), 9, 123),
                new Message.AppendEntries(9, "n3", 0, 0, List.of(), 0, 0),
                new Message.AppendReply(9, "n4", false, 0, 5, 3, 77));
        for (Message m : messages) {
            byte[] wire = Codec.encode(m);
            Message back = Codec.decode(wire);
            Check.equal(m.getClass(), back.getClass(), "type");
            Check.isTrue(Arrays.equals(wire, Codec.encode(back)), "re-encoding is identical for " + m);
            if (!(m instanceof Message.AppendEntries)) Check.equal(m, back, "fields"); // byte[] has identity equals
        }
        Message.AppendEntries a = (Message.AppendEntries) Codec.decode(Codec.encode(messages.get(4)));
        Check.equal("a", new String(a.entries().get(0).command()), "entry payload");
        Check.isTrue(a.entries().get(1).isNoop(), "no-op entry preserved");
    }

    @Test
    public void rejectsGarbage() {
        try {
            Codec.decode(new byte[] {99, 1, 2});
            throw new AssertionError("expected failure");
        } catch (RuntimeException expected) {
            // ok
        }
    }
}
