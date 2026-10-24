package raftkv.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import raftkv.core.Message;

/**
 * Raft messages over TCP using length prefixed frames.
 *
 * <p>Each peer gets one outbound connection and a sender thread fed by a bounded queue. The sender writes
 * every frame that is already queued before flushing, so a burst of messages costs one system call. If a
 * peer is down, frames are dropped and the connection is retried later: Raft's retries make that safe.
 * All threads are Java 21 virtual threads.
 */
public final class TcpTransport implements Transport {
    private static final int MAX_FRAME = 64 << 20;
    private static final int QUEUE_CAPACITY = 10_000;

    private final String selfId;
    private final Consumer<Message> inbound;
    private final ServerSocket server;
    private final Map<String, Peer> peers = new HashMap<>();
    private final Set<Socket> inboundSockets = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    public TcpTransport(String selfId, InetSocketAddress bind, Map<String, InetSocketAddress> peerAddresses,
                        Consumer<Message> inbound) throws IOException {
        this.selfId = selfId;
        this.inbound = inbound;
        this.server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(bind);
        peerAddresses.forEach((id, addr) -> {
            if (!id.equals(selfId)) peers.put(id, new Peer(addr));
        });
        Thread.ofVirtual().name("raft-accept-" + selfId).start(this::acceptLoop);
        peers.values().forEach(p -> Thread.ofVirtual().name("raft-send-" + selfId).start(p::sendLoop));
    }

    @Override
    public void send(String to, Message m) {
        Peer p = peers.get(to);
        if (p != null && !closed) p.queue.offer(Codec.encode(m)); // full queue: drop, Raft will retry
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                inboundSockets.add(s);
                Thread.ofVirtual().start(() -> readLoop(s));
            } catch (IOException e) {
                if (!closed) sleep(50);
            }
        }
    }

    private void readLoop(Socket s) {
        try (s; DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()))) {
            while (!closed) {
                int len = in.readInt();
                if (len <= 0 || len > MAX_FRAME) return;
                byte[] frame = new byte[len];
                in.readFully(frame);
                inbound.accept(Codec.decode(frame));
            }
        } catch (IOException | RuntimeException e) {
            // connection dropped or garbage received: close it, the peer will reconnect
        } finally {
            inboundSockets.remove(s);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
            // shutting down
        }
        for (Socket s : inboundSockets) closeQuietly(s);
        for (Peer p : peers.values()) p.shutdown();
    }

    private static void closeQuietly(Socket s) {
        try {
            if (s != null) s.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final class Peer {
        final InetSocketAddress address;
        final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        volatile Socket socket;
        volatile Thread thread;

        Peer(InetSocketAddress address) {
            this.address = address;
        }

        void sendLoop() {
            thread = Thread.currentThread();
            DataOutputStream out = null;
            while (!closed) {
                byte[] frame;
                try {
                    frame = queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    if (out == null) out = connect();
                    out.writeInt(frame.length);
                    out.write(frame);
                    byte[] next;
                    while ((next = queue.poll()) != null) { // batch whatever is already queued
                        out.writeInt(next.length);
                        out.write(next);
                    }
                    out.flush();
                } catch (IOException e) {
                    closeQuietly(socket);
                    socket = null;
                    out = null;
                    queue.clear(); // stale messages are useless after a reconnect
                    sleep(100);
                }
            }
        }

        DataOutputStream connect() throws IOException {
            Socket s = new Socket();
            try {
                s.connect(address, 500);
                s.setTcpNoDelay(true);
            } catch (IOException e) {
                closeQuietly(s);
                throw e;
            }
            socket = s;
            return new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 64 * 1024));
        }

        void shutdown() {
            closeQuietly(socket);
            Thread t = thread;
            if (t != null) t.interrupt();
        }
    }
}
