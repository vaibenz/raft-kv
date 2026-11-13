package raftkv.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import raftkv.core.Listener;
import raftkv.core.NotLeaderException;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.core.StateMachine;
import raftkv.io.RealDisk;
import raftkv.lsm.LsmStore;
import raftkv.storage.FileStorage;
import raftkv.transport.TcpTransport;

/**
 * A key value server node: Raft over TCP, durable storage on disk, and an HTTP API for clients.
 *
 * <pre>
 * java -jar raftkv.jar --id n1 --data data/n1 \
 *      --cluster n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003
 * </pre>
 *
 * Each cluster entry is {@code id=host:raftPort:httpPort}. {@code --engine lsm} (the default) keeps the data in
 * the LSM tree under {@code data/sm}; {@code --engine memory} keeps it in a hash map and snapshots it whole. API:
 * {@code GET|PUT|DELETE /kv/{key}} and {@code GET /status}. Requests sent to a follower are redirected
 * (HTTP 307) to the leader. Optional headers {@code X-Client-Id} and {@code X-Request-Seq} make retried
 * writes exactly once; a write older than the client's latest applied one gets 409. Values are capped at {@link #MAX_VALUE_BYTES}; a larger PUT gets 413.
 */
public final class KvServer implements AutoCloseable {
    public record NodeAddress(String host, int raftPort, int httpPort) {}

    /**
     * Largest value a PUT may carry. Every value is a Raft log entry that is fsynced, replicated to every follower
     * and kept until the next snapshot, so one unbounded request could stall the whole cluster or exhaust memory.
     */
    public static final int MAX_VALUE_BYTES = 1 << 20;

    private final String id;
    private final Map<String, NodeAddress> cluster;
    private final FileStorage storage;
    private final KvStore kv;
    private final StateMachine machine;
    private final RaftNode node;
    private final HttpServer http;

    public KvServer(String id, Map<String, NodeAddress> cluster, Path dataDir, RaftConfig config) throws IOException {
        this(id, cluster, dataDir, config, "lsm");
    }

    public KvServer(String id, Map<String, NodeAddress> cluster, Path dataDir, RaftConfig config, String engine)
            throws IOException {
        this.id = id;
        this.cluster = cluster;
        this.storage = new FileStorage(dataDir.resolve("raft"));
        switch (engine) {
            case "lsm" -> {
                LsmStateMachine lsm = new LsmStateMachine(new RealDisk(dataDir.resolve("sm")), LsmStore.Options.defaults());
                kv = lsm;
                machine = lsm;
            }
            case "memory" -> {
                KvStateMachine mem = new KvStateMachine();
                kv = mem;
                machine = mem;
            }
            default -> throw new IllegalArgumentException("unknown engine " + engine + " (use lsm or memory)");
        }
        NodeAddress self = cluster.get(id);
        if (self == null) throw new IllegalArgumentException("node " + id + " is not in the cluster list");
        Map<String, InetSocketAddress> raftAddrs = new LinkedHashMap<>();
        cluster.forEach((nid, a) -> raftAddrs.put(nid, new InetSocketAddress(a.host(), a.raftPort())));
        List<String> peers = new ArrayList<>(cluster.keySet());
        peers.remove(id);
        this.node = new RaftNode(id, peers, storage, machine, config, Listener.NONE, (nid, inbound) -> {
            try {
                return new TcpTransport(nid, new InetSocketAddress(self.raftPort()), raftAddrs, inbound);
            } catch (IOException e) {
                throw new IllegalStateException("cannot bind Raft port " + self.raftPort(), e);
            }
        });
        this.http = HttpServer.create(new InetSocketAddress(self.httpPort()), 128);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/kv/", this::handleKv);
        http.createContext("/status", this::handleStatus);
    }

    public void start() {
        node.start();
        http.start();
    }

    public RaftNode node() {
        return node;
    }

    public KvStore store() {
        return kv;
    }

    @Override
    public void close() {
        http.stop(0);
        node.stop();
        storage.close();
        if (machine instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // shutting down
            }
        }
    }

    // ------------------------------------------------------------------ HTTP handlers

    private void handleKv(HttpExchange ex) throws IOException {
        try {
            serveKv(ex);
        } catch (NotLeaderException e) {
            redirect(ex, e.leaderHint());
        } catch (TimeoutException e) {
            reply(ex, 503, "timed out waiting for the cluster\n");
        } catch (NumberFormatException e) {
            reply(ex, 400, "bad X-Client-Id or X-Request-Seq\n");
        } finally {
            ex.close();
        }
    }

    private void serveKv(HttpExchange ex) throws IOException, TimeoutException {
        // URI path decoding: "%20" is a space but "+" stays a plus. URLDecoder is for form bodies and would turn
        // "/kv/a+b" into the key "a b".
        String key = ex.getRequestURI().getPath().substring("/kv/".length());
        if (key.isEmpty()) {
            reply(ex, 400, "missing key\n");
            return;
        }
        long clientId = header(ex, "X-Client-Id", 0);
        long seq = header(ex, "X-Request-Seq", 0);
        switch (ex.getRequestMethod()) {
            case "GET" -> {
                await(node.readBarrier(), 3);
                byte[] v = kv.get(key);
                if (v == null) reply(ex, 404, "not found\n");
                else reply(ex, 200, v);
            }
            case "PUT" -> {
                byte[] value = readValue(ex);
                if (value == null) {
                    reply(ex, 413, "value larger than " + MAX_VALUE_BYTES + " bytes\n");
                    return;
                }
                byte[] r = await(node.propose(KvStateMachine.put(clientId, seq, key, value)), 5);
                if (KvStateMachine.isStale(r)) replyStale(ex);
                else reply(ex, 200, "OK\n");
            }
            case "DELETE" -> {
                byte[] r = await(node.propose(KvStateMachine.delete(clientId, seq, key)), 5);
                if (KvStateMachine.isStale(r)) {
                    replyStale(ex);
                    return;
                }
                boolean deleted = r.length > 0; // KvStateMachine.DELETED, or empty if the key was absent
                reply(ex, deleted ? 200 : 404, deleted ? "deleted\n" : "not found\n");
            }
            default -> {
                ex.getResponseHeaders().set("Allow", "GET, PUT, DELETE");
                reply(ex, 405, "method not allowed\n");
            }
        }
    }

    /**
     * The client already had a write with a higher X-Request-Seq applied, so this older one was skipped. Saying 200
     * here would tell the client a write took effect when it did not.
     */
    private static void replyStale(HttpExchange ex) throws IOException {
        reply(ex, 409, "stale request: a newer X-Request-Seq from this client was already applied\n");
    }

    private void handleStatus(HttpExchange ex) throws IOException {
        try (ex) {
            RaftNode.Status s = node.status();
            String json = String.format(
                    "{\"id\":\"%s\",\"role\":\"%s\",\"term\":%d,\"leader\":%s,\"commitIndex\":%d,"
                            + "\"lastApplied\":%d,\"lastLogIndex\":%d,\"snapshotIndex\":%d,\"keys\":%d}%n",
                    s.id(), s.role(), s.term(), s.leaderId() == null ? "null" : "\"" + s.leaderId() + "\"",
                    s.commitIndex(), s.lastApplied(), s.lastLogIndex(), s.snapshotIndex(), kv.size());
            ex.getResponseHeaders().set("Content-Type", "application/json");
            reply(ex, 200, json);
        }
    }

    private void redirect(HttpExchange ex, String leader) throws IOException {
        NodeAddress a = leader == null ? null : cluster.get(leader);
        if (a == null) {
            ex.getResponseHeaders().set("Retry-After", "1");
            reply(ex, 503, "no leader elected yet, retry\n");
            return;
        }
        ex.getResponseHeaders().set("Location", "http://" + a.host() + ":" + a.httpPort() + ex.getRequestURI());
        reply(ex, 307, "redirecting to leader " + leader + "\n");
    }

    private static <T> T await(CompletableFuture<T> f, int seconds) throws TimeoutException {
        try {
            return f.get(seconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re) throw re;
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TimeoutException("interrupted");
        }
    }

    /**
     * How much of an oversized body is read and thrown away before the 413 goes out. Answering with the body still
     * unread makes the HTTP server close the connection with the client's bytes in its receive buffer, which sends
     * a TCP reset, and the reset can destroy the 413 before the client reads it: the client sees a broken
     * connection instead of a clear answer. Bodies beyond this are not worth reading; those connections just close.
     */
    static final long MAX_DRAIN_BYTES = 16L << 20;

    /** The request body, or null if it is over {@link #MAX_VALUE_BYTES}. Never buffers more than the cap. */
    private static byte[] readValue(HttpExchange ex) throws IOException {
        String declared = ex.getRequestHeaders().getFirst("Content-Length");
        long length = declared == null ? -1 : declared.length() > 18 ? Long.MAX_VALUE : Long.parseLong(declared);
        try (InputStream in = ex.getRequestBody()) {
            if (length > MAX_VALUE_BYTES) { // over the cap before reading a byte
                if (length <= MAX_VALUE_BYTES + MAX_DRAIN_BYTES) discard(in, length);
                return null;
            }
            byte[] value = in.readNBytes(MAX_VALUE_BYTES + 1); // chunked bodies declare no length
            if (value.length <= MAX_VALUE_BYTES) return value;
            discard(in, MAX_DRAIN_BYTES);
            return null;
        }
    }

    /** Reads and drops up to limit bytes, through a small buffer, so nothing large is ever held in memory. */
    private static void discard(InputStream in, long limit) throws IOException {
        byte[] buf = new byte[64 * 1024];
        for (long left = limit; left > 0; ) {
            int n = in.read(buf, 0, (int) Math.min(buf.length, left));
            if (n < 0) return;
            left -= n;
        }
    }

    private static long header(HttpExchange ex, String name, long dflt) {
        String v = ex.getRequestHeaders().getFirst(name);
        return v == null ? dflt : Long.parseLong(v);
    }

    private static void reply(HttpExchange ex, int status, String body) throws IOException {
        reply(ex, status, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void reply(HttpExchange ex, int status, byte[] body) throws IOException {
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    // ------------------------------------------------------------------ command line

    public static Map<String, NodeAddress> parseCluster(String spec) {
        Map<String, NodeAddress> m = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=", 2);
            String[] hp = kv[1].split(":");
            m.put(kv[0], new NodeAddress(hp[0], Integer.parseInt(hp[1]), Integer.parseInt(hp[2])));
        }
        return m;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) opts.put(args[i], args[i + 1]);
        String id = opts.get("--id"), spec = opts.get("--cluster");
        if (id == null || spec == null) {
            System.err.println("usage: java -jar raftkv.jar --id n1 --cluster n1=host:raftPort:httpPort,... "
                    + "[--data dir] [--engine lsm|memory]");
            System.exit(2);
        }
        Path data = Path.of(opts.getOrDefault("--data", "data/" + id));
        KvServer server = new KvServer(id, parseCluster(spec), data, RaftConfig.defaults(),
                opts.getOrDefault("--engine", "lsm"));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        NodeAddress self = server.cluster.get(id);
        System.out.printf("node %s: raft on :%d, http on :%d, data in %s%n", id, self.raftPort(), self.httpPort(),
                data.toAbsolutePath());
    }
}
