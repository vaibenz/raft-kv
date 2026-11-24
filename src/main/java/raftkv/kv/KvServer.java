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
 * writes exactly once (a client id without a positive sequence number gets 400); a write older than the client's latest applied one gets 409. Values are capped at {@link #MAX_VALUE_BYTES}; a larger PUT gets 413. Keys are capped at {@link #MAX_KEY_BYTES}; a longer one gets 414.
 */
public final class KvServer implements AutoCloseable {
    public record NodeAddress(String host, int raftPort, int httpPort) {}

    /**
     * Largest value a PUT may carry. Every value is a Raft log entry that is fsynced, replicated to every follower
     * and kept until the next snapshot, so one unbounded request could stall the whole cluster or exhaust memory.
     */
    public static final int MAX_VALUE_BYTES = 1 << 20;

    /**
     * Longest key, in UTF-8 bytes after URL decoding. The JDK HTTP server accepts a request path of a few hundred
     * KB, and a key is copied into every log entry that writes it, every replica's map and the in memory index of
     * every LSM table that holds it, so it is capped far below the value cap. A longer key gets 414.
     */
    public static final int MAX_KEY_BYTES = 1024;

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
        String path = decodePath(ex.getRequestURI().getRawPath());
        if (path == null || !path.startsWith("/kv/")) {
            reply(ex, 400, "key is not valid UTF-8 once percent decoded\n");
            return;
        }
        String key = path.substring("/kv/".length());
        if (key.isEmpty()) {
            reply(ex, 400, "missing key\n");
            return;
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            reply(ex, 414, "key longer than " + MAX_KEY_BYTES + " bytes\n");
            return;
        }
        long clientId = header(ex, "X-Client-Id", 0);
        long seq = header(ex, "X-Request-Seq", 0);
        if (clientId != 0 && seq <= 0) {
            // Without a sequence number every write would carry seq 0, so each one after the first looks like a
            // retry of it: it is skipped and still answered 200.
            reply(ex, 400, "X-Client-Id needs a positive X-Request-Seq\n");
            return;
        }
        if (clientId == 0 && ex.getRequestHeaders().getFirst("X-Request-Seq") != null) {
            // Client id 0 opts out of deduplication, so a sequence number without an id was accepted and ignored:
            // the client believed its retries were exactly once while each one was applied again.
            reply(ex, 400, "X-Request-Seq needs a nonzero X-Client-Id\n");
            return;
        }
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
        // The raw path and query, not the whole request URI: a client that sent the absolute form ("GET
        // http://follower/kv/x") would otherwise get the follower's URI pasted after the leader's address.
        java.net.URI uri = ex.getRequestURI();
        String query = uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery();
        ex.getResponseHeaders().set("Location", "http://" + a.host() + ":" + a.httpPort() + uri.getRawPath() + query);
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

    /**
     * Percent decodes a raw URI path as UTF-8, or returns null if the bytes are not valid UTF-8. URI.getPath()
     * turns every malformed sequence into U+FFFD, so "/kv/%FF" and "/kv/%FE" would both be stored under the
     * key "\uFFFD" and overwrite each other.
     */
    static String decodePath(String raw) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '%') {
                if (i + 2 >= raw.length()) return null;
                int hi = Character.digit(raw.charAt(i + 1), 16), lo = Character.digit(raw.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) return null;
                bytes.write(hi << 4 | lo);
                i += 2;
            } else {
                int end = Character.isHighSurrogate(c) && i + 1 < raw.length() ? i + 2 : i + 1;
                bytes.writeBytes(raw.substring(i, end).getBytes(StandardCharsets.UTF_8));
                i = end - 1;
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            return null;
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

    /**
     * Parses {@code id=host:raftPort:httpPort,...}. Every node must be started with the same list, so a mistake
     * here is refused with a message rather than tolerated: a repeated id used to overwrite the earlier entry,
     * which quietly shrinks the cluster and changes what a majority is, and a short entry failed with an
     * ArrayIndexOutOfBoundsException. Ids are letters, digits, '.', '_' and '-': an id goes into the /status JSON
     * as is, and "n1 = host:..." used to give the id "n1 " and the host " host", which never resolves. No two
     * nodes may share a host and port (Raft or HTTP): a copy pasted entry such as n3=h:7002:8003 next to
     * n2=h:7002:8002 sent n3's Raft traffic to n2, and the second node to start failed to bind.
     */
    public static Map<String, NodeAddress> parseCluster(String spec) {
        Map<String, NodeAddress> m = new LinkedHashMap<>();
        Map<String, String> endpoints = new java.util.HashMap<>(); // "host:port" -> the node that uses it
        for (String part : spec.split(",")) {
            String entry = part.trim();
            String[] kv = entry.split("=", 2);
            String[] hp = kv.length == 2 ? kv[1].split(":", -1) : new String[0];
            if (kv[0].isEmpty() || hp.length != 3 || hp[0].isEmpty()) {
                throw new IllegalArgumentException("bad cluster entry '" + entry + "', want id=host:raftPort:httpPort");
            }
            if (!kv[0].matches("[A-Za-z0-9._-]+")) {
                throw new IllegalArgumentException("bad node id '" + kv[0] + "' in cluster entry '" + entry
                        + "', use letters, digits, '.', '_' or '-'");
            }
            if (!hp[0].matches("[A-Za-z0-9.-]+")) {
                throw new IllegalArgumentException("bad host '" + hp[0] + "' in cluster entry '" + entry + "'");
            }
            NodeAddress a = new NodeAddress(hp[0], port(hp[1], entry), port(hp[2], entry));
            if (a.raftPort() == a.httpPort()) {
                throw new IllegalArgumentException("node " + kv[0] + " uses port " + a.raftPort() + " for both Raft and HTTP");
            }
            if (m.putIfAbsent(kv[0], a) != null) {
                throw new IllegalArgumentException("node " + kv[0] + " is listed twice in the cluster");
            }
            for (int port : new int[] {a.raftPort(), a.httpPort()}) {
                String endpoint = a.host().toLowerCase(java.util.Locale.ROOT) + ":" + port;
                String other = endpoints.putIfAbsent(endpoint, kv[0]);
                if (other != null) {
                    throw new IllegalArgumentException("nodes " + other + " and " + kv[0] + " both use " + endpoint);
                }
            }
        }
        return m;
    }

    private static int port(String s, String entry) {
        try {
            int p = Integer.parseInt(s);
            if (p >= 1 && p <= 65535) return p;
        } catch (NumberFormatException ignored) {
            // reported below
        }
        throw new IllegalArgumentException("bad port '" + s + "' in cluster entry '" + entry + "'");
    }

    private static final List<String> OPTIONS = List.of("--id", "--cluster", "--data", "--engine");

    /**
     * Reads {@code --name value} pairs. Unknown, repeated or valueless options are refused: they used to be
     * skipped, and a typo such as {@code --date /mnt/n1} then started the node on the default data directory,
     * empty, so it came back having forgotten its log and the vote it cast in its current term.
     */
    public static Map<String, String> parseOptions(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String name = args[i];
            if (!OPTIONS.contains(name)) throw new IllegalArgumentException("unknown option " + name);
            if (i + 1 >= args.length || args[i + 1].isEmpty()) throw new IllegalArgumentException(name + " needs a value");
            if (opts.putIfAbsent(name, args[i + 1]) != null) throw new IllegalArgumentException(name + " given twice");
        }
        if (!opts.containsKey("--id") || !opts.containsKey("--cluster")) {
            throw new IllegalArgumentException("--id and --cluster are required");
        }
        String engine = opts.get("--engine");
        if (engine != null && !engine.equals("lsm") && !engine.equals("memory")) {
            throw new IllegalArgumentException("unknown engine " + engine + " (use lsm or memory)");
        }
        return opts;
    }

    /**
     * Every mistake on the command line is reported as a usage error before anything is opened. A bad --cluster
     * or an --id missing from it used to escape as a stack trace, and an unknown --engine was only noticed after
     * the Raft log had already been created in the data directory.
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> opts;
        Map<String, NodeAddress> cluster;
        try {
            opts = parseOptions(args);
            cluster = parseCluster(opts.get("--cluster"));
            if (!cluster.containsKey(opts.get("--id"))) {
                throw new IllegalArgumentException("node " + opts.get("--id") + " is not in the cluster list");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("raftkv: " + e.getMessage());
            System.err.println("usage: java -jar raftkv.jar --id n1 --cluster n1=host:raftPort:httpPort,... "
                    + "[--data dir] [--engine lsm|memory]");
            System.exit(2);
            return;
        }
        String id = opts.get("--id");
        Path data = Path.of(opts.getOrDefault("--data", "data/" + id));
        KvServer server = new KvServer(id, cluster, data, RaftConfig.defaults(),
                opts.getOrDefault("--engine", "lsm"));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        NodeAddress self = server.cluster.get(id);
        System.out.printf("node %s: raft on :%d, http on :%d, data in %s%n", id, self.raftPort(), self.httpPort(),
                data.toAbsolutePath());
    }
}
