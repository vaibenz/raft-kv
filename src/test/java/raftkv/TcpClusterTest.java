package raftkv;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import raftkv.core.RaftConfig;
import raftkv.core.RaftNode;
import raftkv.kv.KvServer;
import raftkv.kv.KvServer.NodeAddress;

/** End to end: real TCP between nodes, real fsync to disk, and the HTTP client API. */
public class TcpClusterTest {
    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    public void parsesTheClusterListAndRefusesMistakes() {
        Map<String, NodeAddress> m = KvServer.parseCluster("n1=127.0.0.1:7001:8001, n2=10.0.0.2:7002:8002");
        Check.equal(java.util.List.of("n1", "n2"), java.util.List.copyOf(m.keySet()), "ids in order");
        Check.equal(new NodeAddress("10.0.0.2", 7002, 8002), m.get("n2"), "second node");
        for (String bad : new String[] {
                "n1=a:1:2,n2=b:3:4,n2=c:5:6", // a repeated id would silently replace the earlier node
                "n1=a:7001", "n1", "=a:1:2", "n1=:1:2", "n1=a:1:2:3",
                "n1=a:x:2", "n1=a:0:2", "n1=a:1:70000", "n1=a:7001:7001"}) {
            try {
                KvServer.parseCluster(bad);
                throw new AssertionError("accepted " + bad);
            } catch (IllegalArgumentException expected) {
                Check.isTrue(expected.getMessage() != null && !expected.getMessage().isEmpty(), "message for " + bad);
            }
        }
    }

    @Test
    public void refusesUnknownOrIncompleteOptions() {
        Map<String, String> ok = KvServer.parseOptions(new String[] {"--id", "n1", "--cluster", "n1=a:1:2", "--data", "d"});
        Check.equal("d", ok.get("--data"), "data dir");
        for (String[] bad : new String[][] {
                {"--id", "n1", "--cluster", "n1=a:1:2", "--date", "/mnt/n1"}, // a typo used to mean the default dir
                {"--id", "n1", "--cluster", "n1=a:1:2", "--data"},
                {"--id", "n1", "--cluster", "n1=a:1:2", "--data", ""},
                {"--id", "n1", "--id", "n2", "--cluster", "n1=a:1:2"},
                {"--cluster", "n1=a:1:2"},
                {"n1", "--id", "--cluster", "n1=a:1:2"}}) {
            try {
                KvServer.parseOptions(bad);
                throw new AssertionError("accepted " + String.join(" ", bad));
            } catch (IllegalArgumentException expected) {
                Check.isTrue(expected.getMessage() != null && !expected.getMessage().isEmpty(), "message");
            }
        }
    }

    private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    private HttpResponse<String> call(String method, int port, String path, String body) throws Exception {
        return call(method, port, path, body, 0, 0);
    }

    private HttpResponse<String> call(String method, int port, String path, String body, long client, long seq)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
        if (client != 0) b.header("X-Client-Id", Long.toString(client)).header("X-Request-Seq", Long.toString(seq));
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    public void httpApiWithRedirectsFailoverAndRestart() throws Exception {
        Map<String, NodeAddress> cluster = new LinkedHashMap<>();
        for (int i = 1; i <= 3; i++) cluster.put("n" + i, new NodeAddress("127.0.0.1", freePort(), freePort()));
        Path base = Files.createTempDirectory("raftkv-tcp");
        Map<String, KvServer> servers = new HashMap<>();
        for (String id : cluster.keySet()) {
            KvServer s = new KvServer(id, cluster, base.resolve(id), RaftConfig.defaults());
            servers.put(id, s);
            s.start();
        }
        try {
            Check.eventually(() -> servers.values().stream()
                    .anyMatch(s -> s.node().status().role() == RaftNode.Role.LEADER), 5000, "leader elected");
            Check.sleep(400); // let followers learn who the leader is
            String leader = servers.values().stream().filter(s -> s.node().status().role() == RaftNode.Role.LEADER)
                    .findFirst().orElseThrow().node().id();
            String follower = cluster.keySet().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            int leaderPort = cluster.get(leader).httpPort(), followerPort = cluster.get(follower).httpPort();

            // A follower redirects clients to the leader.
            HttpResponse<String> r = call("PUT", followerPort, "/kv/greeting", "hello");
            Check.equal(307, r.statusCode(), "follower redirects");
            Check.isTrue(r.headers().firstValue("Location").orElse("").contains(":" + leaderPort + "/kv/greeting"),
                    "Location points at the leader");

            Check.equal(200, call("PUT", leaderPort, "/kv/greeting", "hello").statusCode(), "write via leader");
            r = call("GET", leaderPort, "/kv/greeting", null);
            Check.equal(200, r.statusCode(), "linearizable read");
            Check.equal("hello", r.body(), "read value");
            Check.equal(404, call("GET", leaderPort, "/kv/missing", null).statusCode(), "missing key");

            // DELETE says whether the key was there, also when its value is empty.
            Check.equal(200, call("PUT", leaderPort, "/kv/empty", "").statusCode(), "put an empty value");
            Check.equal(200, call("DELETE", leaderPort, "/kv/empty", null).statusCode(), "delete an empty value");
            Check.equal(404, call("DELETE", leaderPort, "/kv/empty", null).statusCode(), "delete it again");

            // Exactly once writes: a retry of the latest write is fine, an older write is refused, not acked.
            Check.equal(200, call("PUT", leaderPort, "/kv/seq", "five", 77, 5).statusCode(), "seq 5");
            Check.equal(200, call("PUT", leaderPort, "/kv/seq", "five", 77, 5).statusCode(), "retry of seq 5");
            Check.equal(409, call("PUT", leaderPort, "/kv/seq", "four", 77, 4).statusCode(), "older seq 4");
            Check.equal(409, call("DELETE", leaderPort, "/kv/seq", null, 77, 3).statusCode(), "older delete");
            Check.equal("five", call("GET", leaderPort, "/kv/seq", null).body(), "stale writes changed nothing");
            // A client id with no sequence number would make every write after the first a "retry" of it, skipped
            // but answered 200. It is refused instead, and so is a sequence number that is not positive.
            HttpRequest noSeq = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + leaderPort + "/kv/noseq"))
                    .header("X-Client-Id", "88").PUT(HttpRequest.BodyPublishers.ofString("a")).build();
            Check.equal(400, http.send(noSeq, HttpResponse.BodyHandlers.ofString()).statusCode(), "client id, no seq");
            Check.equal(400, call("PUT", leaderPort, "/kv/noseq", "b", 88, 0).statusCode(), "seq 0");
            Check.equal(400, call("PUT", leaderPort, "/kv/noseq", "b", 88, -1).statusCode(), "negative seq");
            Check.equal(404, call("GET", leaderPort, "/kv/noseq", null).statusCode(), "refused writes changed nothing");

            // Keys are decoded as URI paths: %20 is a space, but a plus sign stays a plus sign.
            Check.equal(200, call("PUT", leaderPort, "/kv/a+b", "plus").statusCode(), "put a+b");
            Check.equal(200, call("PUT", leaderPort, "/kv/a%20b", "space").statusCode(), "put a%20b");
            Check.equal("plus", call("GET", leaderPort, "/kv/a+b", null).body(), "a+b is its own key");
            Check.equal("space", call("GET", leaderPort, "/kv/a%20b", null).body(), "a%20b is the key 'a b'");
            Check.equal("space", new String(servers.get(leader).store().get("a b")), "decoded key in the store");

            // Values are capped, so one request cannot put an unbounded entry in the replicated log.
            String atCap = "x".repeat(KvServer.MAX_VALUE_BYTES);
            Check.equal(200, call("PUT", leaderPort, "/kv/big", atCap).statusCode(), "value at the cap");
            // The 413 must reach the client every time, not be lost to a reset because the body went unread.
            for (int i = 0; i < 20; i++)
                Check.equal(413, call("PUT", leaderPort, "/kv/big", atCap + "x").statusCode(), "value over the cap");
            Check.equal(atCap.length(), call("GET", leaderPort, "/kv/big", null).body().length(), "big value kept");
            // Keys are capped too: the JDK server takes a path of a few hundred KB, and every key is copied into
            // the log, every replica's map and the in memory index of each LSM table that holds it.
            String longestKey = "k".repeat(KvServer.MAX_KEY_BYTES);
            Check.equal(200, call("PUT", leaderPort, "/kv/" + longestKey, "v").statusCode(), "key at the cap");
            Check.equal("v", call("GET", leaderPort, "/kv/" + longestKey, null).body(), "key at the cap kept");
            Check.equal(414, call("PUT", leaderPort, "/kv/" + longestKey + "k", "v").statusCode(), "key over the cap");
            Check.equal(414, call("GET", leaderPort, "/kv/" + longestKey + "k", null).statusCode(), "get, too long");
            // Counted in UTF-8 bytes after decoding: 512 two byte characters fit, one more does not.
            String e9 = "%C3%A9";
            Check.equal(200, call("PUT", leaderPort, "/kv/" + e9.repeat(512), "v").statusCode(), "512 x e-acute");
            Check.equal(414, call("PUT", leaderPort, "/kv/" + e9.repeat(513), "v").statusCode(), "513 x e-acute");
            Check.equal(null, servers.get(leader).store().get("k".repeat(KvServer.MAX_KEY_BYTES + 1)), "nothing stored");
            Check.isTrue(call("GET", followerPort, "/status", null).body().contains("\"role\":\"FOLLOWER\""),
                    "status endpoint");

            // Kill the leader; the two survivors elect a new one and still have the data.
            servers.remove(leader).close();
            Check.eventually(() -> servers.values().stream()
                    .anyMatch(s -> s.node().status().role() == RaftNode.Role.LEADER), 5000, "new leader");
            String newLeader = servers.values().stream()
                    .filter(s -> s.node().status().role() == RaftNode.Role.LEADER).findFirst().orElseThrow().node().id();
            int newPort = cluster.get(newLeader).httpPort();
            Check.equal("hello", call("GET", newPort, "/kv/greeting", null).body(), "data survived failover");
            Check.equal(200, call("PUT", newPort, "/kv/after", "failover").statusCode(), "writes continue");

            // Restart the old leader from its data directory: it rejoins and catches up from disk plus the log.
            KvServer back = new KvServer(leader, cluster, base.resolve(leader), RaftConfig.defaults());
            servers.put(leader, back);
            back.start();
            Check.eventually(() -> back.store().get("after") != null, 5000, "restarted node caught up");
            Check.equal("hello", new String(back.store().get("greeting")), "restarted node has old data");
        } finally {
            servers.values().forEach(KvServer::close);
        }
    }
}
