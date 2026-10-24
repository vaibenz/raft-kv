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

    private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    private HttpResponse<String> call(String method, int port, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
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
