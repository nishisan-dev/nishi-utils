package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class NGridIntegrationTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;

    private NodeInfo info1;
    private NodeInfo info2;
    private NodeInfo info3;

    private Path dir1;
    private Path dir2;
    private Path dir3;

    @BeforeEach
    void setUp() throws IOException {
        // Use ephemeral ports to avoid clashes on shared CI runners (e.g. GitHub Actions).
        // We still pass explicit ports to NodeInfo so peers can connect immediately.
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("node-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-test");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));
        dir3 = Files.createDirectories(baseDir.resolve("node3"));

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2)
                .addPeer(info3)
                .queueDirectory(dir1)
                .queueName("queue")
                .replicationQuorum(2)
                .build());
        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1)
                .addPeer(info3)
                .queueDirectory(dir2)
                .queueName("queue")
                .replicationQuorum(2)
                .build());
        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1)
                .addPeer(info2)
                .queueDirectory(dir3)
                .queueName("queue")
                .replicationQuorum(2)
                .build());

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (node1 != null) {
            node1.close();
        }
        if (node2 != null) {
            node2.close();
        }
        if (node3 != null) {
            node3.close();
        }
    }

    @Test
    void distributedStructuresMaintainStateAcrossClusterAndRestart() {
        DistributedQueue<String> queue1 = node1.queue(String.class);
        DistributedQueue<String> queue2 = node2.queue(String.class);
        DistributedQueue<String> queue3 = node3.queue(String.class);

        DistributedMap<String, String> map1 = node1.map(String.class, String.class);
        DistributedMap<String, String> map3 = node3.map(String.class, String.class);

        // Highest ID should become leader
        assertEquals("node-3", node1.coordinator().leaderInfo().map(info -> info.nodeId().value()).orElseThrow());

        queue3.offer("payload-1");
        queue3.offer("payload-2");

        Optional<String> peekFromFollower = queue1.peek();
        assertTrue(peekFromFollower.isPresent());
        assertEquals("payload-1", peekFromFollower.get());

        Optional<String> polledFromFollower = queue2.poll();
        assertTrue(polledFromFollower.isPresent());
        assertEquals("payload-1", polledFromFollower.get());

        Optional<String> remaining = queue3.peek();
        assertTrue(remaining.isPresent());
        assertEquals("payload-2", remaining.get());

        map3.put("shared-key", "value-1");
        Optional<String> fetched = map1.get("shared-key");
        assertTrue(fetched.isPresent());
        assertEquals("value-1", fetched.get());

        // Restart node1 using the same storage and ensure queue state is replayed
        closeQuietly(node1);
        waitForPortRelease(info1.port());
        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2)
                .addPeer(info3)
                .queueDirectory(dir1)
                .queueName("queue")
                .replicationQuorum(2)
                .build());
        node1.start();
        awaitClusterStability();

        DistributedQueue<String> restartedQueue = node1.queue(String.class);
        Optional<String> afterRestart = restartedQueue.peek();
        assertTrue(afterRestart.isPresent());
        assertEquals("payload-2", afterRestart.get());
    }

    @Test
    void multipleNamedMapsShouldReplicateIndependently() {
        // Ensure maps are created on all nodes (handlers are registered per-map)
        DistributedMap<String, String> users1 = node1.getMap("users", String.class, String.class);
        DistributedMap<String, String> users2 = node2.getMap("users", String.class, String.class);
        DistributedMap<String, String> users3 = node3.getMap("users", String.class, String.class);

        DistributedMap<String, String> sessions1 = node1.getMap("sessions", String.class, String.class);
        DistributedMap<String, String> sessions2 = node2.getMap("sessions", String.class, String.class);
        DistributedMap<String, String> sessions3 = node3.getMap("sessions", String.class, String.class);

        // Write on leader via follower reference and read from other nodes
        assertEquals("node-3", node1.coordinator().leaderInfo().map(info -> info.nodeId().value()).orElseThrow());

        users2.put("u1", "alice");
        assertEquals(Optional.of("alice"), users1.get("u1"));
        assertEquals(Optional.of("alice"), users3.get("u1"));

        sessions1.put("s1", "token-123");
        assertEventually(() -> assertEquals(Optional.of("token-123"), sessions2.get("s1")));
        assertEventually(() -> assertEquals(Optional.of("token-123"), sessions3.get("s1")));

        // Independence: keys from one map must not leak into the other
        assertFalse(users1.get("s1").isPresent());
        assertFalse(sessions1.get("u1").isPresent());
    }

    private void assertEventually(Runnable assertion) {
        long deadline = System.currentTimeMillis() + 5000;
        Throwable lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertion.run();
                return;
            } catch (AssertionError | Exception e) {
                lastError = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        if (lastError instanceof AssertionError) {
            throw (AssertionError) lastError;
        }
        throw new RuntimeException(lastError);
    }

    private void awaitClusterStability() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
        while (System.currentTimeMillis() < deadline) {
            boolean leadersAgree = node1.coordinator().leaderInfo().isPresent()
                    && node1.coordinator().leaderInfo().equals(node2.coordinator().leaderInfo())
                    && node1.coordinator().leaderInfo().equals(node3.coordinator().leaderInfo());
            boolean allMembers = node1.coordinator().activeMembers().size() == 3
                    && node2.coordinator().activeMembers().size() == 3
                    && node3.coordinator().activeMembers().size() == 3;
            if (leadersAgree && allMembers) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Cluster did not stabilize in time");
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (IOException ignored) {
        }
    }

    private static void waitForPortRelease(int port) {
        // After closing the ServerSocket, some environments (notably CI) may keep the port
        // unavailable for a short period due to TIME_WAIT. This helper waits until a bind
        // succeeds, using a small backoff, to reduce test flakiness.
        long[] delaysMs = {500, 1000, 1500, 2000, 2500};
        for (long delay : delaysMs) {
            if (isPortBindable(port)) {
                return;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        if (!isPortBindable(port)) {
            throw new IllegalStateException("Port " + port + " did not become available in time");
        }
    }

    private static boolean isPortBindable(int port) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        // Bind to port 0 on loopback to get a free ephemeral port from the OS.
        // Note: as with any free-port selection, there is a small race window between
        // release and re-bind, but this is far safer than hardcoded ports on CI.
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port after multiple attempts");
    }
}
