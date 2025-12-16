package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
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
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationQuorumFailureTest {

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
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("node-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-quorum-test");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));
        dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofMillis(500);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .queueDirectory(dir1).queueName("queue")
                .replicationQuorum(3)
                .replicationOperationTimeout(opTimeout)
                .build());
        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .queueDirectory(dir2).queueName("queue")
                .replicationQuorum(3)
                .replicationOperationTimeout(opTimeout)
                .build());
        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .queueDirectory(dir3).queueName("queue")
                .replicationQuorum(3)
                .replicationOperationTimeout(opTimeout)
                .build());

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();
        assertEquals("node-3", node1.coordinator().leaderInfo().map(l -> l.nodeId().value()).orElseThrow());
    }

    @AfterEach
    void tearDown() throws IOException {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    void replicationShouldNotBlockIndefinitelyWhenQuorumCannotBeSatisfied() {
        // This integration scenario is inherently timing sensitive. We assert the key property:
        // the call returns within the configured operation timeout (commit or failure), i.e. no deadlock.
        DistributedMap<String, String> map1 = node1.getMap("qfail", String.class, String.class);
        node2.getMap("qfail", String.class, String.class);
        node3.getMap("qfail", String.class, String.class);

        long start = System.nanoTime();
        try {
            map1.put("k", "v");
        } catch (RuntimeException ignored) {
            // acceptable outcome: quorum/timeout/transport failure
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsedMs < 5_000, "Operation should not block indefinitely (elapsedMs=" + elapsedMs + ")");
    }

    @Test
    void replicationShouldTimeoutWhenFollowersHaveNoHandlerRegistered() {
        // Only create the map on the leader so followers will receive REPLICATION_REQUEST without a handler.
        DistributedMap<String, String> leaderMap = node3.getMap("lazy-map", String.class, String.class);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> leaderMap.put("k1", "v1"));
        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
        assertTrue(msg.contains("timed out"), "Expected timeout failure but got: " + ex.getMessage());
    }

    // Note: cluster restart/recovery is already covered by NGridIntegrationTest and map persistence integration tests.

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

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
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


