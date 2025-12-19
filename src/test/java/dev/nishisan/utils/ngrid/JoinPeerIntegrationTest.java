package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinPeerIntegrationTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;

    @BeforeEach
    void setUp() throws IOException {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2);
        NodeInfo info3 = new NodeInfo(NodeId.of("node-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-join-test");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(2);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .queueDirectory(dir1).queueName("queue")
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .build());
        node2 = new NGridNode(NGridConfig.builder(info2)
                .queueDirectory(dir2).queueName("queue")
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .build());
        node3 = new NGridNode(NGridConfig.builder(info3)
                .queueDirectory(dir3).queueName("queue")
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .build());

        node1.start();
        node2.start();
        node3.start();

        node1.join(info2);
        node2.join(info3);
    }

    @AfterEach
    void tearDown() throws IOException {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    void joinShouldDiscoverAllPeersThroughOneKnownNode() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            boolean node1Members = node1.coordinator().activeMembers().size() == 3;
            boolean node2Members = node2.coordinator().activeMembers().size() == 3;
            boolean node3Members = node3.coordinator().activeMembers().size() == 3;
            if (node1Members && node2Members && node3Members) {
                break;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        assertEquals(3, node1.coordinator().activeMembers().size());
        assertEquals(3, node2.coordinator().activeMembers().size());
        assertEquals(3, node3.coordinator().activeMembers().size());
        assertTrue(node1.coordinator().leaderInfo().isPresent());
        assertTrue(node2.coordinator().leaderInfo().isPresent());
        assertTrue(node3.coordinator().leaderInfo().isPresent());
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
