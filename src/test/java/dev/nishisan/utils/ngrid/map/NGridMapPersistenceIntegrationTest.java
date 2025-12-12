package dev.nishisan.utils.ngrid.map;

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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NGridMapPersistenceIntegrationTest {

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

        Path baseDir = Files.createTempDirectory("ngrid-map-persist");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));
        dir3 = Files.createDirectories(baseDir.resolve("node3"));

        node1 = new NGridNode(configFor(info1, dir1, info2, info3));
        node2 = new NGridNode(configFor(info2, dir2, info1, info3));
        node3 = new NGridNode(configFor(info3, dir3, info1, info2));

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();
    }

    @AfterEach
    void tearDown() throws IOException {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    void mapShouldRecoverAfterFullClusterRestartWhenPersistenceEnabled() throws IOException {
        DistributedMap<String, String> map = node1.map(String.class, String.class);

        // Highest ID should become leader
        assertEquals("node-3", node1.coordinator().leaderInfo().map(info -> info.nodeId().value()).orElseThrow());

        map.put("shared-key", "value-1");
        assertEquals(Optional.of("value-1"), map.get("shared-key"));

        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);

        node1 = new NGridNode(configFor(info1, dir1, info2, info3));
        node2 = new NGridNode(configFor(info2, dir2, info1, info3));
        node3 = new NGridNode(configFor(info3, dir3, info1, info2));

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();

        DistributedMap<String, String> mapAfterRestart = node2.map(String.class, String.class);
        Optional<String> recovered = mapAfterRestart.get("shared-key");
        assertTrue(recovered.isPresent());
        assertEquals("value-1", recovered.get());
    }

    private static NGridConfig configFor(NodeInfo local, Path dir, NodeInfo... peers) {
        NGridConfig.Builder b = NGridConfig.builder(local)
                .queueDirectory(dir)
                .queueName("queue")
                .replicationQuorum(2)
                .mapDirectory(dir.resolve("maps"))
                .mapName("map")
                .mapPersistenceMode(MapPersistenceMode.ASYNC_WITH_FSYNC);
        for (NodeInfo peer : peers) {
            b.addPeer(peer);
        }
        return b.build();
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


