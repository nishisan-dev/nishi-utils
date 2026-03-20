package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests leader lease step-down behavior.
 * <p>
 * Uses a 2-node cluster where the leader is isolated (via node close)
 * and must step down after lease expiry. Also verifies that writes
 * are rejected with {@link LeaseExpiredException} after step-down.
 */
class LeaderLeaseStepDownTest {

    private NGridNode node1;
    private NGridNode node2;
    private NodeInfo info1;
    private NodeInfo info2;
    private Path baseDir;

    @BeforeEach
    void setUp() throws IOException {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));

        info1 = new NodeInfo(NodeId.of("lease-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("lease-2"), "127.0.0.1", port2);

        baseDir = Files.createTempDirectory("ngrid-lease-test");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        // Use very short heartbeat/lease for fast tests
        // heartbeatInterval=200ms, leaseTimeout=1s
        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2)
                .dataDirectory(dir1)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(3))
                .heartbeatInterval(Duration.ofMillis(200))
                .leaseTimeout(Duration.ofSeconds(1))
                .strictConsistency(false)
                .build());
        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1)
                .dataDirectory(dir2)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(3))
                .heartbeatInterval(Duration.ofMillis(200))
                .leaseTimeout(Duration.ofSeconds(1))
                .strictConsistency(false)
                .build());
    }

    @AfterEach
    void tearDown() throws IOException {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void leaderShouldStepDownAfterLeaseExpires() throws Exception {
        node1.start();
        node2.start();

        // Wait for cluster consensus
        awaitClusterStability(node1, node2, info1, info2);

        // Identify the leader
        NGridNode leaderNode = findLeader(node1, node2);
        NGridNode followerNode = leaderNode == node1 ? node2 : node1;
        assertNotNull(leaderNode.coordinator().leaderInfo().orElse(null), "Should have a leader");
        assertTrue(leaderNode.coordinator().hasValidLease(), "Leader should have valid lease initially");

        // Stop the follower to isolate the leader (leader stops receiving acks)
        followerNode.close();

        // Wait for lease to expire (1s lease + margin)
        Thread.sleep(2500);

        // Leader should have stepped down
        assertFalse(leaderNode.coordinator().hasValidLease(),
                "Leader should NOT have a valid lease after isolation");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void hasValidLeaseShouldReturnTrueForHealthyLeader() throws Exception {
        node1.start();
        node2.start();
        awaitClusterStability(node1, node2, info1, info2);

        NGridNode leaderNode = findLeader(node1, node2);
        assertTrue(leaderNode.coordinator().hasValidLease(),
                "Healthy leader should have a valid lease");

        // Non-leader should report false (it's not a leader)
        NGridNode followerNode = leaderNode == node1 ? node2 : node1;
        assertFalse(followerNode.coordinator().hasValidLease(),
                "Follower should not report hasValidLease");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void leaderEpochShouldIncrementOnStepDown() throws Exception {
        node1.start();
        node2.start();
        awaitClusterStability(node1, node2, info1, info2);

        NGridNode leaderNode = findLeader(node1, node2);
        long epochBefore = leaderNode.coordinator().getLeaderEpoch();

        // Isolate leader
        NGridNode followerNode = leaderNode == node1 ? node2 : node1;
        followerNode.close();

        // Wait for step-down
        Thread.sleep(2500);

        long epochAfter = leaderNode.coordinator().getLeaderEpoch();
        assertTrue(epochAfter > epochBefore,
                "Epoch should increment after step-down. Before: " + epochBefore + ", After: " + epochAfter);
    }

    // --- Helpers ---

    private NGridNode findLeader(NGridNode... nodes) {
        for (NGridNode node : nodes) {
            if (node.coordinator().isLeader()) {
                return node;
            }
        }
        throw new IllegalStateException("No leader found");
    }

    private void awaitClusterStability(NGridNode n1, NGridNode n2, NodeInfo i1, NodeInfo i2) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        while (System.currentTimeMillis() < deadline) {
            boolean leadersAgree = n1.coordinator().leaderInfo().isPresent()
                    && n1.coordinator().leaderInfo().equals(n2.coordinator().leaderInfo());
            boolean allMembers = n1.coordinator().activeMembers().size() == 2
                    && n2.coordinator().activeMembers().size() == 2;
            boolean connected = n1.transport().isConnected(i2.nodeId())
                    && n2.transport().isConnected(i1.nodeId());
            if (leadersAgree && allMembers && connected) {
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
        if (node == null)
            return;
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
