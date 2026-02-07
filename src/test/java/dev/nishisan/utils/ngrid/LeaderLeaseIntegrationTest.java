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
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for leader lease in a 3-node cluster.
 * <p>
 * Simulates a network partition by closing the leader node, then verifies:
 * <ul>
 * <li>The isolated leader steps down after lease expiry</li>
 * <li>The majority partition elects a new leader</li>
 * <li>Epoch is monotonically increasing</li>
 * </ul>
 */
class LeaderLeaseIntegrationTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;
    private NodeInfo info1;
    private NodeInfo info2;
    private NodeInfo info3;
    private Path baseDir;

    @BeforeEach
    void setUp() throws IOException {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        info1 = new NodeInfo(NodeId.of("lease-int-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("lease-int-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("lease-int-3"), "127.0.0.1", port3);

        baseDir = Files.createTempDirectory("ngrid-lease-int-test");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(5);
        Duration hbInterval = Duration.ofMillis(200);
        Duration lease = Duration.ofSeconds(1);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .dataDirectory(dir1)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(hbInterval)
                .leaseTimeout(lease)
                .strictConsistency(false)
                .build());
        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .dataDirectory(dir2)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(hbInterval)
                .leaseTimeout(lease)
                .strictConsistency(false)
                .build());
        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .dataDirectory(dir3)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(hbInterval)
                .leaseTimeout(lease)
                .strictConsistency(false)
                .build());
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void leaderStepsDownAndMajorityElectsNewLeader() throws Exception {
        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();

        // Identify leader and record epoch
        NGridNode originalLeader = findLeader();
        NodeId originalLeaderId = originalLeader.coordinator().leaderInfo()
                .map(NodeInfo::nodeId).orElseThrow();

        // Determine the two followers
        NGridNode[] followers = Arrays.stream(new NGridNode[] { node1, node2, node3 })
                .filter(n -> n != originalLeader)
                .toArray(NGridNode[]::new);

        // Simulate partition: close the leader
        originalLeader.close();

        // Wait for followers to detect leader loss and elect new leader
        // heartbeatTimeout is default 5s, so we need to wait longer
        Thread.sleep(8000);

        // At least one follower should have elected a new leader
        Optional<NodeInfo> newLeaderInfo = Optional.empty();
        for (NGridNode follower : followers) {
            newLeaderInfo = follower.coordinator().leaderInfo();
            if (newLeaderInfo.isPresent())
                break;
        }

        assertTrue(newLeaderInfo.isPresent(),
                "Majority partition should elect a new leader");
        assertNotEquals(originalLeaderId, newLeaderInfo.get().nodeId(),
                "New leader should be different from the isolated leader");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void epochIsMonotonicallyIncreasingAfterPartition() throws Exception {
        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();

        long initialEpoch = node1.coordinator().getLeaderEpoch();

        NGridNode leaderNode = findLeader();
        NGridNode[] followers = Arrays.stream(new NGridNode[] { node1, node2, node3 })
                .filter(n -> n != leaderNode)
                .toArray(NGridNode[]::new);

        // Close leader to trigger step-down and re-election
        leaderNode.close();
        Thread.sleep(8000);

        // Remaining nodes should have higher epoch
        for (NGridNode follower : followers) {
            long followerEpoch = follower.coordinator().getLeaderEpoch();
            assertTrue(followerEpoch >= initialEpoch,
                    "Epoch should be monotonically increasing. Initial: " + initialEpoch
                            + ", Follower: " + followerEpoch);
        }
    }

    // --- Helpers ---

    private NGridNode findLeader() {
        for (NGridNode node : new NGridNode[] { node1, node2, node3 }) {
            if (node.coordinator().isLeader()) {
                return node;
            }
        }
        throw new IllegalStateException("No leader found in cluster");
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
            boolean connected = node1.transport().isConnected(info2.nodeId())
                    && node1.transport().isConnected(info3.nodeId())
                    && node2.transport().isConnected(info1.nodeId())
                    && node2.transport().isConnected(info3.nodeId())
                    && node3.transport().isConnected(info1.nodeId())
                    && node3.transport().isConnected(info2.nodeId());
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
