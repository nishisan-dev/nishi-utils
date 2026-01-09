package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

@Disabled("Unstable in CI/Virtual environments due to connection refusals")
class ReplicationSuccessIntegrationTest {

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

        Path baseDir = Files.createTempDirectory("ngrid-repl-test");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));
        dir3 = Files.createDirectories(baseDir.resolve("node3"));

        // Using quorum=2 to ensure data is on at least 2 nodes before we consider it "written"
        node1 = createNode(info1, dir1, Set.of(info2, info3));
        node2 = createNode(info2, dir2, Set.of(info1, info3));
        node3 = createNode(info3, dir3, Set.of(info1, info2));

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();
    }

    private NGridNode createNode(NodeInfo info, Path dir, Set<NodeInfo> peers) {
        NGridConfig.Builder builder = NGridConfig.builder(info)
                .queueDirectory(dir)
                .queueName("repl-queue")
                .replicationQuorum(2) // Majority of 3 is 2
                .strictConsistency(true) // Ensure we wait for acks
                .requestTimeout(java.time.Duration.ofSeconds(2)) // Fast fail for retries
                .connectTimeout(java.time.Duration.ofSeconds(1))
                .reconnectInterval(java.time.Duration.ofSeconds(1))
                .transportWorkerThreads(4); // Ignored by Virtual Threads but keeping for config
        peers.forEach(builder::addPeer);
        return new NGridNode(builder.build());
    }

    @AfterEach
    void tearDown() throws IOException {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void dataShouldSurviveLeaderDeath() throws Exception {
        // 1. Identify Leader
        Optional<NodeInfo> leaderOpt = node1.coordinator().leaderInfo();
        assertTrue(leaderOpt.isPresent(), "Leader should be present");
        String leaderId = leaderOpt.get().nodeId().value();

        System.out.println("Initial Leader: " + leaderId);

        // 2. Write data to the cluster
        DistributedQueue<String> queue1 = node1.queue(String.class);
        queue1.offer("critical-data-1");
        
        // Give some time for stability
        Thread.sleep(1000);
        
        // Retry logic for the second offer to tolerate transient network instability
        boolean offered = false;
        for (int i = 0; i < 3; i++) {
            try {
                queue1.offer("critical-data-2");
                offered = true;
                break;
            } catch (Exception e) {
                System.out.println("Offer failed attempt " + (i + 1) + ": " + e.getMessage());
                Thread.sleep(1000);
            }
        }
        if (!offered) {
            throw new RuntimeException("Failed to offer data after retries");
        }

        // Verify it is readable
        assertEquals("critical-data-1", queue1.peek().orElse(""));

        // 3. Kill the leader
        NGridNode leaderNode;
        if (leaderId.equals("node-1")) {
            leaderNode = node1;
            node1 = null;
        } else if (leaderId.equals("node-2")) {
            leaderNode = node2;
            node2 = null;
        } else {
            leaderNode = node3;
            node3 = null;
        }
        
        System.out.println("Killing leader: " + leaderId);
        leaderNode.close();

        // 4. Wait for re-election
        // The remaining nodes should elect a new leader.
        // We pick a survivor to check the state.
        NGridNode survivor = (node1 != null) ? node1 : node2;
        
        assertEventually(() -> {
            Optional<NodeInfo> newLeader = survivor.coordinator().leaderInfo();
            assertTrue(newLeader.isPresent());
            String newLeaderId = newLeader.get().nodeId().value();
            System.out.println("New Leader: " + newLeaderId);
            
            // Ensure the new leader is NOT the old dead one
            if (newLeaderId.equals(leaderId)) {
                throw new AssertionError("Old leader still reported as leader");
            }
        });

        // 5. Verify data is still there via a survivor
        DistributedQueue<String> survivorQueue = survivor.queue(String.class);
        
        // Retries might be needed as the new leader syncs/loads
        assertEventually(() -> {
             Optional<String> msg1 = survivorQueue.poll();
             assertTrue(msg1.isPresent(), "Should have message 1");
             assertEquals("critical-data-1", msg1.get());
             
             Optional<String> msg2 = survivorQueue.poll();
             assertTrue(msg2.isPresent(), "Should have message 2");
             assertEquals("critical-data-2", msg2.get());
        });
    }

    private void assertEventually(Runnable assertion) {
        long deadline = System.currentTimeMillis() + 10000;
        Throwable lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertion.run();
                return;
            } catch (AssertionError | Exception e) {
                lastError = e;
                try {
                    Thread.sleep(500);
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
            try {
                // Check if all active nodes agree on leader
                // and have connected to each other
                if (isStable()) return;
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Cluster did not stabilize in time");
    }

    private boolean isStable() {
        NGridNode[] nodes = {node1, node2, node3};
        NodeInfo leader = null;
        int activeCount = 0;

        for (NGridNode n : nodes) {
            if (n == null) continue; // might be closed
            activeCount++;
            Optional<NodeInfo> l = n.coordinator().leaderInfo();
            if (l.isEmpty()) return false;
            if (leader == null) leader = l.get();
            else if (!leader.equals(l.get())) return false;
            
            if (n.coordinator().activeMembers().size() != 3) return false;
        }
        return activeCount == 3;
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) return;
        try {
            node.close();
        } catch (IOException ignored) {}
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
        throw new IOException("Unable to allocate a free local port");
    }
}
