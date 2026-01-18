package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NGridAutodiscoverIntegrationTest {

    @TempDir
    Path tempDir;

    private NGridNode seedNode;
    private NGridNode newNode;
    private Path seedConfigFile;
    private Path newNodeConfigFile;
    private int newNodePort;

    @BeforeEach
    void setUp() throws IOException {
        // Setup Seed Node Configuration
        int seedPort = allocateFreeLocalPort();
        newNodePort = allocateFreeLocalPort(Set.of(seedPort));
        seedConfigFile = tempDir.resolve("seed.yaml");

        NGridYamlConfig seedConfig = new NGridYamlConfig();

        NodeIdentityConfig seedIdentity = new NodeIdentityConfig();
        seedIdentity.setId("seed-1");
        seedIdentity.setHost("127.0.0.1");
        seedIdentity.setPort(seedPort);
        NodeIdentityConfig.DirsConfig seedDirs = new NodeIdentityConfig.DirsConfig();
        seedDirs.setBase(tempDir.resolve("seed-data").toString());
        seedIdentity.setDirs(seedDirs);
        seedConfig.setNode(seedIdentity);

        // Autodiscover settings (Seed acts as server)
        AutodiscoverConfig seedAuto = new AutodiscoverConfig();
        seedAuto.setSecret("super-secret-token");
        seedConfig.setAutodiscover(seedAuto);

        // Cluster Policy to be distributed
        ClusterPolicyConfig clusterPolicy = new ClusterPolicyConfig();
        clusterPolicy.setName("test-cluster");
        ClusterPolicyConfig.ReplicationConfig rep = new ClusterPolicyConfig.ReplicationConfig();
        rep.setFactor(2); // Match cluster size to ensure stability
        rep.setStrict(true);
        clusterPolicy.setReplication(rep);
        // CRITICAL: The policy must include the seed address so the new node knows how
        // to rejoin!
        clusterPolicy.setSeeds(List.of("127.0.0.1:" + seedPort));
        seedConfig.setCluster(clusterPolicy);

        // Queue Policy to be distributed
        QueuePolicyConfig queuePolicy = new QueuePolicyConfig();
        queuePolicy.setName("global-queue");
        QueuePolicyConfig.RetentionConfig ret = new QueuePolicyConfig.RetentionConfig();
        ret.setPolicy("TIME_BASED");
        ret.setDuration("1h");
        queuePolicy.setRetention(ret);
        seedConfig.setQueue(queuePolicy);

        NGridConfigLoader.save(seedConfigFile, seedConfig);

        // Start Seed Node
        seedNode = new NGridNode(seedConfigFile);
        seedNode.start();

        // Setup New Node Configuration (Minimal)
        newNodeConfigFile = tempDir.resolve("new-node.yaml");

        NGridYamlConfig newConfig = new NGridYamlConfig();

        NodeIdentityConfig newIdentity = new NodeIdentityConfig();
        newIdentity.setId("new-node-1");
        newIdentity.setHost("127.0.0.1");
        newIdentity.setPort(newNodePort);
        NodeIdentityConfig.DirsConfig newDirs = new NodeIdentityConfig.DirsConfig();
        newDirs.setBase(tempDir.resolve("new-node-data").toString());
        newIdentity.setDirs(newDirs);
        newConfig.setNode(newIdentity);

        AutodiscoverConfig newAuto = new AutodiscoverConfig();
        newAuto.setEnabled(true);
        newAuto.setSecret("super-secret-token");
        newAuto.setSeed("127.0.0.1:" + seedPort);
        newConfig.setAutodiscover(newAuto);

        NGridConfigLoader.save(newNodeConfigFile, newConfig);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (newNode != null)
            newNode.close();
        if (seedNode != null)
            seedNode.close();
    }

    @Test
    @Timeout(30) // Prevent test from hanging indefinitely
    void testZeroTouchBootstrap() throws Exception {
        // Start New Node - this should trigger autodiscover
        newNode = new NGridNode(newNodeConfigFile);
        newNode.start();

        // 1. Verify Runtime State via Disk Config
        NGridYamlConfig updatedConfig = NGridConfigLoader.load(newNodeConfigFile);

        // Assert Autodiscover is disabled
        assertFalse(updatedConfig.getAutodiscover().isEnabled(),
                "Autodiscover should be disabled after successful bootstrap");

        // Assert Policies received
        assertNotNull(updatedConfig.getCluster(), "Cluster config should be present");
        assertEquals(2, updatedConfig.getCluster().getReplication().getFactor());
        assertEquals("global-queue", updatedConfig.getQueue().getName());

        // 2. Restart Node to prove persistence and join cluster
        newNode.close();

        // Give time for port release
        waitForPortRelease(newNodePort);

        // Re-open using the same file (which is now updated)
        newNode = new NGridNode(newNodeConfigFile);
        newNode.start();

        awaitClusterStability();

        // 3. Validate Functional Queue Behavior (using the policy-defined queue)
        DistributedQueue<String> newNodeQueue = newNode.getQueue("global-queue", String.class);
        DistributedQueue<String> seedNodeQueue = seedNode.getQueue("global-queue", String.class);

        newNodeQueue.offer("hello-from-new-node");

        // Wait for replication to seedNode
        awaitQueueReplication(seedNodeQueue, "hello-from-new-node");

        // Verify we can poll from seed (proving replication works)
        Optional<String> polled = seedNodeQueue.poll();
        assertTrue(polled.isPresent());
        assertEquals("hello-from-new-node", polled.get());

        // 4. Validate Map Behavior
        seedNode.getMap("test-map", String.class, String.class);
        DistributedMap<String, String> map = newNode.getMap("test-map", String.class, String.class);
        map.put("key1", "value1");

        // Verify replication to seed (eventual consistency)
        DistributedMap<String, String> seedMap = seedNode.getMap("test-map", String.class, String.class);
        awaitMapReplication(seedMap, "key1", "value1");
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

    private static void waitForPortRelease(int port) {
        long[] delaysMs = { 500, 1000, 1500, 2000, 2500 };
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

    private void awaitClusterStability() {
        long deadline = System.currentTimeMillis() + 20000;
        while (System.currentTimeMillis() < deadline) {
            boolean leadersAgree = seedNode.coordinator().leaderInfo().isPresent()
                    && seedNode.coordinator().leaderInfo().equals(newNode.coordinator().leaderInfo());
            boolean allMembers = seedNode.coordinator().activeMembers().size() == 2
                    && newNode.coordinator().activeMembers().size() == 2;
            boolean connected = seedNode.transport().isConnected(newNode.transport().local().nodeId())
                    && newNode.transport().isConnected(seedNode.transport().local().nodeId());
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

    private void awaitMapReplication(DistributedMap<String, String> map, String key, String expectedValue) {
        long deadline = System.currentTimeMillis() + 5000; // 5 seconds timeout
        while (System.currentTimeMillis() < deadline) {
            Optional<String> value = map.get(key);
            if (value.isPresent() && expectedValue.equals(value.get())) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        fail("Map replication did not complete in time. Expected: " + expectedValue + " for key: " + key);
    }

    private void awaitQueueReplication(DistributedQueue<String> queue, String expectedValue) {
        long deadline = System.currentTimeMillis() + 5000; // 5 seconds timeout
        while (System.currentTimeMillis() < deadline) {
            Optional<String> value = queue.peek();
            if (value.isPresent() && expectedValue.equals(value.get())) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        fail("Queue replication did not complete in time. Expected: " + expectedValue);
    }
}
