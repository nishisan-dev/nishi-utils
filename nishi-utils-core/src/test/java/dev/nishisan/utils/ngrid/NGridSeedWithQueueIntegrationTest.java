package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.config.*;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
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
import java.util.concurrent.TimeUnit;

import static dev.nishisan.utils.ngrid.ClusterTestUtils.awaitClusterConsensus;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Teste de integração demonstrando o fluxo completo do NGrid com:
 * - Seed Node: Configurado via YAML com políticas de cluster e fila
 * - Producer Node: Produz mensagens na fila distribuída (configuração
 * programática)
 * - Consumer Node: Usa autodiscovery para configurar-se automaticamente e
 * consumir mensagens
 */
class NGridSeedWithQueueIntegrationTest {

    @TempDir
    Path tempDir;

    private NGridNode seedNode;
    private NGridNode producerNode;
    private NGridNode consumerNode;

    private Path seedConfigFile;
    private Path consumerConfigFile;

    private int seedPort;
    private int producerPort;
    private int consumerPort;

    @BeforeEach
    void setUp() throws Exception {
        // Allocate free ports
        seedPort = allocateFreeLocalPort();
        producerPort = allocateFreeLocalPort(Set.of(seedPort));
        consumerPort = allocateFreeLocalPort(Set.of(seedPort, producerPort));

        // === 1. Setup Seed Node Configuration (via YAML) ===
        seedConfigFile = tempDir.resolve("seed.yaml");

        NGridYamlConfig seedConfig = new NGridYamlConfig();

        // Node identity
        NodeIdentityConfig seedIdentity = new NodeIdentityConfig();
        seedIdentity.setId("seed-node");
        seedIdentity.setHost("127.0.0.1");
        seedIdentity.setPort(seedPort);
        NodeIdentityConfig.DirsConfig seedDirs = new NodeIdentityConfig.DirsConfig();
        seedDirs.setBase(tempDir.resolve("seed-data").toString());
        seedIdentity.setDirs(seedDirs);
        seedConfig.setNode(seedIdentity);

        // Autodiscover settings (Seed acts as server)
        AutodiscoverConfig seedAuto = new AutodiscoverConfig();
        seedAuto.setSecret("test-cluster-secret-2026");
        seedConfig.setAutodiscover(seedAuto);

        // Cluster Policy
        ClusterPolicyConfig clusterPolicy = new ClusterPolicyConfig();
        clusterPolicy.setName("message-cluster");
        ClusterPolicyConfig.ReplicationConfig rep = new ClusterPolicyConfig.ReplicationConfig();
        rep.setFactor(2); // 2 replicas (suitable for 3-node cluster)
        rep.setStrict(false); // Prioritize availability
        clusterPolicy.setReplication(rep);
        // CRITICAL: Include seed address so nodes can rejoin
        clusterPolicy.setSeeds(List.of("127.0.0.1:" + seedPort));
        seedConfig.setCluster(clusterPolicy);

        // Queue Policy
        QueuePolicyConfig queuePolicy = new QueuePolicyConfig();
        queuePolicy.setName("message-queue");
        QueuePolicyConfig.RetentionConfig ret = new QueuePolicyConfig.RetentionConfig();
        ret.setPolicy("TIME_BASED");
        ret.setDuration("1h");
        queuePolicy.setRetention(ret);
        seedConfig.setQueue(queuePolicy);

        NGridConfigLoader.save(seedConfigFile, seedConfig);

        // Start Seed Node
        seedNode = new NGridNode(seedConfigFile);
        seedNode.start();

        // === 2. Setup Producer Node (programmatic config, no YAML) ===
        NodeInfo seedInfo = new NodeInfo(
                seedNode.transport().local().nodeId(),
                "127.0.0.1",
                seedPort);

        NodeInfo producerInfo = new NodeInfo(
                dev.nishisan.utils.ngrid.common.NodeId.of("producer"),
                "127.0.0.1",
                producerPort);

        producerNode = new NGridNode(NGridConfig.builder(producerInfo)
                .addPeer(seedInfo)
                .queueDirectory(tempDir.resolve("producer-data"))
                .queueName("message-queue")
                .replicationQuorum(1)
                .transportWorkerThreads(4)
                .strictConsistency(false)
                .build());
        producerNode.start();

        // Wait for seed + producer to form cluster
        awaitClusterConsensus(seedNode, producerNode);

        // === 3. Setup Consumer Node Configuration (minimal YAML with autodiscover) ===
        consumerConfigFile = tempDir.resolve("consumer.yaml");

        NGridYamlConfig consumerConfig = new NGridYamlConfig();

        NodeIdentityConfig consumerIdentity = new NodeIdentityConfig();
        consumerIdentity.setId("consumer");
        consumerIdentity.setHost("127.0.0.1");
        consumerIdentity.setPort(consumerPort);
        NodeIdentityConfig.DirsConfig consumerDirs = new NodeIdentityConfig.DirsConfig();
        consumerDirs.setBase(tempDir.resolve("consumer-data").toString());
        consumerIdentity.setDirs(consumerDirs);
        consumerConfig.setNode(consumerIdentity);

        AutodiscoverConfig consumerAuto = new AutodiscoverConfig();
        consumerAuto.setEnabled(true);
        consumerAuto.setSecret("test-cluster-secret-2026");
        consumerAuto.setSeed("127.0.0.1:" + seedPort);
        consumerConfig.setAutodiscover(consumerAuto);

        NGridConfigLoader.save(consumerConfigFile, consumerConfig);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (consumerNode != null)
            consumerNode.close();
        if (producerNode != null)
            producerNode.close();
        if (seedNode != null)
            seedNode.close();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void seedWithProducerAndAutoConfiguredConsumer() throws Exception {
        // === PHASE 1: Producer enqueues messages ===
        DistributedQueue<String> producerQueue = producerNode.getQueue("message-queue", String.class);

        producerQueue.offer("msg-1");
        producerQueue.offer("msg-2");
        producerQueue.offer("msg-3");
        producerQueue.offer("msg-4");
        producerQueue.offer("msg-5");

        // Verify queue has 5 items
        Optional<String> peek = producerQueue.peek();
        assertTrue(peek.isPresent());
        assertEquals("msg-1", peek.get());

        // === PHASE 2: Start Consumer (autodiscovery) ===
        consumerNode = new NGridNode(consumerConfigFile);
        consumerNode.start();

        // Wait for consumer to join the cluster
        awaitClusterConsensus(seedNode, producerNode, consumerNode);

        // === PHASE 3: Verify Consumer YAML was updated ===
        NGridYamlConfig updatedConsumerConfig = NGridConfigLoader.load(consumerConfigFile);

        // Autodiscover should be disabled after successful bootstrap
        assertFalse(updatedConsumerConfig.getAutodiscover().isEnabled(),
                "Autodiscover should be disabled after successful bootstrap");

        // Cluster and queue policies should be present
        assertNotNull(updatedConsumerConfig.getCluster(), "Cluster config should be present");
        assertEquals("message-cluster", updatedConsumerConfig.getCluster().getName());
        assertEquals(2, updatedConsumerConfig.getCluster().getReplication().getFactor());

        assertNotNull(updatedConsumerConfig.getQueue(), "Queue config should be present");
        assertEquals("message-queue", updatedConsumerConfig.getQueue().getName());
        assertEquals("TIME_BASED", updatedConsumerConfig.getQueue().getRetention().getPolicy());

        // === PHASE 4: Verify messages are in queue and can be consumed ===
        // Get queue reference from consumer
        DistributedQueue<String> consumerQueue = consumerNode.getQueue("message-queue", String.class);

        // Verify we can see the enqueued messages
        assertEquals(Optional.of("msg-1"), consumerQueue.peek());

        // Consumer polls all 5 messages (will route to leader automatically)
        Optional<String> m1 = consumerQueue.poll();
        Optional<String> m2 = consumerQueue.poll();
        Optional<String> m3 = consumerQueue.poll();
        Optional<String> m4 = consumerQueue.poll();
        Optional<String> m5 = consumerQueue.poll();

        // Verify all messages were consumed
        assertTrue(m1.isPresent() && m1.get().equals("msg-1"));
        assertTrue(m2.isPresent() && m2.get().equals("msg-2"));
        assertTrue(m3.isPresent() && m3.get().equals("msg-3"));
        assertTrue(m4.isPresent() && m4.get().equals("msg-4"));
        assertTrue(m5.isPresent() && m5.get().equals("msg-5"));

        // === PHASE 5: Verify no more messages ===
        // A 6th poll should return empty
        Optional<String> noMore = consumerQueue.poll();
        assertFalse(noMore.isPresent(), "Queue should have no more messages after consuming all 5");

        // === PHASE 6: Verify cluster health ===
        // All nodes should agree on leader
        Optional<NodeInfo> seedLeader = seedNode.coordinator().leaderInfo();
        Optional<NodeInfo> producerLeader = producerNode.coordinator().leaderInfo();
        Optional<NodeInfo> consumerLeader = consumerNode.coordinator().leaderInfo();

        assertTrue(seedLeader.isPresent());
        assertEquals(seedLeader, producerLeader);
        assertEquals(seedLeader, consumerLeader);

        // All nodes should see 3 active members
        assertEquals(3, seedNode.coordinator().activeMembers().size());
        assertEquals(3, producerNode.coordinator().activeMembers().size());
        assertEquals(3, consumerNode.coordinator().activeMembers().size());
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
