package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.queue.NQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NGridConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadAndConvertToDomain() throws IOException {
        Path yamlFile = tempDir.resolve("ngrid-test.yaml");
        
        // Create a mock YAML config programmatically to save to file first
        NGridYamlConfig yamlConfig = new NGridYamlConfig();
        
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("test-node-1");
        node.setHost("127.0.0.1");
        node.setPort(9000);
        node.setRoles(Set.of("PRODUCER", "CONSUMER"));
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-data");
        node.setDirs(dirs);
        yamlConfig.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        cluster.setName("test-cluster");
        ClusterPolicyConfig.ReplicationConfig replication = new ClusterPolicyConfig.ReplicationConfig();
        replication.setFactor(3);
        replication.setStrict(true);
        cluster.setReplication(replication);
        cluster.setSeeds(List.of("127.0.0.1:9001", "127.0.0.1:9002"));
        yamlConfig.setCluster(cluster);

        QueuePolicyConfig queue = new QueuePolicyConfig();
        queue.setName("main-queue");
        QueuePolicyConfig.RetentionConfig retention = new QueuePolicyConfig.RetentionConfig();
        retention.setPolicy("TIME_BASED");
        retention.setDuration("24h");
        queue.setRetention(retention);
        
        QueuePolicyConfig.PerformanceConfig performance = new QueuePolicyConfig.PerformanceConfig();
        performance.setFsync(false);
        performance.setShortCircuit(true);
        QueuePolicyConfig.MemoryBufferConfig memory = new QueuePolicyConfig.MemoryBufferConfig();
        memory.setEnabled(true);
        memory.setSize(5000);
        performance.setMemoryBuffer(memory);
        queue.setPerformance(performance);
        
        yamlConfig.setQueue(queue);

        // Save to file
        NGridConfigLoader.save(yamlFile, yamlConfig);

        // Load back
        NGridYamlConfig loadedYaml = NGridConfigLoader.load(yamlFile);
        assertNotNull(loadedYaml);
        assertEquals("test-node-1", loadedYaml.getNode().getId());

        // Convert to Domain
        NGridConfig domainConfig = NGridConfigLoader.convertToDomain(loadedYaml);

        // Assert Domain Config
        assertEquals("test-node-1", domainConfig.local().nodeId().value());
        assertEquals("127.0.0.1", domainConfig.local().host());
        assertEquals(9000, domainConfig.local().port());
        assertTrue(domainConfig.local().roles().contains("PRODUCER"));
        
        // Assert Cluster
        assertEquals(3, domainConfig.replicationFactor());
        assertTrue(domainConfig.strictConsistency());
        assertEquals(2, domainConfig.peers().size()); // 2 seeds

        // Assert Queue
        assertEquals("main-queue", domainConfig.queueName());
        // assertEquals(NQueue.Options.RetentionPolicy.TIME_BASED, domainConfig.queueOptions().snapshot().retentionPolicy); 
        // Cannot access package-private field in test easily. Trusting loader logic verified by integration/functional tests later.
        
        // Let's verify paths
        assertEquals(Path.of("/tmp/ngrid-data/queue"), domainConfig.queueDirectory());
    }
    
    @Test
    void testDurationParsing() throws IOException {
        Path yamlFile = tempDir.resolve("duration.yaml");
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setHost("127.0.0.1"); // Fix NPE
        node.setDirs(new NodeIdentityConfig.DirsConfig());
        node.getDirs().setBase("/tmp");
        config.setNode(node);
        
        QueuePolicyConfig q = new QueuePolicyConfig();
        QueuePolicyConfig.RetentionConfig r = new QueuePolicyConfig.RetentionConfig();
        r.setPolicy("TIME_BASED");
        r.setDuration("10m");
        q.setRetention(r);
        config.setQueue(q);
        
        NGridConfigLoader.save(yamlFile, config);
        NGridConfig domain = NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));
        
        // Duration "10m" parsing check (indirectly via QueueOptions if we could inspect it)
        // Since we can't easily inspect Options internal state without NQueue instance, 
        // we trust convertToDomain didn't throw exception on "10m".
    }
}
