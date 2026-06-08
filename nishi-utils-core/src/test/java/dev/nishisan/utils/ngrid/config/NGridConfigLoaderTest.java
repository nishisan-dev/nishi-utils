package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.ngrid.replication.FollowerIngestMode;
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

        // Assert Queues - NEW API
        assertNotNull(domainConfig.queues());
        assertEquals(1, domainConfig.queues().size());
        assertEquals("main-queue", domainConfig.queues().get(0).name());

        // Let's verify paths
        assertEquals(Path.of("/tmp/ngrid-data"), domainConfig.dataDirectory());
    }

    @Test
    void followerIngestModeFromYaml() throws IOException {
        // unset -> INLINE (default preserved)
        assertEquals(FollowerIngestMode.INLINE, loadWithIngestMode(null).followerIngestMode());
        // explicit, case-insensitive -> RELAY_LOG
        assertEquals(FollowerIngestMode.RELAY_LOG, loadWithIngestMode("relay_log").followerIngestMode());
        // invalid -> falls back to INLINE (tolerant parse)
        assertEquals(FollowerIngestMode.INLINE, loadWithIngestMode("bogus").followerIngestMode());
    }

    private NGridConfig loadWithIngestMode(String ingest) throws IOException {
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setHost("127.0.0.1");
        node.setDirs(new NodeIdentityConfig.DirsConfig());
        node.getDirs().setBase("/tmp");
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        ClusterPolicyConfig.ReplicationConfig replication = new ClusterPolicyConfig.ReplicationConfig();
        replication.setFollowerIngestMode(ingest);
        cluster.setReplication(replication);
        config.setCluster(cluster);

        QueuePolicyConfig queue = new QueuePolicyConfig();
        queue.setName("ingest-queue");
        config.setQueue(queue);

        Path yamlFile = tempDir.resolve("ingest-" + (ingest == null ? "default" : ingest) + ".yaml");
        NGridConfigLoader.save(yamlFile, config);
        return NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));
    }

    @Test
    void binlogSegmentRetentionFromYaml() throws IOException {
        Path yamlFile = tempDir.resolve("binlog-retention.yaml");
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setHost("127.0.0.1");
        node.setDirs(new NodeIdentityConfig.DirsConfig());
        node.getDirs().setBase("/tmp");
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        ClusterPolicyConfig.ReplicationConfig replication = new ClusterPolicyConfig.ReplicationConfig();
        replication.setFollowerIngestMode("relay_stream");
        replication.setResendLogSegmentMaxBytes(10L * 1024 * 1024 * 1024); // 10GB per file
        replication.setResendLogMaxSegments(10);                            // keep 10 files
        replication.setRelayExpireAfterWrite("30m");                        // follower relay TTL
        cluster.setReplication(replication);
        config.setCluster(cluster);

        QueuePolicyConfig queue = new QueuePolicyConfig();
        queue.setName("binlog-queue");
        config.setQueue(queue);

        // Round-trip through YAML to exercise Jackson (de)serialization.
        NGridConfigLoader.save(yamlFile, config);
        NGridConfig domain = NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));

        assertEquals(10L * 1024 * 1024 * 1024, domain.resendLogSegmentMaxBytes());
        assertEquals(10, domain.resendLogMaxSegments());
        assertEquals(Duration.ofMinutes(30), domain.relayExpireAfterWrite());
    }

    @Test
    void binlogSegmentRetentionUnsetFromYamlStaysNull() {
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setHost("127.0.0.1");
        node.setDirs(new NodeIdentityConfig.DirsConfig());
        node.getDirs().setBase("/tmp");
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        cluster.setReplication(new ClusterPolicyConfig.ReplicationConfig());
        config.setCluster(cluster);

        NGridConfig domain = NGridConfigLoader.convertToDomain(config);

        assertNull(domain.resendLogSegmentMaxBytes(),
                "absent YAML keys must leave the binlog byte cap unset (replication default applies)");
        assertNull(domain.resendLogMaxSegments(),
                "absent YAML keys must leave the segment-count cap unset");
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
        q.setName("test-queue"); // Add queue name
        QueuePolicyConfig.RetentionConfig r = new QueuePolicyConfig.RetentionConfig();
        r.setPolicy("TIME_BASED");
        r.setDuration("10m");
        q.setRetention(r);
        config.setQueue(q);

        NGridConfigLoader.save(yamlFile, config);
        NGridConfig domain = NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));

        // Duration "10m" parsing check (indirectly via QueueOptions if we could inspect
        // it)
        // Since we can't easily inspect Options internal state without NQueue instance,
        // we trust convertToDomain didn't throw exception on "10m".
    }

    @Test
    void shouldBindOutboundQueueCapacityFromTransportYaml() throws IOException {
        Path yamlFile = tempDir.resolve("outbound-capacity.yaml");
        NGridYamlConfig config = new NGridYamlConfig();

        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("node-1");
        node.setHost("127.0.0.1");
        node.setPort(9000);
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-outbound-test");
        node.setDirs(dirs);
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        ClusterPolicyConfig.TransportConfig transport = new ClusterPolicyConfig.TransportConfig();
        transport.setWorkers(4);
        transport.setOutboundQueueCapacity(10_000);
        cluster.setTransport(transport);
        config.setCluster(cluster);

        // Round-trip through YAML to exercise Jackson (de)serialization.
        NGridConfigLoader.save(yamlFile, config);
        NGridConfig domain = NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));

        assertEquals(10_000, domain.outboundQueueCapacity());
        assertEquals(4, domain.transportWorkerThreads());
    }

    @Test
    void shouldDefaultOutboundQueueCapacityToUnboundedWhenAbsent() {
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("node-1");
        node.setHost("127.0.0.1");
        node.setPort(9000);
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-outbound-test");
        node.setDirs(dirs);
        config.setNode(node);

        NGridConfig domain = NGridConfigLoader.convertToDomain(config);

        assertEquals(0, domain.outboundQueueCapacity());
    }

    @Test
    void shouldBindTransportCompressionFromYaml() throws IOException {
        Path yamlFile = tempDir.resolve("compression.yaml");
        NGridYamlConfig config = new NGridYamlConfig();

        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("node-1");
        node.setHost("127.0.0.1");
        node.setPort(9000);
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-compression-test");
        node.setDirs(dirs);
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        ClusterPolicyConfig.TransportConfig transport = new ClusterPolicyConfig.TransportConfig();
        ClusterPolicyConfig.CompressionConfig compression = new ClusterPolicyConfig.CompressionConfig();
        compression.setEnabled(false);
        compression.setMinSize(2048);
        transport.setCompression(compression);
        cluster.setTransport(transport);
        config.setCluster(cluster);

        // Round-trip through YAML to exercise Jackson (de)serialization.
        NGridConfigLoader.save(yamlFile, config);
        NGridConfig domain = NGridConfigLoader.convertToDomain(NGridConfigLoader.load(yamlFile));

        assertFalse(domain.transportCompressionEnabled());
        assertEquals(2048, domain.transportCompressionMinSize());
    }

    @Test
    void shouldDefaultTransportCompressionEnabledWhenSectionAbsent() {
        NGridYamlConfig config = new NGridYamlConfig();
        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("node-1");
        node.setHost("127.0.0.1");
        node.setPort(9000);
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-compression-test");
        node.setDirs(dirs);
        config.setNode(node);

        NGridConfig domain = NGridConfigLoader.convertToDomain(config);

        assertTrue(domain.transportCompressionEnabled());
        assertEquals(512, domain.transportCompressionMinSize());
    }

    @Test
    void shouldNotDuplicateSeedPrefixWhenSeedHostAlreadyHasPrefix() {
        NGridYamlConfig config = new NGridYamlConfig();

        NodeIdentityConfig node = new NodeIdentityConfig();
        node.setId("node-2");
        node.setHost("node-2");
        node.setPort(9000);
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase("/tmp/ngrid-config-loader-test");
        node.setDirs(dirs);
        config.setNode(node);

        ClusterPolicyConfig cluster = new ClusterPolicyConfig();
        cluster.setSeeds(List.of("seed-1:9000"));
        config.setCluster(cluster);

        NGridConfig domain = NGridConfigLoader.convertToDomain(config);

        assertEquals(1, domain.peers().size());
        var peer = domain.peers().iterator().next();
        assertEquals("seed-1", peer.nodeId().value());
        assertEquals("seed-1", peer.host());
        assertEquals(9000, peer.port());
    }
}
