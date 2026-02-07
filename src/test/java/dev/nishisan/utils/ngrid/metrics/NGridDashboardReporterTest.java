package dev.nishisan.utils.ngrid.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NGridDashboardReporter}.
 */
class NGridDashboardReporterTest {

    @TempDir
    Path tempDir;

    private NGridOperationalSnapshot createSnapshot(long lag, int reachable, int total) {
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                Instant.now(),
                Map.of("node-1", 500L, "node-2", 300L),
                Map.of("node-1", 200L),
                Map.of("node-1", 100L),
                Map.of("node-1", 50L),
                Map.of(),
                Map.of(),
                Map.of("node-2", 2.5),
                Map.of());

        return new NGridOperationalSnapshot(
                "node-1", "node-1", 5L, 5L, total,
                true, true, 1000L,
                1000L, 1000L - lag, lag,
                3L, 1L, 0L, 8.5, 2,
                reachable, total, ioStats, Instant.now());
    }

    @Test
    void reportWritesYamlFile() throws Exception {
        Path outputFile = tempDir.resolve("dashboard.yaml");
        AtomicReference<NGridOperationalSnapshot> snapshot = new AtomicReference<>(createSnapshot(50, 3, 3));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            NGridDashboardReporter reporter = new NGridDashboardReporter(
                    snapshot::get, scheduler, outputFile, Duration.ofMinutes(5));

            // Manual report
            reporter.report();

            assertTrue(Files.exists(outputFile));
            String content = Files.readString(outputFile);

            // Verify key sections present
            assertTrue(content.contains("cluster:"));
            assertTrue(content.contains("replication:"));
            assertTrue(content.contains("health:"));
            assertTrue(content.contains("io:"));
            assertTrue(content.contains("localNodeId: \"node-1\""));
            assertTrue(content.contains("leaderId: \"node-1\""));
            assertTrue(content.contains("leaderEpoch: 5"));
            assertTrue(content.contains("replicationLag: 50"));
            assertTrue(content.contains("reachableNodesCount: 3"));
            assertTrue(content.contains("quorumRatio: 1.0"));

            reporter.close();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void toYamlReturnsString() {
        AtomicReference<NGridOperationalSnapshot> snapshot = new AtomicReference<>(createSnapshot(100, 2, 3));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            NGridDashboardReporter reporter = new NGridDashboardReporter(
                    snapshot::get, scheduler, tempDir.resolve("unused.yaml"), Duration.ofMinutes(5));

            String yaml = reporter.toYaml();
            assertNotNull(yaml);
            assertTrue(yaml.contains("replicationLag: 100"));
            assertTrue(yaml.contains("quorumRatio: 0.67"));

            reporter.close();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void buildDashboardStructure() {
        AtomicReference<NGridOperationalSnapshot> snapshot = new AtomicReference<>(createSnapshot(0, 3, 3));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            NGridDashboardReporter reporter = new NGridDashboardReporter(
                    snapshot::get, scheduler, tempDir.resolve("unused.yaml"), Duration.ofMinutes(5));

            Map<String, Object> dashboard = reporter.buildDashboard(snapshot.get());

            assertTrue(dashboard.containsKey("capturedAt"));
            assertTrue(dashboard.containsKey("cluster"));
            assertTrue(dashboard.containsKey("replication"));
            assertTrue(dashboard.containsKey("health"));
            assertTrue(dashboard.containsKey("io"));

            @SuppressWarnings("unchecked")
            Map<String, Object> cluster = (Map<String, Object>) dashboard.get("cluster");
            assertEquals("node-1", cluster.get("localNodeId"));
            assertEquals(true, cluster.get("isLeader"));
            assertEquals(5L, cluster.get("leaderEpoch"));

            @SuppressWarnings("unchecked")
            Map<String, Object> replication = (Map<String, Object>) dashboard.get("replication");
            assertEquals(0L, replication.get("replicationLag"));
            assertEquals(3L, replication.get("gapsDetected"));

            @SuppressWarnings("unchecked")
            Map<String, Object> health = (Map<String, Object>) dashboard.get("health");
            assertEquals(3, health.get("reachableNodesCount"));
            assertEquals(1.0, health.get("quorumRatio"));

            reporter.close();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void nullSnapshotDoesNotWriteFile() {
        Path outputFile = tempDir.resolve("dashboard-null.yaml");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            NGridDashboardReporter reporter = new NGridDashboardReporter(
                    () -> null, scheduler, outputFile, Duration.ofMinutes(5));

            reporter.report();
            assertFalse(Files.exists(outputFile));

            assertNull(reporter.toYaml());

            reporter.close();
        } finally {
            scheduler.shutdownNow();
        }
    }
}
