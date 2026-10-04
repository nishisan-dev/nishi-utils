package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.*;
import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.protocol.*;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(180)
class SeriesDeleteClusterTest {
    @TempDir Path base;
    private static Map<String, String> tags(String device) {
        return Map.of("deviceId", device, "interfaceId", "eth0", "region", "br-sp", "vendor", "x", "role", "core");
    }
    private static String key(String device) { return "device:" + device + "/iface:eth0"; }
    private String yaml() throws Exception { return Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml")); }
    private NgrrdStorageNode owner(NgrrdClusterTestHarness h, String key) {
        String id = h.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
        return h.nodes().stream().filter(n -> n.config().nodeId().equals(id)).findFirst().orElseThrow();
    }
    @Test void deletionBatchGenerationAndRestart() throws Exception {
        var migrationReady = new CountDownLatch(1);
        var releaseMigration = new CountDownLatch(1);
        var hook = new MigrationCoordinator.MigrationHooks() {
            @Override public void beforeStart(String seriesKey, String migrationId) {
                migrationReady.countDown();
                try {
                    if (!releaseMigration.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("migration test release timed out");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
        try (var h = NgrrdClusterTestHarness.start(base, 3, b -> b.rebalanceEnabled(false), index -> hook)) {
            h.awaitLeader(); h.awaitMeshStable(); h.awaitNodeStatuses(3);
            var client = h.connectClient(b -> b.retryTimeout(Duration.ofSeconds(30)));
            var producer = h.connectClient(b -> b.retryTimeout(Duration.ofSeconds(30)));
            NgrrdHandle deleted = producer.open(yaml(), tags("deleted"));
            NgrrdHandle recent = producer.open(yaml(), tags("recent"));
            NgrrdHandle migrating = producer.open(yaml(), tags("migrating"));
            long now = System.currentTimeMillis();
            deleted.write("in_octets", new Sample(now, 10)); deleted.checkpoint();
            recent.write("in_octets", new Sample(now - Duration.ofDays(40).toMillis(), 20)); recent.checkpoint();
            NgrrdStorageNode storage = owner(h, key("deleted"));
            var oldPlacement = h.leaderNode().catalog().placementStrong(key("deleted")).orElseThrow();
            byte[] duplicate = storage.volume().storage().get("series/" + key("deleted") + ".ngrr").orElseThrow();
            for (var copy : h.nodes()) if (copy != storage) copy.volume().storage().put("series/" + key("deleted") + ".ngrr", duplicate);
            long used = Arrays.stream(storage.volume().stats().shardUsedBytes()).sum();
            long cutoff = storage.registry().lifecycle().journal().get(key("deleted")).receivedThrough() + 1;
            var migrationPlacement = h.leaderNode().catalog().placementStrong(key("migrating")).orElseThrow();
            String target = h.nodes().stream().map(n -> n.config().nodeId()).filter(id -> !id.equals(migrationPlacement.ownerNodeId())).findFirst().orElseThrow();
            h.awaitCatalogReplicaCaughtUp(target);
            var migration = h.leaderNode().migrationCoordinator().migrate(key("migrating"), migrationPlacement.ownerNodeId(), target);
            assertTrue(migrationReady.await(20, TimeUnit.SECONDS), "real migration must reserve its placement before deletion");
            Map<String, DeleteResult> results;
            try {
                results = client.deleteSeriesBatch(new LinkedHashMap<>(Map.of(
                        key("deleted"), new DeletePrecondition(cutoff), key("recent"), new DeletePrecondition(now),
                        key("migrating"), new DeletePrecondition(cutoff), "absent", new DeletePrecondition(now))));
                // Measure reclamation before the unrelated migration can allocate in this volume.
                assertFalse(storage.volume().storage().exists("series/" + key("deleted") + ".ngrr"));
                assertTrue(Arrays.stream(storage.volume().stats().shardUsedBytes()).sum() < used);
            } finally {
                releaseMigration.countDown();
            }
            assertEquals(DeleteStatus.DELETED, results.get(key("deleted")).status(), results.toString());
            assertEquals(DeleteStatus.REFUSED_RECENT_WRITE, results.get(key("recent")).status());
            assertEquals(DeleteStatus.REFUSED_MIGRATING, results.get(key("migrating")).status());
            assertEquals(DeleteStatus.NOT_FOUND, results.get("absent").status());
            assertEquals(MigrationCoordinator.MigrationOutcome.COMPLETED, migration.get(60, TimeUnit.SECONDS).outcome());
            for (var copy : h.nodes()) assertFalse(copy.volume().storage().exists("series/" + key("deleted") + ".ngrr"));
            assertFalse(client.exists(key("deleted")));
            assertTrue(h.leaderNode().catalog().placementStrong(key("deleted")).isEmpty());
            assertEquals(DeleteStatus.NOT_FOUND, client.deleteSeries(key("deleted"), new DeletePrecondition(cutoff)).status());
            var stale = assertThrows(NgrrdClusterException.class, deleted::checkpoint);
            assertEquals(ErrorCode.SERIES_DELETED, stale.code());
            assertEquals(ErrorCode.SERIES_DELETED, assertThrows(NgrrdClusterException.class,
                    () -> deleted.write("in_octets", new Sample(now + 1, 11))).code());
            int index = h.nodes().indexOf(storage);
            h.restartStorageNode(index); h.awaitMeshStable(); h.awaitNodeStatuses(3);
            assertTrue(h.leaderNode().catalog().placementStrong(key("deleted")).isEmpty());
            assertFalse(h.nodes().get(index).volume().storage().exists("series/" + key("deleted") + ".ngrr"));
            var recreated = producer.open(yaml(), tags("deleted"));
            recreated.write("in_octets", new Sample(now + 300_000, 100)); recreated.checkpoint();
            assertNotEquals(oldPlacement.generationId(), h.leaderNode().catalog().placementStrong(key("deleted")).orElseThrow().generationId());
            assertEquals(ErrorCode.SERIES_DELETED, assertThrows(NgrrdClusterException.class, deleted::checkpoint).code());
        }
    }
    @Test void lostCatalogQuarantinesAndExplicitAdoptionPreservesData() throws Exception {
        try (var h = NgrrdClusterTestHarness.start(base, 3, b -> b.rebalanceEnabled(false))) {
            h.awaitLeader(); h.awaitMeshStable(); h.awaitNodeStatuses(3);
            var client = h.connectClient(b -> b.retryTimeout(Duration.ofSeconds(20)));
            var handle = client.open(yaml(), tags("quarantine"));
            handle.write("in_octets", new Sample(System.currentTimeMillis(), 42)); handle.checkpoint();
            var storage = owner(h, key("quarantine"));
            handle.close();
            byte[] before = storage.volume().storage().get("series/" + key("quarantine") + ".ngrr").orElseThrow();
            h.leaderNode().catalog().removePlacement(key("quarantine"));
            for (var node : h.nodes()) {
                node.localReconciler().reconcileOnce(); node.localReconciler().reconcileOnce();
            }
            assertEquals(ErrorCode.QUARANTINED, assertThrows(NgrrdClusterException.class,
                    () -> client.open(yaml(), tags("quarantine"))).code());
            assertTrue(h.leaderNode().catalog().placementStrong(key("quarantine")).isEmpty());
            var report = client.reconcile(storage.config().nodeId(), new ReconcileRequest(ReconcileRequest.Action.REPORT, null));
            assertEquals("QUARANTINED", report.results().get(key("quarantine")));
            assertTrue(report.quarantinedBytes() > 0);
            for (var node : h.nodes()) client.reconcile(node.config().nodeId(), new ReconcileRequest(ReconcileRequest.Action.ADOPT, null));
            assertArrayEquals(before, storage.volume().storage().get("series/" + key("quarantine") + ".ngrr").orElseThrow());
            var restored = client.open(yaml(), tags("quarantine"));
            restored.write("in_octets", new Sample(System.currentTimeMillis() + 300_000, 52)); restored.checkpoint();
            assertTrue(h.leaderNode().catalog().placementStrong(key("quarantine")).isPresent());
        }
    }
}
