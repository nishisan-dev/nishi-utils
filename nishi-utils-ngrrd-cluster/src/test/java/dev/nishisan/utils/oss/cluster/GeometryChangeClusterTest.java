package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 90, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class GeometryChangeClusterTest {
    @Test void confirmationIsInvalidUntilResizeFinishesAndReflectsPhysicalLayout(@TempDir Path directory) throws Exception {
        try (var cluster = NgrrdClusterTestHarness.start(directory, 1, builder -> builder
                .rebalanceEnabled(false).shardCount(1).segmentBytes(16L << 20).initialShardCapacityBytes(16L << 20))) {
            cluster.awaitNodeStatuses(1);
            var client = cluster.connectClient(builder -> { });
            String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"))
                    .replace("rows: 8640", "rows: 8").replace("rows: 4320", "rows: 4");
            Map<String, String> tags = Map.of("deviceId", "resize", "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            String key;
            try (var handle = client.open(yaml, tags)) { key = handle.seriesKey(); }
            var node = cluster.leaderNode();
            var original = node.catalog().placementStrong(key).orElseThrow();
            assertTrue(original.geometryConfirmed());
            var originalGeometry = node.catalog().geometryStrong(original.geometryId()).orElseThrow();
            String expanded = yaml.replace("metadata:\n", "metadata:\n  schemaRevision: 2\n")
                    .replace("rows: 8\n", "rows: 1000\n");
            var allocating = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            node.volume().storage().configureCapacity(0, () -> {
                allocating.countDown();
                try {
                    if (!release.await(15, TimeUnit.SECONDS)) { throw new IllegalStateException("allocation not released"); }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return Long.MAX_VALUE;
            });
            var resizing = CompletableFuture.runAsync(() -> {
                try (var handle = client.open(expanded, tags, Ngrrd.OpenOptions.onGeometryChange(OnGeometryChange.RECREATE))) {
                    assertEquals(key, handle.seriesKey());
                }
            });
            try {
                assertTrue(allocating.await(15, TimeUnit.SECONDS));
                var pending = node.catalog().placementStrong(key).orElseThrow();
                assertFalse(pending.geometryConfirmed());
                assertEquals(original.ownerNodeId(), pending.ownerNodeId());
                assertNull(pending.migrationId());
            } finally {
                release.countDown();
            }
            resizing.get(20, TimeUnit.SECONDS);
            var confirmed = node.catalog().placementStrong(key).orElseThrow();
            assertTrue(confirmed.geometryConfirmed());
            assertNotEquals(original.geometryId(), confirmed.geometryId());
            var actual = GeometryDescriptor.fromStaticSection(node.volume().storage()
                    .seriesStaticSection("series/" + key + ".ngrr").orElseThrow());
            assertEquals(actual, node.catalog().geometryStrong(confirmed.geometryId()).orElseThrow());
            assertTrue(actual.regionBytes() > originalGeometry.regionBytes());
            assertTrue(node.catalog().geometryStrong(original.geometryId()).isPresent());
        }
    }
}
