package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 180, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class WeightedGeometryClusterTest {
    @Test void weightedPlacementBackfillRebalanceAndPersistentGeometry(@TempDir Path directory) throws Exception {
        AtomicInteger index = new AtomicInteger();
        try (var cluster = NgrrdClusterTestHarness.start(directory, 3, builder -> builder
                .distributionMode(DistributionMode.WEIGHT).weight(index.incrementAndGet())
                .rebalanceEnabled(false).rebalanceMinDelta(0).rebalanceTolerance(0)
                .shardCount(2).segmentBytes(16L << 20).initialShardCapacityBytes(16L << 20))) {
            cluster.awaitNodeStatuses(3);
            var client = cluster.connectClient(builder -> builder.requestTimeout(Duration.ofSeconds(5)));
            String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
            for (int i = 0; i < 36; i++) {
                try (var handle = client.open(yaml, Map.of("deviceId", "weighted-" + i, "interfaceId", "eth0",
                        "region", "br-sp", "vendor", "x", "role", "core"))) {
                    assertNotNull(handle.seriesKey());
                }
            }
            cluster.awaitPlacements(36);
            var leader = cluster.leaderNode();
            var entries = leader.catalog().placementsLocal();
            assertTrue(entries.values().stream().allMatch(SeriesPlacement::geometryConfirmed));
            assertEquals(1, entries.values().stream().map(SeriesPlacement::geometryId).distinct().count());
            String geometryId = entries.values().iterator().next().geometryId();
            await(() -> cluster.nodes().stream().allMatch(n -> n.catalog().geometryLocal(geometryId).isPresent()));
            var before = leader.catalog().seriesByOwnerLocal();
            assertTrue(before.getOrDefault("storage-2", java.util.List.of()).size()
                    > before.getOrDefault("storage-0", java.util.List.of()).size());

            // Simulate a persisted pre-upgrade placement whose owner already has the physical series.
            String key = entries.keySet().iterator().next();
            var existing = entries.get(key);
            leader.catalog().putPlacement(key, new SeriesPlacement(existing.ownerNodeId(), null, PlacementState.ACTIVE,
                    null, existing.createdAtEpochMs(), existing.updatedAtEpochMs()));
            await(() -> cluster.leaderNode().catalog().placementStrong(key).orElseThrow().geometryConfirmed());

            for (int cycle = 0; cycle < 5; cycle++) {
                cluster.leaderNode().rebalancer().triggerNow();
                await(() -> cluster.leaderNode().catalog().placementsLocal().values().stream()
                        .allMatch(p -> p.state() == PlacementState.ACTIVE));
                var owners = cluster.leaderNode().catalog().seriesByOwnerLocal();
                if (owners.getOrDefault("storage-0", java.util.List.of()).size() == 6
                        && owners.getOrDefault("storage-1", java.util.List.of()).size() == 12
                        && owners.getOrDefault("storage-2", java.util.List.of()).size() == 18) { break; }
                Thread.sleep(300);
            }
            var balanced = cluster.leaderNode().catalog().seriesByOwnerLocal();
            assertEquals(6, balanced.get("storage-0").size());
            assertEquals(12, balanced.get("storage-1").size());
            assertEquals(18, balanced.get("storage-2").size());
            int leaderIndex = cluster.nodes().indexOf(cluster.leaderNode());
            cluster.restartStorageNode(leaderIndex);
            cluster.awaitNodeStatuses(3);
            assertTrue(cluster.leaderNode().catalog().geometryStrong(geometryId).isPresent());
            assertTrue(cluster.leaderNode().catalog().placementStrong(key).orElseThrow().geometryConfirmed());
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) { Thread.sleep(100); }
        assertTrue(condition.getAsBoolean());
    }
}
