/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.api.*;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CheckpointAfterMigrationClusterTest {
    @Test
    void existingHandlesRecoverOnTheirFirstOperationAfterMigration(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
        try (var harness = NgrrdClusterTestHarness.start(base, 2, builder -> builder.rebalanceEnabled(false))) {
            harness.awaitNodeStatuses(2);
            try (var client = harness.connectClient(builder -> builder.requestTimeout(Duration.ofSeconds(5))
                    .retryTimeout(Duration.ofSeconds(30)))) {
                for (String operation : new String[]{"checkpoint", "flush", "read", "readPreset"}) {
                    try (var handle = client.open(yaml, Map.of("deviceId", operation, "interfaceId", "eth0"))) {
                        String key = handle.seriesKey();
                        long start = 1_700_000_100_000L;
                        for (int i = 0; i < 4; i++) {
                            handle.write("in_octets", new Sample(start + i * 300_000L, 1000 + i * 1000));
                        }
                        handle.checkpoint();
                        var query = new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 500);
                        long end = start + 4 * 300_000L;
                        SeriesResult before = handle.read("in_bps", query, end);
                        assertTrue(before.points().stream().anyMatch(p -> Double.isFinite(p.value())));

                        String source = harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
                        NgrrdStorageNode destination = harness.nodes().stream()
                                .filter(n -> !n.node().transport().local().nodeId().value().equals(source))
                                .findFirst().orElseThrow();
                        String target = destination.node().transport().local().nodeId().value();
                        harness.awaitCatalogReplicaCaughtUp(target);
                        var result = harness.leaderNode().migrationCoordinator().migrate(key, source, target)
                                .get(60, TimeUnit.SECONDS);
                        assertEquals(MigrationOutcome.COMPLETED, result.outcome(), result.reason());
                        assertTrue(destination.registry().cachedYaml(key).isEmpty());

                        // Exactly one public call: external retries would hide issue #169.
                        switch (operation) {
                            case "checkpoint" -> assertDoesNotThrow(handle::checkpoint);
                            case "flush" -> assertDoesNotThrow(handle::flush);
                            case "read" -> assertEquals(before, handle.read("in_bps", query, end));
                            case "readPreset" -> assertTrue(handle.read("daily", end).containsKey("in_bps"));
                            default -> throw new AssertionError(operation);
                        }
                        assertTrue(destination.registry().isOpen(key));
                        assertEquals(before, handle.read("in_bps", query, end));
                        handle.write("in_octets", new Sample(end, 5000));
                        assertDoesNotThrow(handle::checkpoint);
                        assertTrue(handle.read("in_bps", query, end + 300_000L).points().stream()
                                .anyMatch(p -> p.tsEpochMs() >= end && Double.isFinite(p.value())));
                    }
                }
            }
        }
    }
}
