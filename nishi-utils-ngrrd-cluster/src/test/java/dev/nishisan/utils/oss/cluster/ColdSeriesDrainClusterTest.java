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

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 180, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ColdSeriesDrainClusterTest {
    private NgrrdClusterTestHarness harness;

    @AfterEach
    void close() {
        if (harness != null) { harness.close(); }
    }

    @Test
    void drainAfterRestartAndDrainNewOwnerWithoutOpeningSeries(@TempDir Path base) throws Exception {
        String prefix = "tenant/archive";
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"))
                .replace("seriesPrefix: \"series\"", "seriesPrefix: \"" + prefix + "\"");
        harness = NgrrdClusterTestHarness.start(base, 3,
                builder -> builder.seriesObjectPrefix(prefix).rebalanceEnabled(false).rebalanceMinDelta(1L));
        harness.awaitNodeStatuses(3);
        try (NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5)).retryTimeout(Duration.ofSeconds(30)))) {
            String key;
            try (var handle = client.open(yaml, Map.of("deviceId", "cold", "interfaceId", "eth0"))) {
                key = handle.seriesKey();
                handle.write("in_octets", new Sample(1_700_000_100_000L, 1000));
                handle.checkpoint();
            }
            String physicalKey = prefix + "/" + key + ".ngrr";
            NgrrdStorageNode originalOwner = ownerOf(key);
            String originalId = originalOwner.node().transport().local().nodeId().value();
            byte[] image = originalOwner.volume().storage().get(physicalKey).orElseThrow();
            int index = harness.nodes().indexOf(originalOwner);
            NgrrdStorageNode restarted = harness.restartStorageNode(index);
            harness.awaitMeshStable();
            assertTrue(restarted.registry().cachedYaml(key).isEmpty());
            drain(client, originalId);
            NgrrdStorageNode nextOwner = ownerOf(key);
            assertNotEquals(originalId, nextOwner.node().transport().local().nodeId().value());
            assertArrayEquals(image, nextOwner.volume().storage().get(physicalKey).orElseThrow());
            assertTrue(nextOwner.registry().cachedYaml(key).isEmpty());
            String nextId = nextOwner.node().transport().local().nodeId().value();
            drain(client, nextId);
            NgrrdStorageNode finalOwner = ownerOf(key);
            assertNotEquals(nextId, finalOwner.node().transport().local().nodeId().value());
            assertArrayEquals(image, finalOwner.volume().storage().get(physicalKey).orElseThrow());
        }
    }

    private NgrrdStorageNode ownerOf(String key) {
        String owner = harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
        return harness.nodes().stream()
                .filter(n -> n.node().transport().local().nodeId().value().equals(owner)).findFirst().orElseThrow();
    }

    private void drain(NgrrdClusterClient client, String id) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        long nextTrigger = 0;
        while (System.nanoTime() < deadline) {
            if (System.nanoTime() >= nextTrigger) {
                client.drainNode(id);
                nextTrigger = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            }
            var catalog = harness.leaderNode().catalog();
            if (catalog.nodeStatusStrong(id).map(s -> s.state() == NodeState.DRAINED).orElse(false)
                    && catalog.seriesByOwnerLocal().getOrDefault(id, java.util.List.of()).isEmpty()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("drenagem não terminou para " + id);
    }
}
