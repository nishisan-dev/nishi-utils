/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Content-level regressions for #212, using real TCP and the same identity/data directory. */
class VolatileMapCleanRestartTest {
    private static final String MAP = "entries";
    @TempDir Path directory;
    private NGridNode survivor;
    private NGridNode preferred;

    @AfterEach
    void close() throws Exception {
        try {
            if (preferred != null) preferred.close();
        } finally {
            if (survivor != null) survivor.close();
        }
    }

    @ParameterizedTest(name = "delta={0}, initial={1}, restarted={2}")
    @CsvSource({
            "false, DISABLED, DISABLED",
            "true, DISABLED, DISABLED",
            "true, DISABLED, ASYNC_WITH_FSYNC",
            "true, ASYNC_WITH_FSYNC, ASYNC_WITH_FSYNC"
    })
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void cleanRestartRecoversPrefixBeforeLeading(boolean delta, NMapPersistenceMode initial,
                                                 NMapPersistenceMode restarted) throws Exception {
        NodeInfo low = new NodeInfo(NodeId.of("node1"), "127.0.0.1", port(), Set.of(), 50);
        int highPort;
        do { highPort = port(); } while (highPort == low.port());
        NodeInfo high = new NodeInfo(NodeId.of("node2"), "127.0.0.1", highPort, Set.of(), 100);
        survivor = new NGridNode(config(low, high, directory.resolve("low"), initial, delta ? 1 : 2));
        preferred = new NGridNode(config(high, low, directory.resolve("high"), initial, delta ? 1 : 2));
        survivor.start();
        preferred.start();
        ClusterTestUtils.awaitClusterConsensus(survivor, preferred);
        await(() -> preferred.coordinator().isLeader() && !preferred.replicationManager().isLeaderSyncing());
        preferred.getMap(MAP, String.class, String.class).put("before", "A");
        await(() -> "A".equals(local(survivor, "before")));

        preferred.close();
        preferred = null;
        assertTrue(Files.exists(directory.resolve("high/replication/relay/.clean-shutdown")),
                "the reproduction requires a genuinely clean shutdown");
        if (delta) {
            await(() -> survivor.coordinator().isLeader() && !survivor.replicationManager().isLeaderSyncing());
            survivor.getMap(MAP, String.class, String.class).put("during", "B");
        }
        preferred = new NGridNode(config(high, low, directory.resolve("high"), restarted, delta ? 1 : 2));
        preferred.start();
        await(() -> preferred.coordinator().isLeader() && !preferred.replicationManager().isLeaderSyncing());
        assertEquals("A", local(preferred, "before"), "the old prefix must exist when leadership returns");
        assertEquals("A", preferred.getMap(MAP, String.class, String.class).getOptional("before").orElse(null));
        if (delta) assertEquals("B", local(preferred, "during"), "the absent-period delta must also survive");
        assertEquals("A", local(survivor, "before"));
    }

    private NGridConfig config(NodeInfo self, NodeInfo peer, Path dir, NMapPersistenceMode mode, int replicationFactor) {
        return NGridConfig.builder(self).clusterName("volatile-restart-212")
                .dataDirectory(dir).addPeer(peer).replicationFactor(replicationFactor).mapName(MAP)
                .mapPersistenceMode(mode).pairMode(true).minClusterSize(1)
                .heartbeatInterval(Duration.ofMillis(200)).leaseTimeout(Duration.ofSeconds(3))
                .bootDiscoveryWindow(Duration.ofSeconds(2)).build();
    }

    private static String local(NGridNode node, String key) {
        return node.getMap(MAP, String.class, String.class).getOptional(key, Consistency.EVENTUAL).orElse(null);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(35).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "cluster condition did not converge");
    }

    private static int port() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }
}
