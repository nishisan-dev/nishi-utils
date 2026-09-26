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

package dev.nishisan.utils.ngrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.TopicFrontiers;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;

/**
 * Issue #178 (E2E, 2 nós reais, 3 tópicos como no ngrrd): depois de reiniciar o seguidor, o odômetro
 * aplicado dos dois nós fica na MESMA escala (soma das fronteiras por tópico), o lag global é zero,
 * o líder não observa "peer watermark above its own applied" e a liderança não muda. Na 8.7.0 o
 * seguidor reiniciado semeava a SOMA e depois commitava o MÁXIMO por tópico, enquanto o líder contava
 * um tick por op — três escalas incomparáveis.
 */
class MultiTopicRestartWatermarkE2ETest {

    private static final String CATALOG = "ngrrd.catalog";
    private static final String NODES = "ngrrd.nodes";
    private static final String GEOMETRIES = "ngrrd.geometries";

    private NGridNode node1;
    private NGridNode node2;
    private final List<String> coordinatorWarnings = new CopyOnWriteArrayList<>();
    private Handler handler;

    @AfterEach
    void tearDown() {
        if (handler != null) {
            Logger.getLogger(ClusterCoordinator.class.getName()).removeHandler(handler);
        }
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void followerRestartKeepsBothNodesOnTheSameScaleWithZeroLag() throws Exception {
        handler = new Handler() {
            @Override public void publish(LogRecord record) {
                if (record.getMessage() != null && record.getMessage().contains("peer watermark above its own applied")) {
                    coordinatorWarnings.add(record.getMessage());
                }
            }
            @Override public void flush() { }
            @Override public void close() { }
        };
        Logger.getLogger(ClusterCoordinator.class.getName()).addHandler(handler);

        int port1 = allocateFreeLocalPort(Set.of());
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);
        Path baseDir = Files.createTempDirectory("multi-topic-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();
        registerMaps(node1);
        registerMaps(node2);
        awaitLeader(node1, 30_000);

        // Volume desigual por tópico, como no ngrrd: o status (`nodes`) domina qualquer agregado.
        write(node1, CATALOG, 10);
        write(node1, NODES, 300);
        write(node1, GEOMETRIES, 3);
        long leaderApplied = node1.replicationManager().getLastAppliedSequence();
        assertEquals(313L, leaderApplied, "o odômetro do líder é a soma das fronteiras por tópico");
        awaitApplied(node2, leaderApplied, 60_000);
        assertEquals(node1.replicationManager().appliedFrontiers(), node2.replicationManager().appliedFrontiers());

        // (2) O seguidor reinicia LIMPO do mesmo diretório; o líder continua produzindo.
        closeQuietly(node2);
        node2 = null;
        write(node1, NODES, 50);
        write(node1, CATALOG, 2);
        node2 = newNode(info2, info1, dir2);
        node2.start();
        registerMaps(node2);

        long target = node1.replicationManager().getLastAppliedSequence();
        assertEquals(365L, target);
        awaitApplied(node2, target, 60_000);
        awaitTotalLagZero(node2, 30_000);

        // (3) Mesma escala nos dois nós, por tópico e no total; lag zero; liderança inalterada.
        TopicFrontiers leaderFrontiers = node1.replicationManager().appliedFrontiers();
        TopicFrontiers followerFrontiers = node2.replicationManager().appliedFrontiers();
        assertEquals(leaderFrontiers, followerFrontiers, "vetores idênticos após o restart");
        assertEquals(target, node2.replicationManager().getLastAppliedSequence());
        assertEquals(target, node2.coordinator().getTrackedLeaderHighWatermark(),
                "o watermark anunciado pelo líder está na mesma escala do odômetro do seguidor");
        assertEquals(0L, node2.operationalSnapshot().replicationLag());
        assertEquals(0L, node1.operationalSnapshot().replicationLag());
        assertEquals(leaderFrontiers.byTopic(), node2.operationalSnapshot().appliedByTopic());
        for (Map.Entry<String, ReplicationManager.TopicReplicationStatus> e
                : node2.replicationManager().getTopicReplicationStatuses().entrySet()) {
            assertEquals(0L, e.getValue().lag(), "lag por tópico zero em " + e.getKey());
        }

        long stableUntil = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < stableUntil) {
            assertTrue(node1.coordinator().isLeader(), "a liderança não pode mudar por dessincronia de escala");
            sleep(100);
        }
        assertTrue(coordinatorWarnings.isEmpty(),
                "o líder nunca deve observar um watermark de peer acima do próprio: " + coordinatorWarnings);
    }

    private static void registerMaps(NGridNode node) {
        node.getMap(CATALOG, String.class, String.class);
        node.getMap(NODES, String.class, String.class);
        node.getMap(GEOMETRIES, String.class, String.class);
    }

    private static void write(NGridNode node, String map, int count) {
        DistributedMap<String, String> m = node.getMap(map, String.class, String.class);
        for (int i = 0; i < count; i++) {
            putWithRetry(m, map + "-" + System.nanoTime() + "-" + i, "v" + i);
        }
    }

    private static void putWithRetry(DistributedMap<String, String> map, String key, String value) {
        long deadline = System.currentTimeMillis() + 30_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                map.put(key, value);
                return;
            } catch (RuntimeException e) {
                String name = e.getClass().getSimpleName();
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired") || name.contains("IllegalState")) {
                    last = e;
                    sleep(50);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("put não concluiu (líder não ficou pronto)", last);
    }

    private NGridNode newNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(300))
                .pairMode(true)
                .minClusterSize(1)
                .bootDiscoveryWindow(Duration.ofSeconds(2))
                .leaderPauseOnJoin(true)
                .build());
    }

    private static void awaitLeader(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            sleep(100);
        }
        fail("nó não virou líder pronto em " + timeoutMs + "ms");
    }

    private static void awaitApplied(NGridNode node, long target, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.replicationManager().getLastAppliedSequence() >= target) {
                return;
            }
            sleep(100);
        }
        fail("nó não alcançou applied " + target + " em " + timeoutMs + "ms (atual="
                + node.replicationManager().getLastAppliedSequence() + ", vetor="
                + node.replicationManager().appliedFrontiers() + ")");
    }

    private static void awaitTotalLagZero(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.replicationManager().getTotalReplicationLag() == 0L
                    && node.coordinator().getTrackedLeaderHighWatermark() == node.replicationManager().getLastAppliedSequence()) {
                return;
            }
            sleep(100);
        }
        fail("lag não zerou em " + timeoutMs + "ms (lag=" + node.replicationManager().getTotalReplicationLag()
                + ", hwm=" + node.coordinator().getTrackedLeaderHighWatermark()
                + ", applied=" + node.replicationManager().getLastAppliedSequence() + ")");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (IOException ignored) {
        }
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
        throw new IOException("Unable to allocate a free local port after multiple attempts");
    }
}
