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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;

/**
 * Revisão #178 (A1): um líder rebaixado AO VIVO (sem restart) não pode re-puxar e re-aplicar as
 * próprias escritas. Antes, o líder nunca avançava {@code nextExpected} nem o cursor de fetch
 * enquanto produzia; ao ser rebaixado por um peer de afinidade maior, o laço de fetch partia do
 * cursor obsoleto, o novo líder servia a cauda (espelhada como seguidor) e o laço de apply a
 * aplicava de novo — OFFER de fila não é idempotente, os itens duplicavam.
 *
 * <p>Cenário: node-2 (prio 50) lidera sozinho e produz {@code BASE_OPS}; node-1 (prio 100) entra,
 * sincroniza e reclama por afinidade; node-1 produz {@code NEWER_OPS}. A fila drenada em node-2
 * (agora seguidor, lendo via líder) deve conter cada item exatamente uma vez.
 */
class LiveDemotionNoReplayE2ETest {

    private static final String QUEUE = "a1-queue";
    private static final int BASE_OPS = 120;
    private static final int NEWER_OPS = 40;

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void demotedLeaderDoesNotReplayItsOwnProducedTail() throws Exception {
        int port1 = allocateFreeLocalPort(Set.of());
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);
        Path baseDir = Files.createTempDirectory("a1-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        // (1) node-2 lidera sozinho e produz a base.
        node2 = newNode(info2, info1, dir2);
        node2.start();
        node2.getQueue(QUEUE, String.class);
        awaitLeader(node2, 30_000);
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        for (int i = 0; i < BASE_OPS; i++) {
            offerWithRetry(q2, "base-" + i);
        }
        long producedByNode2 = node2.replicationManager().getLastAppliedSequence();
        assertEquals(BASE_OPS, producedByNode2);

        // (2) node-1 (afinidade maior) entra: sincroniza a base e reclama a liderança ao vivo.
        node1 = newNode(info1, info2, dir1);
        node1.start();
        node1.getQueue(QUEUE, String.class);
        awaitApplied(node1, producedByNode2, 60_000);
        awaitLeader(node1, 60_000);
        awaitFollower(node2, 30_000);

        // (3) node-1 produz a cauda; node-2 (rebaixado) a streama sem re-puxar a própria base.
        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < NEWER_OPS; i++) {
            offerWithRetry(q1, "newer-" + i);
        }
        awaitApplied(node2, node1.replicationManager().getLastAppliedSequence(), 60_000);
        assertEquals(node1.replicationManager().appliedFrontiers(), node2.replicationManager().appliedFrontiers(),
                "as fronteiras devem coincidir (nada re-aplicado, nada faltando)");

        assertFalse(node2.coordinator().isLeader(), "node-2 continua seguidor");

        // (4) PROVA POR CONTEÚDO no backend LOCAL de node-2: um poll de seguidor é encaminhado ao
        // líder, então node-1 sai e node-2 reassume para drenar a PRÓPRIA fila — um replay da base
        // teria duplicado os itens ali (invisível enquanto node-1 servia as leituras).
        closeQuietly(node1);
        node1 = null;
        awaitLeader(node2, 60_000);
        List<String> drained = drainQueue(node2.getQueue(QUEUE, String.class), 120_000);
        long base = drained.stream().filter(v -> v.startsWith("base-")).count();
        long newer = drained.stream().filter(v -> v.startsWith("newer-")).count();
        long distinct = drained.stream().distinct().count();
        assertEquals(BASE_OPS, base, "a base produzida por node-2 deve existir UMA vez (drenado=" + drained.size() + ")");
        assertEquals(NEWER_OPS, newer, "a cauda de node-1 deve existir uma vez");
        assertEquals(drained.size(), distinct, "nenhuma duplicata");
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

    private List<String> drainQueue(DistributedQueue<String> queue, long timeoutMs) {
        List<String> drained = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        int emptyStreak = 0;
        while (System.currentTimeMillis() < deadline && emptyStreak < 10) {
            try {
                Optional<String> item = queue.poll();
                if (item.isPresent()) {
                    drained.add(item.get());
                    emptyStreak = 0;
                } else {
                    emptyStreak++;
                    sleep(100);
                }
            } catch (RuntimeException e) {
                String name = e.getClass().getSimpleName();
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired") || name.contains("IllegalState")) {
                    sleep(100);
                    continue;
                }
                throw e;
            }
        }
        return drained;
    }

    private void offerWithRetry(DistributedQueue<String> queue, String value) {
        long deadline = System.currentTimeMillis() + 30_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                queue.offer(value);
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
        throw new IllegalStateException("offer não concluiu (líder não ficou pronto)", last);
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

    private static void awaitFollower(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!node.coordinator().isLeader()) {
                return;
            }
            sleep(100);
        }
        fail("nó não foi rebaixado em " + timeoutMs + "ms");
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
                + node.replicationManager().getLastAppliedSequence() + ")");
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
