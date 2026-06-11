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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reprodução E2E da issue tems#9/D8 (linhagens DIVERGENTES — a lacuna dos demais E2E de reclaim):
 * o líder preferido morre em kill -9 enquanto o standby está ATRASADO (parte da história nunca
 * replicou). O standby promove e produz a própria cauda — duas linhagens: o frontier durável do
 * morto (300) é numericamente MAIOR que o watermark honesto do incumbente (~160), mas pertence a
 * uma linhagem que o cluster não tem mais.
 *
 * <p>Nos outros E2E o incumbente sempre termina numericamente à frente (awaitApplied/proporções),
 * então os gates de watermark seguram "por acidente". Aqui o número do morto vence — e sem o fix o
 * retornante reassumia com o estado da linhagem morta sem nunca instalar o snapshot do incumbente
 * (pending desarmado no REQUEST + SYNC_REQUEST morto em silêncio + cutover preservando o contador).
 *
 * <p>Com o fix: o retornante fica inelegível (anuncia -1) até o snapshot do incumbente INSTALAR;
 * o cutover re-ancora contadores e binlog na linhagem do incumbente; só então o reclaim por
 * afinidade acontece. A prova é por CONTEÚDO: o estado final do novo líder é exatamente o do
 * incumbente (história comum + cauda newer-*), sem resíduo da cauda morta.
 */
class DivergentLineageReclaimE2ETest {

    private static final String QUEUE = "d8-queue";
    /** História comum, replicada para o standby antes do lag. */
    private static final int SHARED_OPS = 100;
    /** Cauda do líder preferido produzida com o standby FORA — a linhagem morta. */
    private static final int DEAD_TAIL_OPS = 200;
    /** Cauda do incumbente após promover — a linhagem canônica. */
    private static final int NEWER_OPS = 60;

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void divergentReturningLeaderMustInstallIncumbentStateBeforeReclaiming() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("d8-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();
        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        // (1) História COMUM: node-1 lidera, node-2 replica tudo.
        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < SHARED_OPS; i++) {
            offerWithRetry(q1, "base-" + i);
        }
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);

        // (2) node-2 SAI (limpo) e node-1 produz a cauda que NUNCA replica — a linhagem morta.
        closeQuietly(node2);
        node2 = null;
        for (int i = 0; i < DEAD_TAIL_OPS; i++) {
            offerWithRetry(q1, "dead-" + i);
        }
        long deadFrontier = node1.replicationManager().getLastAppliedSequence();

        // (3) node-1 MORRE em kill -9 (close + marker removido = crash real).
        closeQuietly(node1);
        node1 = null;
        Path marker1 = dir1.resolve("replication").resolve("relay").resolve(".clean-shutdown");
        assertTrue(Files.deleteIfExists(marker1),
                "pré-condição: o clean marker deveria existir após close() em " + marker1);

        // (4) node-2 volta sozinho, promove e produz a cauda CANÔNICA (linhagem do incumbente).
        node2 = newNode(info2, info1, dir2);
        node2.start();
        node2.getQueue(QUEUE, String.class);
        awaitLeader(node2, 30_000);
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        for (int i = 0; i < NEWER_OPS; i++) {
            offerWithRetry(q2, "newer-" + i);
        }
        long incumbentWatermark = node2.replicationManager().getLastAppliedSequence();
        assertTrue(incumbentWatermark < deadFrontier,
                "pré-condição do D8: o watermark do incumbente (" + incumbentWatermark
                        + ") deve ser MENOR que o frontier da linhagem morta (" + deadFrontier + ")");

        // (5) node-1 RETORNA unclean com o frontier da linhagem morta numericamente maior.
        node1 = newNode(info1, info2, dir1);
        node1.start();
        node1.getQueue(QUEUE, String.class);

        // (6) O reclaim por afinidade SÓ pode acontecer após instalar o snapshot do incumbente.
        awaitLeader(node1, 120_000);
        long reanchored = node1.replicationManager().getLastAppliedSequence();
        assertTrue(reanchored < deadFrontier,
                "o contador do novo líder deve estar re-ancorado na linhagem do incumbente (atual="
                        + reanchored + "), nunca no frontier morto (" + deadFrontier + ")");
        assertTrue(reanchored >= incumbentWatermark,
                "o novo líder deve cobrir o watermark do incumbente no handoff (atual=" + reanchored
                        + ", incumbente=" + incumbentWatermark + ")");

        // (7) PROVA POR CONTEÚDO: o estado do novo líder é a linhagem do incumbente — toda a cauda
        // newer-* presente, NENHUM resíduo da cauda morta dead-* (o snapshot substituiu o estado).
        // Contagens por valor DISTINTO: o offerWithRetry pode duplicar uma entrega numa janela de
        // handoff (retry após timeout com a op já aplicada) — entrega duplicada não é o que está
        // sob teste aqui; perda e ressurreição de linhagem morta são.
        DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
        List<String> drained = drainQueue(q1b, 120_000);
        long newer = drained.stream().filter(v -> v.startsWith("newer-")).distinct().count();
        long dead = drained.stream().filter(v -> v.startsWith("dead-")).count();
        long base = drained.stream().filter(v -> v.startsWith("base-")).distinct().count();
        assertEquals(NEWER_OPS, newer,
                "a cauda do incumbente deve sobreviver ao reclaim (drenado=" + drained.size()
                        + ", incumbentWatermark=" + incumbentWatermark + ")");
        assertEquals(0, dead,
                "nenhum item da linhagem morta pode reaparecer após o install do snapshot");
        assertEquals(SHARED_OPS, base, "a história comum deve estar íntegra");
    }

    // ---- helpers (espelho do StaleFrontierReclaimE2ETest) ----

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
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                        || name.contains("IllegalState")) {
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
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                        || name.contains("IllegalState")) {
                    last = e;
                    sleep(50);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("offer não concluiu (líder não ficou pronto)", last);
    }

    private void awaitLeader(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            sleep(100);
        }
        fail("nó " + node.coordinator().leaderInfo() + " não virou líder pronto em " + timeoutMs + "ms");
    }

    private void awaitApplied(NGridNode node, long target, long timeoutMs) {
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

    private void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (IOException ignored) {
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
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
