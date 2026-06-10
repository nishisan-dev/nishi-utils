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
 * Reprodução E2E da issue tems#9/D9: o reingresso LIMPO de um ex-líder re-aplicava a PRÓPRIA cauda
 * produzida e desestabilizava a liderança.
 *
 * <p>Um líder nunca avança o próprio frontier de follower; após um restart limpo o frontier fica
 * stale enquanto {@code _topic:{t}} guarda tudo que o nó produziu. Sem a re-âncora do seed (F1), o
 * cursor de fetch nascia no frontier stale e o nó re-puxava do binlog do novo líder — e RE-APLICAVA
 * — as próprias ops (duplicação no engine, fila não-idempotente!), inflando o contador aplicado:
 * reclaim prematuro, applied congelado pós-reclaim, abdicação ~20 heartbeats depois e o cluster num
 * impasse leaderless de deferência mútua (observado em ambiente real: 62 minutos sem líder).
 *
 * <p>Prova por CONTEÚDO: após o ciclo produzir→sair limpo→incumbente produz cauda→reingressar→
 * reclamar, a fila do novo líder contém exatamente a história + a cauda — sem NENHUMA duplicata —
 * e a liderança permanece estável por mais de 20 períodos de heartbeat (a janela da abdicação).
 */
class CleanRestartExLeaderRejoinE2ETest {

    private static final String QUEUE = "d9-queue";
    private static final int BASE_OPS = 100;
    private static final int NEWER_OPS = 30;

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void cleanRejoinMustNotReapplyOwnTailNorDestabilizeLeadership() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("d9-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();
        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        // (1) node-1 lidera e produz a história; node-2 sincroniza tudo (linhagem única).
        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < BASE_OPS; i++) {
            offerWithRetry(q1, "base-" + i);
        }
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);

        // (2) node-1 sai LIMPO (marker presente, frontier durável persistido).
        closeQuietly(node1);
        node1 = null;

        // (3) node-2 promove e produz a cauda.
        awaitLeader(node2, 30_000);
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        for (int i = 0; i < NEWER_OPS; i++) {
            offerWithRetry(q2, "newer-" + i);
        }

        // (4) node-1 reingressa LIMPO do mesmo data dir: o seed re-ancorado (F1) não pode re-puxar a
        // própria história; ele só streama a cauda do incumbente e reassume por afinidade.
        node1 = newNode(info1, info2, dir1);
        node1.start();
        node1.getQueue(QUEUE, String.class);
        awaitLeader(node1, 60_000);

        // (5) Estabilidade: a liderança sobrevive à janela da abdicação do incidente (~20 heartbeats;
        // aqui 300ms × 20 = 6s) sob o regime normal de heartbeats.
        long stableUntil = System.currentTimeMillis() + 6_500;
        while (System.currentTimeMillis() < stableUntil) {
            assertTrue(node1.coordinator().isLeader(),
                    "o líder reclamado não pode abdicar (issue tems#9, D9: applied congelado/inflado "
                            + "derrubava a liderança ~20 heartbeats após o reclaim)");
            sleep(100);
        }

        // (6) PROVA POR CONTEÚDO: história + cauda, sem NENHUM re-apply da cauda própria.
        DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
        List<String> drained = drainQueue(q1b, 120_000);
        long base = drained.stream().filter(v -> v.startsWith("base-")).count();
        long newer = drained.stream().filter(v -> v.startsWith("newer-")).count();
        long distinct = drained.stream().distinct().count();
        assertEquals(BASE_OPS, base,
                "a história deve estar presente exatamente UMA vez — re-apply da própria cauda "
                        + "duplicaria os itens (drenado=" + drained.size() + ")");
        assertEquals(NEWER_OPS, newer, "a cauda do incumbente deve sobreviver ao reclaim");
        assertEquals(drained.size(), distinct, "nenhuma duplicata pode existir na fila do novo líder");
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
