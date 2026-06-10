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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verificação do Achado 1 da revisão do PR #140: um nó com bootstrap pendente anuncia watermark
 * {@code -1}; a deferral {@code hasActivePeerWithUnknownWatermark} dispara sem guarda de boot-window
 * e sem exceção para o líder já estabelecido — então um líder saudável pode ABDICAR (ficar
 * leaderless) quando o peer reinicia UNCLEAN, levando a deadlock (o peer precisa de um líder para
 * concluir o bootstrap).
 *
 * <p>Restart unclean é simulado como nos testes existentes: fechar limpo e APAGAR o marker
 * {@code <dir>/replication/relay/.clean-shutdown}.
 */
class StaleFollowerRestartLivenessE2ETest {

    private static final String QUEUE = "liveness-queue";
    private static final int BASE_OPS = 60;

    private NGridNode node1;
    private NGridNode node2;
    private NodeInfo info1;
    private NodeInfo info2;
    private Path dir1;
    private Path dir2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    /**
     * 1a: node-1 (prio 100) é o líder saudável; node-2 (prio 50, standby) reinicia UNCLEAN.
     * Asserção: o cluster mantém um líder estável (node-1) — não fica leaderless.
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void healthyLeaderRetainsWhenStandbyRestartsUnclean() throws Exception {
        bootPairAndSync();

        // node-2 (standby) cai UNCLEAN: fecha e apaga o clean marker (= crash/pkill real).
        closeQuietly(node2);
        node2 = null;
        deleteCleanMarker(dir2);

        // node-2 volta do MESMO data dir (tem dado de relay prévio → relayPendingBootstrap armado →
        // anuncia watermark -1 enquanto faz bootstrap).
        node2 = newNode(info2, info1, dir2);
        node2.start();
        node2.getQueue(QUEUE, String.class);

        // Asserção de liveness: o cluster deve ter um líder ESTÁVEL (node-1) em até 30s.
        awaitStableClusterLeader("node-1", 30_000);
    }

    /**
     * 1b: ambos reiniciam UNCLEAN. Asserção: o cluster ELEGE um líder em até 40s (não fica
     * leaderless permanente).
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void clusterElectsLeaderWhenBothRestartUnclean() throws Exception {
        bootPairAndSync();

        closeQuietly(node1);
        closeQuietly(node2);
        node1 = null;
        node2 = null;
        deleteCleanMarker(dir1);
        deleteCleanMarker(dir2);

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        awaitStableClusterLeader(null, 40_000); // qualquer líder consistente serve
    }

    // ---- cenário comum ----

    private void bootPairAndSync() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("liveness-e2e");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();

        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < BASE_OPS; i++) {
            offerWithRetry(q1, "base-" + i);
        }
        // Garante que node-2 ingeriu o tópico via relay → terá dado de relay no disco (pré-condição
        // para o ramo unclean armar o bootstrap).
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 30_000);
    }

    // ---- helpers ----

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

    private void deleteCleanMarker(Path dir) throws IOException {
        Path marker = dir.resolve("replication").resolve("relay").resolve(".clean-shutdown");
        if (!Files.deleteIfExists(marker)) {
            // Não falha o teste por isso (o caminho do marker pode variar); apenas registra.
            System.out.println("[liveness-test] clean marker não encontrado para apagar: " + marker);
        }
    }

    /**
     * Espera o cluster convergir para um líder ESTÁVEL: ambos os nós ativos concordam no mesmo líder
     * e esse nó se reporta líder, mantido por algumas amostras consecutivas. Falha com diagnóstico
     * (incluindo se ficou leaderless — o sintoma do deadlock).
     */
    private void awaitStableClusterLeader(String expectedLeader, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int stable = 0;
        String lastSnapshot = "";
        while (System.currentTimeMillis() < deadline) {
            String l1 = leaderSeenBy(node1);
            String l2 = leaderSeenBy(node2);
            boolean someoneLeads = (node1 != null && node1.coordinator().isLeader())
                    || (node2 != null && node2.coordinator().isLeader());
            boolean agree = l1 != null && l1.equals(l2);
            boolean matchesExpected = expectedLeader == null || expectedLeader.equals(l1);
            lastSnapshot = snapshot();
            if (someoneLeads && agree && matchesExpected) {
                if (++stable >= 5) {
                    return; // líder consistente e estável
                }
            } else {
                stable = 0;
            }
            sleep(200);
        }
        fail("cluster NÃO convergiu para um líder estável em " + timeoutMs + "ms"
                + (expectedLeader != null ? " (esperado=" + expectedLeader + ")" : "")
                + " — sintoma de deadlock leaderless. Último estado: " + lastSnapshot);
    }

    private String leaderSeenBy(NGridNode node) {
        if (node == null) {
            return null;
        }
        return node.coordinator().leaderInfo().map(i -> i.nodeId().value()).orElse(null);
    }

    private String snapshot() {
        return "node1{leader=" + leaderSeenBy(node1)
                + ", isLeader=" + (node1 != null && node1.coordinator().isLeader())
                + ", applied=" + (node1 != null ? node1.replicationManager().getLastAppliedSequence() : "n/a")
                + ", global=" + (node1 != null ? node1.replicationManager().getGlobalSequence() : "n/a")
                + ", peerWm=" + (node1 != null ? node1.coordinator().maxActivePeerHighWatermark() : "n/a")
                + ", syncing=" + (node1 != null && node1.replicationManager().isLeaderSyncing())
                + "} node2{leader=" + leaderSeenBy(node2)
                + ", isLeader=" + (node2 != null && node2.coordinator().isLeader())
                + ", applied=" + (node2 != null ? node2.replicationManager().getLastAppliedSequence() : "n/a")
                + ", global=" + (node2 != null ? node2.replicationManager().getGlobalSequence() : "n/a")
                + ", peerWm=" + (node2 != null ? node2.coordinator().maxActivePeerHighWatermark() : "n/a")
                + ", syncing=" + (node2 != null && node2.replicationManager().isLeaderSyncing())
                + "}";
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
