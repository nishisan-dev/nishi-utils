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
import org.junit.jupiter.api.Disabled;
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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reprodução E2E da issue #139 (2 NGridNodes reais, RELAY_STREAM, pairMode) com as PROPORÇÕES do
 * incidente real: a história pré-queda do líder preferido é MUITO maior que a produção do
 * incumbente pós-promoção (no incidente: ~3,09M ops de história vs ~140k produzidas pelo node-2).
 *
 * <p>Nessas proporções o D1 (seed do frontier contaminado pelas chaves sintéticas
 * {@code _global}/{@code _topic:*} do sequence-state.dat) faz o node-1 retornar anunciando um
 * frontier aplicado ~2× a posição real do stream — acima do watermark honesto do incumbente — e os
 * dois gates do sync-before-reclaim passam: node-1 reassume IMEDIATAMENTE sem sincronizar a cauda
 * do node-2, e o cluster converge estável porém divergente (a cauda do incumbente some da visão do
 * líder).
 *
 * <p>O {@code LeaderSyncBeforeReclaimE2ETest} não captura isso por artefato de escala: lá o
 * incumbente produz MAIS que a história inteira (40 &gt; 30), então o frontier dobrado (~58) ainda
 * fica abaixo do watermark (~70) e o gate segura por acidente.
 *
 * <p>A prova é por CONTEÚDO (imune ao contador contaminado): no handoff, a fila do novo líder deve
 * conter os itens {@code newer-*} produzidos pelo incumbente.
 */
class StaleFrontierReclaimE2ETest {

    private static final String QUEUE = "issue9-queue";
    /** História pré-queda do node-1 — grande em relação à produção pós-promoção do node-2. */
    private static final int BASE_OPS = 300;
    /** Produção do incumbente após assumir — pequena (proporção do incidente: ~4,5%). */
    private static final int NEWER_OPS = 60;

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
     * Variante CLEAN (D1 isolado): node-1 cai LIMPO (marker presente, frontier durável íntegro) e
     * volta. Mesmo assim, o seed contaminado o faz "alcançado" e ele reassume sem puxar a cauda do
     * incumbente.
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Disabled("reprodução da issue #139 (D1) — habilitar junto com o fix do filtro de chaves sintéticas")
    void staleFrontierMustNotShortCircuitReclaimSync() throws Exception {
        runScenario(false);
    }

    /**
     * Variante UNCLEAN (D1 + D2/D3): além do seed contaminado, o restart sem marker exige bootstrap
     * antes do apply — que hoje é abandonado na promoção. A perda é maior (relay/frontier stale).
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Disabled("reprodução da issue #139 (D1+D2/D3) — habilitar junto com o fix do ramo unclean")
    void uncleanReturnMustBootstrapBeforeReclaiming() throws Exception {
        runScenario(true);
    }

    private void runScenario(boolean uncleanReturn) throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("issue9-e2e");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();

        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        // (1) node-1 lidera e acumula uma história GRANDE; node-2 sincroniza tudo.
        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < BASE_OPS; i++) {
            offerWithRetry(q1, "base-" + i, node1);
        }
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);

        // (2) node-1 CAI (shutdown limpo — o marker é escrito e o estado durável fica íntegro).
        closeQuietly(node1);
        node1 = null;
        if (uncleanReturn) {
            // Simula um crash real (kill/queda do processo): o run não deixou o clean marker. O
            // estado durável é o MESMO do close (frontier flushado), mas a lib deve tratá-lo como
            // não-confiável e exigir bootstrap antes do apply (ramo relayUncleanRestart).
            Path marker = dir1.resolve("replication").resolve("relay").resolve(".clean-shutdown");
            assertTrue(Files.deleteIfExists(marker),
                    "pré-condição: o clean marker deveria existir após close() em " + marker);
        }

        // (3) node-2 assume e produz uma cauda PEQUENA em relação à história (proporção do incidente).
        awaitLeader(node2, 20_000);
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        for (int i = 0; i < NEWER_OPS; i++) {
            offerWithRetry(q2, "newer-" + i, node2);
        }
        long incumbentWatermark = node2.replicationManager().getLastAppliedSequence();
        assertTrue(incumbentWatermark >= BASE_OPS + NEWER_OPS,
                "incumbente deve refletir história + cauda (watermark=" + incumbentWatermark + ")");

        // (4) node-1 VOLTA do MESMO data dir e (hoje) reassume quase imediatamente: o seed
        // contaminado (~2x a história) supera o watermark honesto do incumbente e fura os gates.
        node1 = newNode(info1, info2, dir1);
        node1.start();
        node1.getQueue(QUEUE, String.class);
        awaitLeader(node1, 60_000);

        // (5) PROVA POR CONTEÚDO: como novo líder, node-1 deve refletir o estado TOTAL do
        // incumbente — a cauda 'newer-*' não pode ter sido perdida no handoff. (Não usamos
        // getLastAppliedSequence() do node-1 como prova: é exatamente o contador contaminado.)
        DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
        List<String> drained = drainQueue(q1b, 120_000);
        long newerPresent = drained.stream().filter(v -> v.startsWith("newer-")).count();
        assertTrue(newerPresent == NEWER_OPS,
                "o novo líder deve conter TODA a cauda do incumbente após o handoff (issue #139): "
                        + "esperados " + NEWER_OPS + " itens 'newer-*', presentes " + newerPresent
                        + " (total drenado=" + drained.size()
                        + ", incumbentWatermark=" + incumbentWatermark + ")");
    }

    // ---- helpers ----

    private NGridNode newNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1) // RELAY_STREAM async (pairMode 1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(300))
                .pairMode(true)
                .minClusterSize(1)
                .bootDiscoveryWindow(Duration.ofSeconds(2))
                .leaderPauseOnJoin(true)
                .build());
    }

    /** Drena a fila do líder coletando os valores (tolera exceções transitórias de handoff). */
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

    private void offerWithRetry(DistributedQueue<String> queue, String value, NGridNode leader) {
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
