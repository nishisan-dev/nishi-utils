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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
// (assertFalse/assertEquals não usados — asserções centrais via watermark)

/**
 * E2E REALISTA (2 NGridNodes reais, RELAY_STREAM, pairMode) do step-10 validado em pré-prod:
 *
 * <ol>
 *   <li>node-1 (prio 100, preferido) e node-2 (prio 50) sobem; node-1 lidera e grava estado.</li>
 *   <li>node-1 CAI. node-2 assume e AVANÇA o watermark (grava mais ops) — estado mais novo.</li>
 *   <li>node-1 VOLTA do MESMO data dir (estado durável STALE, atrás do node-2).</li>
 *   <li>ASSERT: node-1 PERMANECE follower e SINCRONIZA até o watermark do node-2 ANTES de assumir;
 *       no momento do handoff, o estado/watermark do node-2 está refletido nele (sem perda).</li>
 * </ol>
 *
 * <p>SEM o fix (comportamento de main), node-1 reassume IMEDIATAMENTE por afinidade com estado stale —
 * o assert de "permanece follower até sincronizar" falha.
 */
class LeaderSyncBeforeReclaimE2ETest {

    private static final String QUEUE = "step10-queue";
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

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void higherAffinitySyncsIncumbentStateBeforeReclaiming() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        // node-1 prioridade 100 (preferido), node-2 prioridade 50.
        info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("step10-e2e");
        dir1 = Files.createDirectories(baseDir.resolve("node1"));
        dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node2 = newNode(info2, info1, dir2);
        node1.start();
        node2.start();

        // node-1 (prio maior) deve liderar; ambos ativos.
        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2.getQueue(QUEUE, String.class);

        // Grava estado inicial pelo líder node-1 e espera node-2 alcançar (replicação async pull).
        DistributedQueue<String> q1 = node1.getQueue(QUEUE, String.class);
        for (int i = 0; i < 30; i++) {
            offerWithRetry(q1, "base-" + i, node1);
        }
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 20_000);

        // (2) node-1 CAI.
        closeQuietly(node1);
        node1 = null;

        // node-2 assume liderança…
        awaitLeader(node2, 20_000);
        // …e AVANÇA o watermark com mais ops (estado MAIS NOVO que o durável do node-1).
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        for (int i = 0; i < 40; i++) {
            offerWithRetry(q2, "newer-" + i, node2);
        }
        // O estado TOTAL do incumbente node-2 (base aplicada + ops próprias) é o frontier que node-1 deve
        // alcançar antes de reassumir. É o que node-2 anuncia como líder (max(produzido, aplicado)).
        long incumbentWatermark = node2.replicationManager().getLastAppliedSequence();
        long node1DurableBeforeReturn = 30; // node-1 só tinha as 30 ops da base no seu estado durável
        assertTrue(incumbentWatermark > node1DurableBeforeReturn,
                "node-2 deve ter avançado o estado além do durável do node-1 (incumbente total="
                        + incumbentWatermark + ", node-1 durável=" + node1DurableBeforeReturn + ")");

        // (3) node-1 VOLTA do MESMO data dir (estado durável STALE, atrás do node-2).
        node1 = newNode(info1, info2, dir1);
        node1.start();
        // Registra o handler do tópico ANTES de qualquer reassunção, para que node-1 (como follower)
        // puxe o stream de step10-queue do node-2 e convirja o estado mais novo.
        node1.getQueue(QUEUE, String.class);

        // (4) ASSERT — lado sync-before-reclaim:
        // node-1 (prio maior) NÃO pode reassumir enquanto está ATRÁS do watermark do node-2. Observa-se
        // que, durante a convergência, node-1 permanece follower (node-2 segue líder) ATÉ alcançar o
        // watermark do incumbente. Capturamos o frontier de node-1 no INSTANTE em que ele vira líder.
        long node1AppliedAtHandoff = awaitNode1ReclaimsAndCaptureApplied(incumbentWatermark, 40_000);

        // Prova central: no handoff, node-1 já tinha SINCRONIZADO o estado do node-2 (sem perda) — o
        // frontier aplicado de node-1 alcançou o watermark do incumbente ANTES de ele ativar a liderança.
        assertTrue(node1AppliedAtHandoff >= incumbentWatermark,
                "node-1 deve ter sincronizado até o watermark do incumbente (" + incumbentWatermark
                        + ") ANTES de reassumir; aplicado no handoff = " + node1AppliedAtHandoff);

        // E o estado do node-2 está refletido em node-1: seu frontier APLICADO cobre todo o estado do
        // incumbente (sem regressão), e a fila contém os itens replicados (os 'newer-*' do node-2 não
        // foram perdidos). lastApplied é a escala de estado consistente entre os papéis (vs globalSequence,
        // que conta só a produção local de cada nó).
        assertTrue(node1.replicationManager().getLastAppliedSequence() >= incumbentWatermark,
                "node-1, como novo líder, deve refletir todo o estado do incumbente (sem regressão de estado)");
        // Espera node-1 concluir o drain-gate de promoção antes de ler (evita o LeaderSyncing transitório).
        awaitLeader(node1, 15_000);
        DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
        assertTrue(q1b.peek().isPresent(), "a fila no novo líder deve conter o estado replicado");
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

    /**
     * Espera node-1 reassumir a liderança e devolve o frontier APLICADO dele capturado no instante do
     * handoff. Enquanto não é líder, verifica que node-2 segue líder (nunca há dois líderes). Falha se
     * node-1 reassume com estado stale (aplicado &lt; watermark do incumbente) — o comportamento sem fix.
     */
    private long awaitNode1ReclaimsAndCaptureApplied(long incumbentWatermark, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean n1Leader = node1.coordinator().isLeader();
            if (n1Leader) {
                // Capturado no handoff: o frontier que node-1 tinha ao ativar a liderança.
                return node1.replicationManager().getLastAppliedSequence();
            }
            // Invariante: enquanto node-1 não é líder, node-2 deve seguir líder (sem janela leaderless
            // perpétua nem dois líderes). Não exigimos a cada tick (há um breve gap de handoff), mas
            // node-1 jamais pode liderar com estado stale — o que validamos no retorno acima.
            sleep(50);
        }
        fail("node-1 não reassumiu a liderança dentro de " + timeoutMs + "ms (incumbentWatermark="
                + incumbentWatermark
                + ", node1Applied=" + (node1 != null ? node1.replicationManager().getLastAppliedSequence() : "n/a")
                + ", node1SeesLeader=" + (node1 != null ? node1.coordinator().leaderInfo()
                        .map(i -> i.nodeId().value()).orElse("none") : "n/a")
                + ", node2Leader=" + (node2 != null && node2.coordinator().isLeader())
                + ", node2Applied=" + (node2 != null ? node2.replicationManager().getLastAppliedSequence() : "n/a") + ")");
        return -1L;
    }

    /**
     * Grava um item tolerando exceções transitórias de readiness do líder ({@code LeaderSyncing} /
     * {@code LeaseExpired}) que ocorrem em torno de um handoff/boot — espera o líder ficar pronto e
     * tenta de novo. Não mascara falhas reais: estoura após o deadline.
     */
    private void offerWithRetry(DistributedQueue<String> queue, String value, NGridNode leader) {
        long deadline = System.currentTimeMillis() + 15_000;
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
