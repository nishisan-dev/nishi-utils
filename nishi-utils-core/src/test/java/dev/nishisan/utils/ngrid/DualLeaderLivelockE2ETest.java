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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reprodução E2E da issue tems#9/D10: dual-leader livelock no handoff por afinidade sob PRODUÇÃO
 * CONTÍNUA — o padrão de teste que não existia (todos os E2E anteriores pausavam a produção em
 * volta das transições).
 *
 * <p>Incidente real (pré-prod, 2 nós, firehose): o nó de maior afinidade religou limpo, o
 * join-quiesce liberou em 1,5s (catch-up real: 64s), o candidato ficou eternamente "atrás" pelo
 * delta in-flight (gate B estrito jamais abria) e o latch de caught-up armou contra watermark de
 * heartbeat stale — o candidato assumiu SEM o incumbente ceder: dois líderes estáveis por 3,5min,
 * escada infinita de epochs (22→105+, um re-stamp a cada 3s) e contenção manual (SIGTERM + wipe).
 *
 * <p>Com o pacote D10 (a: join-quiesce cobre o catch-up real; b: quiesce-assisted reclaim; c:
 * resolução determinística), o ciclo religa→aproxima→pausa bounded→emparelha→cede converge para
 * líder único sob o firehose, com epoch estável e sem perda nem duplicata de itens ackados.
 */
class DualLeaderLivelockE2ETest {

    private static final String QUEUE = "d10-queue";
    /** Mínimos de produção por fase (história pré-restart, cauda do incumbente). */
    private static final int MIN_OPS_PHASE1 = 100;
    private static final int MIN_OPS_PHASE2 = 100;
    /** Janela máxima tolerada de dual-leader contínuo (debounce de 3 HBs de 300ms + folga). */
    private static final long MAX_DUAL_LEADER_WINDOW_MS = 3_000;
    /** Delta máximo de epoch pós-rejoin (a escada do incidente somava +1 a cada heartbeat). */
    private static final long MAX_EPOCH_DELTA = 10;

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void affinityHandoffUnderContinuousProductionConvergesToSingleLeader() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("d10-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        // Boot SEQUENCIAL e determinístico: node-1 lidera sozinho e node-2 entra como follower
        // vazio (nunca "à frente" — sem dança de eleição no boot). O alvo deste teste é o handoff
        // do REJOIN sob firehose com linhagem única, como no incidente; churn de boot criaria
        // ramos descartados cujas ops inflam os contadores escalares e quebram o emparelhamento
        // exato (a limitação estrutural endereçada no follow-up epoch-aware da PR #142).
        node1 = newNode(info1, info2, dir1);
        node1.start();
        awaitLeader(node1, 20_000);
        node1.getQueue(QUEUE, String.class);
        node2 = newNode(info2, info1, dir2);
        node2.start();
        DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
        // A produção só começa com o par ESTÁVEL (node-1 líder, node-2 follower emparelhado,
        // sustentado): a dança de eleição do boot (deferral por watermark desconhecido no connect +
        // escape do D9) pode ter janelas de liderança transitória do node-2, e itens ackados nelas
        // morreriam no yield — ruído de boot, não o cenário do D10.
        awaitStablePair(10_000, 90_000);

        // Produtora contínua via node-2 (vivo o teste inteiro): cada item ACKADO é registrado —
        // a prova por conteúdo no final exige todos exatamente uma vez.
        Producer producer = new Producer(q2);
        Thread producerThread = new Thread(producer, "d10-firehose");
        producerThread.start();

        try {
            // (1) node-1 lidera sob o firehose; node-2 sincroniza a história.
            producer.awaitAcked(MIN_OPS_PHASE1, 60_000);

            // (2) node-1 sai LIMPO com o node-2 EMPARELHADO (como no incidente: o streaming
            // acompanhava o firehose). A produtora é pausada só em volta do close — divergência de
            // linhagem no shutdown é o cenário do D8, não o alvo deste teste.
            producer.pause();
            // Quiesce de shutdown: o ack do RELAY_STREAM é no binlog e o apply local é assíncrono —
            // fechar com applies em voo é a janela de outro defeito (fora do alvo deste teste).
            awaitApplied(node1, node1.replicationManager().getGlobalSequence(), 60_000);
            awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);
            closeQuietly(node1);
            node1 = null;
            awaitLeader(node2, 30_000);
            producer.resume();

            // (3) node-2 produz a cauda como incumbente, sob o firehose.
            int ackedBeforeRejoin = producer.ackedCount();
            producer.awaitAcked(ackedBeforeRejoin + MIN_OPS_PHASE2, 60_000);

            // (4) node-1 religa do mesmo data dir COM A PRODUÇÃO RODANDO — o cenário do incidente.
            long epochAtRejoin = node2.coordinator().getLeaderEpoch();
            node1 = newNode(info1, info2, dir1);
            node1.start();
            node1.getQueue(QUEUE, String.class);
            DualLeaderWatcher watcher = new DualLeaderWatcher();
            Thread watcherThread = new Thread(watcher, "d10-dual-watcher");
            watcherThread.start();

            // (5) O handoff por afinidade COMPLETA sob produção contínua (sem o pacote D10, o gate
            // B estrito retém para sempre ou o cluster degenera em dual-leader estável).
            awaitLeader(node1, 90_000);

            // (6) Estabilidade além da janela da abdicação do D9 (20 HBs de 300ms = 6s).
            long stableUntil = System.currentTimeMillis() + 6_500;
            while (System.currentTimeMillis() < stableUntil) {
                assertTrue(node1.coordinator().isLeader(),
                        "a liderança reclamada deve ser estável sob produção contínua");
                sleep(100);
            }

            watcher.stop();
            watcherThread.join(5_000);
            producer.stop();
            producerThread.join(30_000);
            producer.rethrowUnexpected();

            // (7) NUNCA dois líderes além do overlap transitório: a resolução determinística (c)
            // quebra qualquer dual em ~1 ciclo de debounce; o incidente sustentou 3,5 MINUTOS.
            assertTrue(watcher.maxDualWindowMs() < MAX_DUAL_LEADER_WINDOW_MS,
                    "janela máxima de dual-leader contínuo foi " + watcher.maxDualWindowMs()
                            + "ms — a resolução deveria tê-la quebrado em < "
                            + MAX_DUAL_LEADER_WINDOW_MS + "ms");

            // (8) Epoch estável: sem a escada de re-stamps (no incidente, +1 a cada heartbeat).
            long epochDelta = node1.coordinator().getLeaderEpoch() - epochAtRejoin;
            assertTrue(epochDelta >= 0 && epochDelta < MAX_EPOCH_DELTA,
                    "delta de epoch pós-rejoin foi " + epochDelta
                            + " — a escada de re-stamps deveria estar morta (< " + MAX_EPOCH_DELTA + ")");

            // (9) PROVA POR CONTEÚDO: todo item ackado presente exatamente UMA vez no líder final —
            // o handoff coordenado não perde a cauda do incumbente nem duplica nada.
            DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
            List<String> drained = drainQueue(q1b, 120_000);
            Set<String> drainedSet = new HashSet<>(drained);
            assertEquals(drained.size(), drainedSet.size(),
                    "nenhuma duplicata pode existir na fila do líder final");
            List<String> lost = new ArrayList<>();
            for (String acked : producer.ackedItems()) {
                if (!drainedSet.contains(acked)) {
                    lost.add(acked);
                }
            }
            assertTrue(lost.isEmpty(),
                    "itens ACKADOS perdidos no handoff (" + lost.size() + "): "
                            + lost.subList(0, Math.min(10, lost.size()))
                            + " — ackados=" + producer.ackedCount() + ", drenados=" + drained.size());
        } finally {
            producer.stop();
            producerThread.join(10_000);
        }
    }

    /** Produtora contínua com retry transitório; registra cada item ackado, na ordem. */
    private final class Producer implements Runnable {
        private final DistributedQueue<String> queue;
        private final List<String> acked = new CopyOnWriteArrayList<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicBoolean paused = new AtomicBoolean(false);
        private final AtomicLong sequence = new AtomicLong();
        private final AtomicReference<RuntimeException> unexpected = new AtomicReference<>();

        Producer(DistributedQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (running.get()) {
                if (paused.get()) {
                    sleep(20);
                    continue;
                }
                String item = "op-" + sequence.get();
                try {
                    queue.offer(item);
                    acked.add(item);
                    sequence.incrementAndGet();
                    sleep(5);
                } catch (RuntimeException e) {
                    String name = e.getClass().getSimpleName();
                    if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                            || name.contains("IllegalState") || name.contains("Timeout")
                            || name.contains("Quorum")) {
                        sleep(50); // transitório: handoff/quiesce/promoção — retry do MESMO item
                        continue;
                    }
                    unexpected.set(e);
                    return;
                }
            }
        }

        void pause() {
            paused.set(true);
            sleep(200); // deixa um offer in-flight concluir antes do close
        }

        void resume() {
            paused.set(false);
        }

        void stop() {
            running.set(false);
        }

        int ackedCount() {
            return acked.size();
        }

        List<String> ackedItems() {
            return new ArrayList<>(acked);
        }

        void awaitAcked(int target, long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                rethrowUnexpected();
                if (acked.size() >= target) {
                    return;
                }
                sleep(100);
            }
            fail("a produtora não alcançou " + target + " acks em " + timeoutMs + "ms (atual="
                    + acked.size() + ")");
        }

        void rethrowUnexpected() {
            RuntimeException e = unexpected.get();
            if (e != null) {
                throw new IllegalStateException("a produtora morreu com exceção não-transitória", e);
            }
        }
    }

    /** Amostra os dois nós e mede a maior janela CONTÍNUA em que ambos respondem isLeader(). */
    private final class DualLeaderWatcher implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile long maxDualWindowMs = 0;

        @Override
        public void run() {
            long dualSince = -1;
            while (running.get()) {
                NGridNode n1 = node1;
                NGridNode n2 = node2;
                boolean dual = n1 != null && n2 != null
                        && n1.coordinator().isLeader() && n2.coordinator().isLeader();
                long now = System.currentTimeMillis();
                if (dual) {
                    if (dualSince < 0) {
                        dualSince = now;
                    }
                    maxDualWindowMs = Math.max(maxDualWindowMs, now - dualSince);
                } else {
                    dualSince = -1;
                }
                sleep(50);
            }
        }

        void stop() {
            running.set(false);
        }

        long maxDualWindowMs() {
            return maxDualWindowMs;
        }
    }

    // ---- helpers (espelho do CleanRestartExLeaderRejoinE2ETest) ----

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
                .joinQuiesceMaxDuration(Duration.ofSeconds(10))
                .leaderPauseOnReclaim(true)
                .reclaimQuiesceThreshold(500L)
                .reclaimQuiesceMaxDuration(Duration.ofSeconds(10))
                .reclaimQuiesceCooldown(Duration.ofSeconds(5))
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

    /** Aguarda node-1 líder + node-2 follower emparelhado, SUSTENTADO por {@code holdMs}. */
    private void awaitStablePair(long holdMs, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long stableSince = -1;
        while (System.currentTimeMillis() < deadline) {
            boolean stable = node1.coordinator().isLeader()
                    && !node1.replicationManager().isLeaderSyncing()
                    && !node2.coordinator().isLeader()
                    && node2.replicationManager().getLastAppliedSequence() >= node1
                            .replicationManager().getAdvertisedHighWatermark();
            long now = System.currentTimeMillis();
            if (stable) {
                if (stableSince < 0) {
                    stableSince = now;
                }
                if (now - stableSince >= holdMs) {
                    return;
                }
            } else {
                stableSince = -1;
            }
            sleep(100);
        }
        fail("o par não estabilizou (node-1 líder + node-2 emparelhado) em " + timeoutMs + "ms");
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
