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
package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Reprodução do D1 da issue #139: o seed do applied a partir do frontier persistido
 * ({@code seedAppliedSequenceFromFrontier}) soma TODAS as entradas de
 * {@code nextExpectedSequenceByTopic} — incluindo as chaves sintéticas {@code _global} e
 * {@code _topic:{t}} que {@code saveSequenceState} grava no MESMO mapa e que
 * {@code loadSequenceState} injeta de volta via {@code putAll} sem filtrar.
 *
 * <p>Resultado: um nó que restarta com sequence-state persistido acorda reportando um frontier
 * aplicado ~2× a posição real do stream (em ambiente real: ~6,17M contra 3,087M reais), o que fura os
 * dois gates do sync-before-reclaim (o nó que volta se julga "caught up" e o incumbente julga o
 * candidato "à frente") — o líder preferido reassume sem sincronizar e o cluster converge
 * divergente.
 *
 * <p>O teste fabrica um {@code sequence-state.dat} exatamente como {@code saveSequenceState}
 * persiste (frontier real + chaves sintéticas) e asserta que o seed reflete APENAS o frontier
 * real do tópico.
 */
class ReplicationManagerSequenceStateTest {

    private static final String TOPIC = "ha-state";

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("seq-state-seed-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void seedAppliedIgnoresSyntheticSequenceStateKeys() throws Exception {
        // sequence-state.dat como um nó real persiste (saveSequenceState): o frontier do tópico
        // convive com as chaves sintéticas "_global" e "_topic:{t}" no MESMO mapa serializado.
        // Cenário: nó aplicou 1000 ops (nextExpected=1001), globalSequence=1000, seq do tópico=1000.
        Map<String, Long> persisted = new HashMap<>();
        persisted.put(TOPIC, 1001L);
        persisted.put("_global", 1000L);
        persisted.put("_topic:" + TOPIC, 1000L);
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(persisted);
        }

        NodeInfo local = new NodeInfo(NodeId.of("aaa-node"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("zzz-peer"), "127.0.0.1", 0);
        StubTransport transport = new StubTransport(local, List.of(peer));
        // minClusterSize=2 com um único membro ativo: sem eleição — o teste é só sobre o seed.
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 2), scheduler);
        coordinator.start();

        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .build());
        manager.start();
        try {
            // O frontier real é 1000 ops aplicadas. Com o bug, o seed devolve
            // (1001-1) + (1000-1) + (1000-1) = 2998 — quase 3× o estado real — e esse número é o
            // que alimenta o gate do sync-before-reclaim e o watermark anunciado nos heartbeats.
            assertEquals(1000L, manager.getLastAppliedSequence(),
                    "o seed do applied deve considerar apenas os frontiers reais por tópico; chaves "
                            + "sintéticas (_global/_topic:*) do sequence-state.dat não são frontier "
                            + "(issue #139: frontier forjado fura o sync-before-reclaim)");
        } finally {
            manager.close();
            coordinator.close();
        }
    }

    /**
     * D9 (issue tems#9): o frontier de um EX-LÍDER deve re-ancorar no contador PRODUZIDO do tópico no
     * seed do boot. Um líder nunca avança o próprio frontier de follower — após um restart limpo o
     * frontier fica stale no ponto do último apply-como-follower enquanto "_topic:{t}" guarda tudo que
     * o nó produziu. Sem a re-âncora, o cursor de fetch nasce no frontier stale e o nó RE-PUXA e
     * RE-APLICA a própria cauda produzida ao reingressar (apply duplicado + contador inflado →
     * reclaim prematuro → applied congelado → abdicação aos ~20 heartbeats → impasse leaderless).
     */
    @Test
    void seedReanchorsExLeaderFrontierOnProducedCounter() throws Exception {
        // Ex-líder: aplicou 100 como follower no passado (frontier stale = 101), depois liderou e
        // produziu até 300 (_topic e _global em 300). O frontier verdadeiro do nó é 301.
        Map<String, Long> persisted = new HashMap<>();
        persisted.put(TOPIC, 101L);
        persisted.put("_global", 300L);
        persisted.put("_topic:" + TOPIC, 300L);
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(persisted);
        }

        NodeInfo local = new NodeInfo(NodeId.of("aaa-node"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("zzz-peer"), "127.0.0.1", 0);
        StubTransport transport = new StubTransport(local, List.of(peer));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 2), scheduler);
        coordinator.start();

        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .build());
        manager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        manager.start();
        try {
            ReplicationManager.TopicReplicationStatus status =
                    manager.getTopicReplicationStatuses().get(TOPIC);
            assertEquals(301L, status.nextExpectedSequence(),
                    "o frontier do ex-líder deve re-ancorar no contador produzido (+1): o nó é a fonte "
                            + "da verdade do que produziu — frontier stale faria o fetch re-puxar e "
                            + "RE-APLICAR a própria cauda (issue tems#9, D9)");
            assertEquals(300L, manager.getLastAppliedSequence(),
                    "o applied semeado deve refletir o estado real (produzido), numa escala única");
        } finally {
            manager.close();
            coordinator.close();
        }
    }

    /**
     * Issue #178: com VÁRIOS tópicos o odômetro aplicado não pode misturar escalas. O seed antigo
     * somava as fronteiras (ou pegava {@code _global}, maior ainda), o commit do seguidor tomava o
     * MÁXIMO por tópico e o líder contava um tick por op — um nó reiniciado com 3 tópicos ficava
     * preso na escala da soma e anunciava um watermark acima do líder ("peer watermark above its own
     * applied"). Agora o applied é DERIVADO do vetor de fronteiras: soma por definição, em todos os
     * papéis e caminhos.
     */
    @Test
    void appliedIsDerivedFromPerTopicFrontiersWithMultipleTopics() throws Exception {
        // Três tópicos como no ngrrd (catalog/nodes/geometries) com fronteiras distintas; _global
        // deliberadamente MAIOR que a soma (líder antigo após cutover), para provar que não entra.
        Map<String, Long> persisted = new HashMap<>();
        persisted.put("map:ngrrd.catalog", 11L);      // 10 aplicadas
        persisted.put("map:ngrrd.nodes", 5001L);      // 5000 aplicadas
        persisted.put("map:ngrrd.geometries", 4L);    // 3 aplicadas
        persisted.put("_global", 9_999_999L);
        persisted.put("_topic:map:ngrrd.catalog", 10L);
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(persisted);
        }

        NodeInfo local = new NodeInfo(NodeId.of("aaa-node"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("zzz-peer"), "127.0.0.1", 0);
        StubTransport transport = new StubTransport(local, List.of(peer));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 2), scheduler);
        coordinator.start();

        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .build());
        manager.start();
        try {
            TopicFrontiers frontiers = manager.appliedFrontiers();
            assertEquals(10L, frontiers.frontier("map:ngrrd.catalog"));
            assertEquals(5000L, frontiers.frontier("map:ngrrd.nodes"));
            assertEquals(3L, frontiers.frontier("map:ngrrd.geometries"));
            assertEquals(0L, frontiers.frontier("_global"), "chaves sintéticas nunca entram no vetor");
            assertEquals(5013L, manager.getLastAppliedSequence(),
                    "o applied é a SOMA das fronteiras por tópico — nem o máximo (5000) nem _global");
            assertEquals(5013L, manager.getAdvertisedHighWatermark(),
                    "o watermark anunciado nos heartbeats usa a mesma escala derivada");
        } finally {
            manager.close();
            coordinator.close();
        }
    }

    /** Transporte mínimo para montar ClusterCoordinator + ReplicationManager reais (assembly manual). */
    private static final class StubTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();

        private StubTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }

        @Override
        public void addListener(TransportListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
        }

        @Override
        public void send(ClusterMessage message) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return Boolean.TRUE.equals(connected.get(nodeId));
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() throws IOException {
        }
    }
}
