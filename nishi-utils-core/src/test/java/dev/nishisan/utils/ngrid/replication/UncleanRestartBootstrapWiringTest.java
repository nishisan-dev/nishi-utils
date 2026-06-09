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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Reprodução do D2 da issue #139: a proteção de bootstrap pós-restart-unclean
 * ({@code relayPendingBootstrap}) é um NO-OP quando o handler é registrado ANTES de
 * {@code ReplicationManager.start()} — fluxo legítimo de assembly manual (o construtor é público e
 * {@code start()} religa os loops de handlers pré-registrados).
 *
 * <p>{@code relayUncleanRestart} só é detectado em {@code start()}
 * ({@code detectUncleanRestartAndMarkBootstrap}); {@code registerHandler} chamado antes roda com o
 * flag ainda {@code false} e nunca arma o bootstrap do tópico. {@code start()} religa os loops dos
 * handlers pré-registrados mas NÃO faz o backfill da marcação — o nó aplica relay stale como se o
 * frontier fosse confiável.
 *
 * <p>O par de testes documenta a assimetria: registro APÓS start() arma o bootstrap (passa hoje,
 * regressão do caminho correto); registro ANTES de start() não arma (falha hoje; é o defeito).
 */
class UncleanRestartBootstrapWiringTest {

    private static final String TOPIC = "ha-state";

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private ClusterCoordinator coordinator;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("unclean-bootstrap-wiring-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (coordinator != null) {
            coordinator.close();
        }
        scheduler.shutdownNow();
    }

    /**
     * Assembly manual com handler registrado ANTES de start(): após um restart unclean com
     * dados prévios do tópico no relay, o bootstrap DEVE ficar pendente até sincronizar.
     * Hoje não arma (NO-OP) — o defeito D2 da issue #139.
     */
    @Test
    @Disabled("reprodução da issue #139 (D2) — habilitar junto com o backfill da marcação em start()")
    void preStartHandlerRegistrationArmsBootstrapOnUncleanRestart() throws Exception {
        simulatePriorUncleanRunWithTopicData();

        ReplicationManager manager = newManager();
        // Assembly manual: registerHandler na construção…
        manager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        // …e start() só depois (é aqui que o unclean restart é detectado — tarde demais).
        manager.start();
        try {
            ReplicationManager.TopicReplicationStatus status =
                    manager.getTopicReplicationStatuses().get(TOPIC);
            assertNotNull(status, "status do tópico registrado deve existir");
            assertTrue(status.relayPendingBootstrap(),
                    "restart unclean com dados prévios do tópico exige bootstrap pendente antes do "
                            + "apply, independentemente da ordem registerHandler/start() (issue "
                            + "#139: com registro pré-start a proteção nunca arma)");
        } finally {
            manager.close();
        }
    }

    /**
     * Caminho que já funciona (regressão): handler registrado APÓS start() — fluxo do NGridNode —
     * arma o bootstrap pendente no restart unclean.
     */
    @Test
    void registerAfterStartArmsBootstrapOnUncleanRestart() throws Exception {
        simulatePriorUncleanRunWithTopicData();

        ReplicationManager manager = newManager();
        manager.start();
        manager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        try {
            ReplicationManager.TopicReplicationStatus status =
                    manager.getTopicReplicationStatuses().get(TOPIC);
            assertNotNull(status, "status do tópico registrado deve existir");
            assertTrue(status.relayPendingBootstrap(),
                    "registro após start() deve armar o bootstrap pendente em restart unclean");
        } finally {
            manager.close();
        }
    }

    /**
     * Fabrica o estado durável de um run anterior morto sem shutdown limpo: diretório de relay com
     * dados do tópico ({@code RelayStore.hasTopicData}) e SEM o marker {@code .clean-shutdown}
     * ({@code RelayStore.isUncleanRestart} = true).
     */
    private void simulatePriorUncleanRunWithTopicData() throws IOException {
        Path relayDir = tempDir.resolve("relay");
        Files.createDirectories(relayDir.resolve(RelayStore.dirName(TOPIC)));
    }

    private ReplicationManager newManager() {
        NodeInfo local = new NodeInfo(NodeId.of("aaa-node"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("zzz-peer"), "127.0.0.1", 0);
        StubTransport transport = new StubTransport(local, List.of(peer));
        // minClusterSize=2 com um único membro ativo: o nó NÃO se elege — o pendente não pode ser
        // descartado pelo caminho de promoção; o teste observa o estado pós-start como follower
        // sem líder conhecido (drainRelayOnce aguarda, não remove).
        coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 2), scheduler);
        coordinator.start();
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .build());
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
