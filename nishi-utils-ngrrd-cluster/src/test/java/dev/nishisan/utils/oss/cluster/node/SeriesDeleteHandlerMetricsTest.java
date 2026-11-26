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

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.DeletePrecondition;
import dev.nishisan.utils.oss.cluster.api.DeleteResult;
import dev.nishisan.utils.oss.cluster.api.DeleteStatus;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesDeletion;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.DeleteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.DeleteBatchResponse;
import dev.nishisan.utils.oss.cluster.protocol.DeleteRequest;
import dev.nishisan.utils.oss.cluster.protocol.DeleteControlRequest;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Métricas de exclusão de série de {@link SeriesDeleteHandler}: resultado final por pedido de cliente,
 * conclusão por operação, causa das falhas, tentativas da recuperação e as chaves obsoletas. RPC, catálogo e
 * transporte são fakes; o journal é real e vazio.
 */
class SeriesDeleteHandlerMetricsTest {

    private static final String SELF = "storage-a";
    private static final String OTHER = "storage-b";
    private static final String KEY = "br-sp/if-1";
    private static final long NOW = Instant.parse("2026-11-25T12:00:00Z").toEpochMilli();

    @TempDir
    Path tempDir;

    private final MutableClock clock = new MutableClock(Instant.ofEpochMilli(NOW));
    private final Map<String, SeriesPlacement> placements = new ConcurrentHashMap<>();
    private final Map<String, StorageNodeStatus> nodes = new ConcurrentHashMap<>();
    private final CatalogFake catalog = new CatalogFake();
    private final ScriptedRpc rpc = new ScriptedRpc();
    private volatile boolean leader = true;
    private SeriesLifecycleJournal journal;
    private SeriesLifecycleService lifecycle;
    private SeriesDeleteHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        nodes.put(SELF, status(SELF, StorageCapabilities.ALL));
        nodes.put(OTHER, status(OTHER, StorageCapabilities.ALL));
        journal = new SeriesLifecycleJournal(tempDir.resolve("lifecycle"));
        lifecycle = new SeriesLifecycleService(journal, null, catalog, null, SELF, "series", clock, 3_600_000L);
        handler = new SeriesDeleteHandler(fakeTransport(), catalog, rpc, lifecycle, () -> leader, () -> true, clock,
                () -> Set.of(SELF, OTHER), Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() throws IOException {
        handler.close();
        journal.close();
    }

    private static StorageNodeStatus status(String nodeId, Set<String> capabilities) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0, 0, 0, NOW, DistributionMode.COUNT, 1, 0,
                capabilities);
    }

    private static DeleteRequest request(String key) {
        return new DeleteRequest(key, new DeletePrecondition(0), "op-" + key);
    }

    private void place(String key) {
        placements.put(key, SeriesPlacement.active(SELF, NOW));
    }

    /** Placement com reserva já gravada: o que o {@code recover()} encontra depois de uma tentativa interrompida. */
    private void placePending(String key, boolean committed) {
        placements.put(key, SeriesPlacement.active(SELF, NOW)
                .withDeletion(new SeriesDeletion("op-" + key, 0, List.of(SELF, OTHER), committed), NOW));
    }

    private long metric(String name) {
        Long value = lifecycle.deleteMetrics().get(name);
        return value == null ? 0L : value;
    }

    @Test
    void pedidoDeClienteContaRespostaUmaVez() {
        place(KEY);
        place("br-sp/if-2");

        assertEquals(DeleteStatus.DELETED, handler.delete(request(KEY)).status());
        assertEquals(DeleteStatus.NOT_FOUND, handler.delete(request(KEY)).status());
        var batch = (DeleteBatchResponse) handler.handle(Commands.SERIES_DELETE_BATCH,
                new DeleteBatchRequest(List.of(request("br-sp/if-2"), request("br-sp/ausente"))), null);
        assertEquals(DeleteStatus.DELETED, batch.results().get("br-sp/if-2").status());
        assertEquals(DeleteStatus.NOT_FOUND, batch.results().get("br-sp/ausente").status());

        assertEquals(2, metric("seriesDelete.requests.DELETED"));
        assertEquals(2, metric("seriesDelete.requests.NOT_FOUND"));
        assertEquals(2, metric("seriesDelete.completed"));
        assertEquals(0, metric("seriesDelete.recoveryAttempts"));
        assertEquals(0, metric("seriesDelete.cancelled"));
    }

    @Test
    void recuperacaoNaoContaComoPedido() {
        placePending(KEY, true);

        handler.recover();

        assertEquals(1, metric("seriesDelete.recoveryAttempts"));
        assertEquals(1, metric("seriesDelete.completed"));
        assertEquals(1, metric("seriesDelete.pending"));
        assertEquals(0, metric("seriesDelete.requests.DELETED"));
        assertEquals(0, metric("seriesDelete.requests.ERROR"));
        assertFalse(lifecycle.deleteMetrics().keySet().stream().anyMatch(k -> k.startsWith("seriesDelete.requests.")),
                "recuperação não gera nenhuma chave de pedido: " + lifecycle.deleteMetrics());
    }

    @Test
    void exclusaoConcluidaPelaRecuperacaoContaCompletedUmaVez() {
        place(KEY);
        rpc.script((node, command) -> {
            if (Commands.DELETE_APPLY.equals(command) && rpc.applyFails) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "apply caiu");
            }
            return null;
        });
        rpc.applyFails = true;

        assertEquals(DeleteStatus.ERROR, handler.delete(request(KEY)).status());
        assertEquals(0, metric("seriesDelete.completed"));
        assertTrue(placements.containsKey(KEY), "a reserva fica para a recuperação");

        rpc.applyFails = false;
        handler.recover();
        handler.recover();

        assertEquals(1, metric("seriesDelete.completed"));
        assertEquals(1, metric("seriesDelete.requests.ERROR"));
        assertEquals(1, metric("seriesDelete.recoveryAttempts"), "o segundo ciclo não acha mais pendência");
        assertEquals(0, metric("seriesDelete.pending"));
    }

    @Test
    void causasDeFalha() {
        // NO_LEADER: ensureLeader falha antes de qualquer fase.
        place("k-no-leader");
        leader = false;
        assertEquals(DeleteStatus.ERROR, handler.delete(request("k-no-leader")).status());
        leader = true;

        // TIMEOUT: RPC de fase estoura o prazo.
        place("k-timeout");
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "prazo");
        });
        handler.delete(request("k-timeout"));

        // PARTICIPANT: recusa no prepare (cancela a reserva) e REMOTE_ERROR de participante.
        place("k-refused");
        rpc.script((node, command) -> Commands.DELETE_PREPARE.equals(command)
                ? DeleteResult.of(DeleteStatus.REFUSED_RECENT_WRITE, SELF) : null);
        assertEquals(DeleteStatus.REFUSED_RECENT_WRITE, handler.delete(request("k-refused")).status());
        place("k-remote");
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
        handler.delete(request("k-remote"));
        // PARTICIPANT: nó sem series.delete.
        nodes.put("storage-old", status("storage-old", Set.of()));
        place("k-old");
        rpc.script((node, command) -> null);
        handler.delete(request("k-old"));
        nodes.remove("storage-old");

        // STALE: a reserva muda durante o prepare, e o commit não a reconhece mais.
        place("k-stale");
        rpc.script((node, command) -> {
            if (Commands.DELETE_PREPARE.equals(command)) {
                placements.put("k-stale", SeriesPlacement.active(SELF, NOW)
                        .withDeletion(new SeriesDeletion("outra-operacao", 0, List.of(SELF, OTHER), false), NOW));
            }
            return null;
        });
        handler.delete(request("k-stale"));

        // OTHER: qualquer outra exceção.
        place("k-other");
        rpc.script((node, command) -> {
            throw new IllegalArgumentException("boom");
        });
        handler.delete(request("k-other"));

        assertEquals(1, metric("seriesDelete.attemptFailed.NO_LEADER"));
        assertEquals(1, metric("seriesDelete.attemptFailed.TIMEOUT"));
        assertEquals(3, metric("seriesDelete.attemptFailed.PARTICIPANT"));
        assertEquals(1, metric("seriesDelete.attemptFailed.STALE"));
        assertEquals(1, metric("seriesDelete.attemptFailed.OTHER"));
        assertEquals(1, metric("seriesDelete.cancelled"), "só a recusa de prepare cancela com contagem");
        assertEquals(1, metric("seriesDelete.requests.REFUSED_RECENT_WRITE"));
        assertEquals(6, metric("seriesDelete.requests.ERROR"));
        assertEquals(0, metric("seriesDelete.completed"));
    }

    @Test
    void chavesObsoletasInalteradas() {
        // Contam tentativas (cliente e recuperação) diferentes de DELETED/NOT_FOUND, como na 8.14.
        place("k-refused");
        rpc.script((node, command) -> Commands.DELETE_PREPARE.equals(command)
                ? DeleteResult.of(DeleteStatus.REFUSED_RECENT_WRITE, SELF) : null);
        handler.delete(request("k-refused"));
        place("k-ok");
        rpc.script((node, command) -> null);
        handler.delete(request("k-ok"));
        handler.delete(request("k-ok"));
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
        place("k-fail");
        handler.delete(request("k-fail"));
        placePending("k-pending", true);
        handler.recover();
        handler.recover();

        Map<String, Long> metrics = lifecycle.deleteMetrics();
        assertEquals(1L, metrics.get("seriesDelete.REFUSED_RECENT_WRITE"));
        assertEquals(3L, metrics.get("seriesDelete.ERROR"), "1 do cliente e 2 da recuperação");
        assertNull(metrics.get("seriesDelete.DELETED"));
        assertNull(metrics.get("seriesDelete.NOT_FOUND"));
        assertNull(metrics.get("seriesDelete.REFUSED_MIGRATING"));
        assertEquals(2, metric("seriesDelete.recoveryAttempts"));
        assertEquals(Long.valueOf(1L), lifecycle.refusalCounts().get(DeleteStatus.REFUSED_RECENT_WRITE));
    }

    @Test
    void alarmeDePendenciaTemTaxaLimitadaEPrecisaDeCiclosSeguidos() {
        placePending(KEY, true);
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
        List<String> warnings = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(SeriesDeleteHandler.class.getName());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage() != null
                        && record.getMessage().startsWith("NGRRD_DELETE_PENDING")) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(capture);
        try {
            for (int i = 0; i < SeriesDeleteHandler.PENDING_CYCLES_BEFORE_WARNING; i++) {
                handler.recover();
            }
            assertTrue(warnings.isEmpty(), "até 5 ciclos seguidos não alarma: " + warnings);
            handler.recover();
            assertEquals(List.of("NGRRD_DELETE_PENDING count=1 oldestOperationId=op-" + KEY), warnings);
            clock.advance(Duration.ofSeconds(59));
            handler.recover();
            handler.recover();
            assertEquals(1, warnings.size(), "no máximo um por minuto");
            clock.advance(Duration.ofSeconds(1));
            handler.recover();
            assertEquals(2, warnings.size());
        } finally {
            logger.removeHandler(capture);
        }
        assertEquals(1, metric("seriesDelete.pending"));
    }

    @Test
    void ciclosSemPendenciaZeramAContagemSeguida() {
        placePending(KEY, true);
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
        List<String> warnings = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(SeriesDeleteHandler.class.getName());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage() != null
                        && record.getMessage().startsWith("NGRRD_DELETE_PENDING")) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(capture);
        try {
            for (int i = 0; i < 5; i++) {
                handler.recover();
            }
            SeriesPlacement saved = placements.remove(KEY);
            handler.recover();
            assertEquals(0, metric("seriesDelete.pending"));
            placements.put(KEY, saved);
            for (int i = 0; i < 5; i++) {
                handler.recover();
            }
        } finally {
            logger.removeHandler(capture);
        }
        assertTrue(warnings.isEmpty(), "a interrupção reinicia a contagem: " + warnings);
    }

    /** {@link Clock} controlado pelo teste. */
    private static final class MutableClock extends Clock {
        private volatile Instant instant;

        MutableClock(Instant start) {
            this.instant = start;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("não usado nos testes");
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    private static Transport fakeTransport() {
        return (Transport) Proxy.newProxyInstance(Transport.class.getClassLoader(), new Class<?>[]{Transport.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Catálogo em memória: placements e status de nós definidos pelo teste. */
    private final class CatalogFake implements CatalogView {
        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodes.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            placements.put(seriesKey, placement);
        }

        @Override
        public void removePlacement(String seriesKey) {
            placements.remove(seriesKey);
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            nodes.put(status.nodeId(), status);
        }
    }

    /**
     * {@link ClusterRpc} fake das fases da exclusão. O script devolve a resposta (ou {@code null} para o
     * padrão {@code DELETED}) ou lança a falha da fase, por nó e comando.
     */
    private static final class ScriptedRpc implements ClusterRpc {
        private volatile BiFunction<String, String, DeleteResult> script = (node, command) -> null;
        volatile boolean applyFails;

        void script(BiFunction<String, String, DeleteResult> script) {
            this.script = script;
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            DeleteResult scripted = script.apply(target.value(), command);
            DeleteResult result = scripted != null ? scripted
                    : DeleteResult.of(DeleteStatus.DELETED, ((DeleteControlRequest) body).placement().ownerNodeId());
            return responseType.cast(result);
        }

        @Override
        public NodeId localId() {
            return NodeId.of(SELF);
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.of(localId());
        }
    }
}
