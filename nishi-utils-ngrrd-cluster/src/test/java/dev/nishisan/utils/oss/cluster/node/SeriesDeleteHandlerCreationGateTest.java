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
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesInspectResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o gate de criação de série nova de {@link SeriesDeleteHandler}: alcançabilidade antes do RPC,
 * inspeções paralelas com prazo próprio e a precedência {@code QUARANTINED} &gt; {@code MIGRATING} &gt;
 * {@code PLACEMENT_UNAVAILABLE}. RPC, catálogo e transporte são fakes; o gate não usa o ciclo de vida local.
 */
class SeriesDeleteHandlerCreationGateTest {

    private static final String SELF = "storage-a";
    private static final String KEY = "br-sp/if-1";

    private final Map<String, StorageNodeStatus> nodes = new ConcurrentHashMap<>();
    private final Set<String> reachable = ConcurrentHashMap.newKeySet();
    private final InspectRpc rpc = new InspectRpc();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-04T12:00:00Z"));
    /** Liberado no fim de cada teste: solta as inspeções "penduradas" que o gate abandonou. */
    private final CountDownLatch hang = new CountDownLatch(1);
    private SeriesDeleteHandler handler;

    @AfterEach
    void tearDown() {
        hang.countDown();
        if (handler != null) {
            handler.close();
        }
    }

    private SeriesDeleteHandler handler(Duration inspectTimeout) {
        handler = new SeriesDeleteHandler(fakeTransport(), new NodesCatalog(), rpc, null, () -> true, () -> true,
                clock, () -> Set.copyOf(reachable), inspectTimeout);
        return handler;
    }

    private void participant(String nodeId, boolean isReachable) {
        nodes.put(nodeId, status(nodeId, StorageCapabilities.ALL));
        if (isReachable) {
            reachable.add(nodeId);
        }
    }

    private static StorageNodeStatus status(String nodeId, Set<String> capabilities) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0, 0, 0, System.currentTimeMillis(),
                DistributionMode.COUNT, 1, 0, capabilities);
    }

    private static SeriesInspectResponse clean() {
        return new SeriesInspectResponse(false, false, null, false);
    }

    @Test
    void participanteInalcancavelFicaIndisponivelSemRpc() {
        participant(SELF, false);
        participant("storage-b", true);
        participant("storage-c", false);
        rpc.respond(SELF, timeout -> clean());
        rpc.respond("storage-b", timeout -> clean());

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertEquals(SeriesStatus.PLACEMENT_UNAVAILABLE, result.status());
        assertEquals(List.of("storage-c"), result.unavailableNodeIds());
        assertFalse(rpc.targets().contains("storage-c"), "nó inalcançável não recebe RPC");
        assertTrue(rpc.targets().contains(SELF), "o próprio líder é inspecionado mesmo fora da visão de alcance");
        assertEquals(Set.of(Commands.SERIES_INSPECT), Set.copyOf(rpc.commands()));
    }

    @Test
    void noLentoFicaIndisponivelNoPrazoCurtoDeInspecaoENaoNoRequestTimeout() {
        participant(SELF, true);
        participant("storage-b", true);
        rpc.respond(SELF, timeout -> clean());
        rpc.respond("storage-b", timeout -> {
            await(hang, Duration.ofSeconds(30));
            return clean();
        });
        Duration inspectTimeout = Duration.ofMillis(200);

        long startedAt = System.nanoTime();
        var result = handler(inspectTimeout).creationGate(KEY);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertEquals(SeriesStatus.PLACEMENT_UNAVAILABLE, result.status());
        assertEquals(List.of("storage-b"), result.unavailableNodeIds());
        assertTrue(elapsed.compareTo(Duration.ofSeconds(3)) < 0, "o gate deveria voltar em ~200 ms: " + elapsed);
        assertTrue(elapsed.compareTo(inspectTimeout) >= 0, "nunca antes do prazo de inspeção: " + elapsed);
        assertEquals(Set.of(inspectTimeout), Set.copyOf(rpc.timeouts()), "cada RPC leva o prazo de inspeção");
    }

    @Test
    void falhaDeTransporteFicaIndisponivel() {
        participant(SELF, true);
        participant("storage-b", true);
        rpc.respond(SELF, timeout -> clean());
        rpc.respond("storage-b", timeout -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "falha de transporte",
                    new IOException("No connection available"));
        });

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertEquals(SeriesStatus.PLACEMENT_UNAVAILABLE, result.status());
        assertEquals(List.of("storage-b"), result.unavailableNodeIds());
    }

    @Test
    void inspecoesCorremEmParalelo() {
        // Cada inspeção só responde depois que a outra também começou: em série, a primeira esperaria a
        // segunda até desistir (e o gate a daria como indisponível).
        participant("storage-b", true);
        participant("storage-c", true);
        CountDownLatch bothStarted = new CountDownLatch(2);
        rpc.respond("storage-b", timeout -> rendezvous(bothStarted));
        rpc.respond("storage-c", timeout -> rendezvous(bothStarted));

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertTrue(result.open(), "as duas inspeções deveriam ter rodado ao mesmo tempo: " + result);
    }

    @Test
    void quarentenaTemPrecedenciaSobreRemocaoEIndisponibilidade() {
        participant("storage-b", true);
        participant("storage-c", true);
        participant("storage-d", false);
        rpc.respond("storage-b", timeout -> new SeriesInspectResponse(true, true, "g1", false));
        rpc.respond("storage-c", timeout -> new SeriesInspectResponse(true, false, "g2", true));

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertEquals(SeriesStatus.QUARANTINED, result.status());
        assertTrue(result.unavailableNodeIds().isEmpty());
    }

    @Test
    void remocaoEmCursoTemPrecedenciaSobreIndisponibilidade() {
        participant("storage-c", true);
        participant("storage-d", false);
        rpc.respond("storage-c", timeout -> new SeriesInspectResponse(true, false, "g2", true));

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertEquals(SeriesStatus.MIGRATING, result.status());
        assertTrue(result.unavailableNodeIds().isEmpty());
    }

    @Test
    void indisponiveisVemOrdenadosEUnicos() {
        participant("storage-e", false);
        participant("storage-c", true);
        participant("storage-d", false);
        rpc.respond("storage-c", timeout -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "tempo esgotado");
        });

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertEquals(List.of("storage-c", "storage-d", "storage-e"), result.unavailableNodeIds());
    }

    @Test
    void nosLegadosSemSeriesDeleteFicamDeForaMesmoInalcancaveis() {
        participant("storage-b", true);
        nodes.put("storage-old", status("storage-old", Set.of()));
        rpc.respond("storage-b", timeout -> clean());

        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertTrue(result.open());
        assertEquals(List.of("storage-b"), rpc.targets());
    }

    @Test
    void semParticipantesOGateFicaAberto() {
        var result = handler(Duration.ofSeconds(2)).creationGate(KEY);

        assertTrue(result.open());
        assertTrue(rpc.targets().isEmpty());
    }

    @Test
    void logDePlacementUnavailableTemTaxaLimitadaEContaOsSuprimidos() {
        participant("storage-b", false);
        SeriesDeleteHandler gate = handler(Duration.ofSeconds(2));
        List<String> warnings = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(SeriesDeleteHandler.class.getName());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage() != null
                        && record.getMessage().startsWith("NGRRD_PLACEMENT_UNAVAILABLE")) {
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
            gate.creationGate("s1");
            clock.advance(Duration.ofSeconds(5));
            gate.creationGate("s2");
            gate.creationGate("s3");
            participant("storage-c", false);
            clock.advance(Duration.ofMillis(4_999));
            gate.creationGate("s4");
            clock.advance(Duration.ofMillis(1));
            gate.creationGate("s5");
        } finally {
            logger.removeHandler(capture);
        }

        assertEquals(List.of(
                "NGRRD_PLACEMENT_UNAVAILABLE series=s1 nodes=[storage-b] suppressed=0",
                "NGRRD_PLACEMENT_UNAVAILABLE series=s5 nodes=[storage-b, storage-c] suppressed=3"), warnings);
    }

    private static SeriesInspectResponse rendezvous(CountDownLatch bothStarted) {
        bothStarted.countDown();
        if (!await(bothStarted, Duration.ofSeconds(1))) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "a outra inspeção não começou");
        }
        return clean();
    }

    private static boolean await(CountDownLatch latch, Duration timeout) {
        try {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrompido", e);
        }
    }

    /** {@link Clock} controlado pelo teste (janela do log com taxa limitada). */
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

    /** {@link Transport} que só existe para satisfazer o construtor: o gate não o usa. */
    private static Transport fakeTransport() {
        return (Transport) Proxy.newProxyInstance(Transport.class.getClassLoader(), new Class<?>[]{Transport.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /** {@link CatalogView} que só expõe os status de storage registrados no teste. */
    private final class NodesCatalog implements CatalogView {
        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            throw new UnsupportedOperationException("não usado pelo gate");
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            throw new UnsupportedOperationException("não usado pelo gate");
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodes.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            throw new UnsupportedOperationException("não usado pelo gate");
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado pelo gate");
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("não usado pelo gate");
        }
    }

    /** Resposta programada de {@code ngrrd.series.inspect} para um nó, em função do prazo recebido. */
    @FunctionalInterface
    private interface InspectResponder {
        SeriesInspectResponse respond(Duration timeout);
    }

    /** {@link ClusterRpc} fake: responde {@code ngrrd.series.inspect} por nó e grava alvo, comando e prazo. */
    private static final class InspectRpc implements ClusterRpc {
        private final Map<String, InspectResponder> responders = new ConcurrentHashMap<>();
        private final List<String> targets = new CopyOnWriteArrayList<>();
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private final List<Duration> timeouts = new CopyOnWriteArrayList<>();

        void respond(String nodeId, InspectResponder responder) {
            responders.put(nodeId, responder);
        }

        List<String> targets() {
            return List.copyOf(targets);
        }

        List<String> commands() {
            return List.copyOf(commands);
        }

        List<Duration> timeouts() {
            return List.copyOf(timeouts);
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            throw new AssertionError("o gate deve sempre passar o prazo de inspeção");
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType, Duration timeout) {
            targets.add(target.value());
            commands.add(command);
            timeouts.add(timeout);
            assertEquals(KEY, ((SeriesCommandRequest) body).seriesKey());
            InspectResponder responder = responders.get(target.value());
            if (responder == null) {
                throw new AssertionError("RPC inesperado para " + target);
            }
            return responseType.cast(responder.respond(timeout));
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
