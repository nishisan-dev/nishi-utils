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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.WriteMark;
import dev.nishisan.utils.oss.cluster.api.WriteMarkResult;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Marcas de escrita ({@link NgrrdClusterConfig.WriteFailureReporting#MARKS}): fronteira sem bloqueio, conclusão em
 * ordem, atribuição de cada falha a exatamente uma marca, barreiras que não lançam por falha de escrita e o contrato
 * de ciclo de vida ({@code SERIES_DELETED}/{@code QUARANTINED}).
 */
class WriteMarkTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String OWNER = "A";

    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private final Rpc rpc = new Rpc();
    private WriteDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        gates.forEach(CountDownLatch::countDown);
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.OK);
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private WriteDispatcher newDispatcher(NgrrdClusterConfig.WriteFailureReporting mode, int batchMaxSamples,
            Duration batchMaxDelay) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(20));
        dispatcher = new WriteDispatcher(rpc, new StaticPlacementLookup(), retry, batchMaxSamples, batchMaxDelay,
                10_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(3), key -> true,
                (key, owner) -> { }, Clock.systemUTC(), null, null, mode);
        return dispatcher;
    }

    private WriteDispatcher marksDispatcher() {
        return newDispatcher(NgrrdClusterConfig.WriteFailureReporting.MARKS, 1, Duration.ofMillis(10));
    }

    private CountDownLatch gate() {
        var gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    @Test
    void marcaSemPendenciasConcluiSemFalhas() throws Exception {
        marksDispatcher();
        WriteMark mark = dispatcher.mark();

        WriteMarkResult result = mark.completion().get(10, TimeUnit.SECONDS);

        assertEquals(1L, mark.id());
        assertEquals(0L, result.samplesFailed());
        assertTrue(result.succeeded());
        assertEquals(Optional.of(result), mark.result());
    }

    @Test
    void markNaoBloqueiaEConcluiSoDepoisDoAckDeTodasAsRotas() throws Exception {
        var entered = gate();
        var release = gate();
        rpc.writes = (owner, request) -> {
            if (request.writes().getFirst().seriesKey().equals("b")) {
                entered.countDown();
                awaitGate(release);
            }
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("a", 1));
        dispatcher.enqueue(OWNER, write("b", 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        WriteMark mark = CompletableFuture.supplyAsync(dispatcher::mark).get(2, TimeUnit.SECONDS);
        Await.untilTrue("ACK de a", WAIT, () -> dispatcher.samplesSent() == 1);
        assertFalse(mark.isDone());
        assertTrue(mark.result().isEmpty());

        release.countDown();
        WriteMarkResult result = mark.completion().get(10, TimeUnit.SECONDS);
        assertEquals(0L, result.samplesFailed());
        assertEquals(2L, result.samplesAdmitted());
    }

    @Test
    void marcasConcluemEmOrdem() throws Exception {
        var release = gate();
        rpc.writes = (owner, request) -> {
            if (request.writes().getFirst().seriesKey().equals("a")) {
                awaitGate(release);
            }
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("a", 1));
        WriteMark first = dispatcher.mark();
        dispatcher.enqueue(OWNER, write("b", 1));
        WriteMark second = dispatcher.mark();
        List<Long> order = new CopyOnWriteArrayList<>();
        first.completion().thenRun(() -> order.add(first.id()));
        second.completion().thenRun(() -> order.add(second.id()));

        release.countDown();
        second.completion().get(10, TimeUnit.SECONDS);
        first.completion().get(10, TimeUnit.SECONDS);

        Await.untilTrue("callbacks das duas marcas", WAIT, () -> order.size() == 2);
        assertEquals(List.of(1L, 2L), order);
    }

    @Test
    void marcasComAMesmaFronteiraNaoEmpilhamEsperaEAFalhaVaiParaAPrimeira() throws Exception {
        var release = gate();
        rpc.writes = (owner, request) -> {
            awaitGate(release);
            return respond(request, key -> SeriesStatus.ERROR);
        };
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        List<WriteMark> marks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            marks.add(dispatcher.mark());
        }

        assertEquals(1, dispatcher.markWaiters("s"), "dono parado: uma espera por rota, não uma por marca");
        assertTrue(marks.stream().noneMatch(WriteMark::isDone));
        List<Long> order = new CopyOnWriteArrayList<>();
        marks.forEach(mark -> mark.completion().thenRun(() -> order.add(mark.id())));
        release.countDown();

        assertEquals(1L, marks.getFirst().completion().get(10, TimeUnit.SECONDS).samplesFailed());
        for (WriteMark mark : marks.subList(1, marks.size())) {
            assertEquals(0L, mark.completion().get(10, TimeUnit.SECONDS).samplesFailed());
        }
        Await.untilTrue("callbacks das marcas", WAIT, () -> order.size() == marks.size());
        assertEquals(marks.stream().map(WriteMark::id).toList(), order);
        assertEquals(0, dispatcher.markWaiters("s"));
    }

    @Test
    void closeFalhaTambemAsMarcasQueDependiamDaEsperaDaAnterior() throws Exception {
        var release = gate();
        rpc.writes = (owner, request) -> {
            awaitGate(release);
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        WriteMark first = dispatcher.mark();
        WriteMark second = dispatcher.mark();

        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> dispatcher.close(Duration.ofMillis(100)));
        var firstFailure = assertThrows(ExecutionException.class, () -> first.completion().get(10, TimeUnit.SECONDS));
        var secondFailure = assertThrows(ExecutionException.class,
                () -> second.completion().get(10, TimeUnit.SECONDS));
        release.countDown();
        closing.get(10, TimeUnit.SECONDS);

        assertEquals(ErrorCode.CLOSED, assertInstanceOf(NgrrdClusterException.class, firstFailure.getCause()).code());
        assertEquals(ErrorCode.CLOSED, assertInstanceOf(NgrrdClusterException.class, secondFailure.getCause()).code());
    }

    @Test
    void cadaFalhaVaiParaAMarcaDaJanelaEmQueFoiAdmitida() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> {
            long ts = request.writes().getFirst().tsEpochMs();
            return (key.equals("a") && ts == 1) || (key.equals("b") && ts == 2) ? SeriesStatus.ERROR : SeriesStatus.OK;
        });
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("a", 1));
        dispatcher.enqueue(OWNER, write("b", 1));
        WriteMark first = dispatcher.mark();
        dispatcher.enqueue(OWNER, write("a", 2));
        dispatcher.enqueue(OWNER, write("b", 2));
        WriteMark second = dispatcher.mark();

        WriteMarkResult firstResult = first.completion().get(10, TimeUnit.SECONDS);
        WriteMarkResult secondResult = second.completion().get(10, TimeUnit.SECONDS);

        assertEquals(List.of("a"), firstResult.failedSeriesSample());
        assertEquals(1L, firstResult.samplesFailed());
        assertEquals(List.of("b"), secondResult.failedSeriesSample());
        assertEquals(1L, secondResult.samplesFailed());
    }

    @Test
    void erroEContadoNaMarcaCertaUmaUnicaVezESerieSegueAceitandoEscrita() throws Exception {
        rpc.writes = (owner, request) -> respond(request,
                key -> request.writes().getFirst().tsEpochMs() == 1 ? SeriesStatus.ERROR : SeriesStatus.OK);
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));

        WriteMarkResult first = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);
        assertEquals(1L, first.samplesFailed());
        assertEquals(Map.of(SeriesStatus.ERROR, 1L), first.failuresByStatus());
        assertEquals(List.of("s"), first.failedSeriesSample());
        assertFalse(first.failedSeriesTruncated());

        dispatcher.enqueue(OWNER, write("s", 2));
        dispatcher.flushAllSync();
        dispatcher.flushSeriesSync("s", OWNER, WAIT);

        WriteMarkResult second = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);
        assertEquals(0L, second.samplesFailed());
        assertEquals(2L, dispatcher.samplesEnqueued());
        assertEquals(1L, dispatcher.samplesSent());
    }

    @Test
    void faixaQueCruzaAFronteiraEPartidaEntreDuasMarcas() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        newDispatcher(NgrrdClusterConfig.WriteFailureReporting.MARKS, 10, Duration.ofMinutes(1));
        dispatcher.enqueue(OWNER, write("s", 1));
        dispatcher.enqueue(OWNER, write("s", 2));
        WriteMark first = dispatcher.mark();
        dispatcher.enqueue(OWNER, write("s", 3));
        WriteMark second = dispatcher.mark();

        dispatcher.flushAllSync();

        assertEquals(1, rpc.writeBatches.size(), "as três amostras saem num único lote");
        assertEquals(2L, first.completion().get(10, TimeUnit.SECONDS).samplesFailed());
        assertEquals(1L, second.completion().get(10, TimeUnit.SECONDS).samplesFailed());
    }

    @Test
    void falhaAntesDeQualquerMarcaComRotaOciosaEColhidaPelaProximaMarca() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        Await.untilTrue("ERROR final", WAIT, () -> dispatcher.samplesFailed() == 1);

        WriteMark first = dispatcher.mark();
        WriteMark second = dispatcher.mark();

        assertEquals(1L, first.completion().get(10, TimeUnit.SECONDS).samplesFailed());
        assertEquals(0L, second.completion().get(10, TimeUnit.SECONDS).samplesFailed());
    }

    @Test
    void modoMarksSerieTerminalLancaSoNaBarreiraPorSerie() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.QUARANTINED);
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        Await.untilTrue("QUARANTINED final", WAIT, () -> dispatcher.samplesFailed() == 1);

        dispatcher.flushAllSync();
        dispatcher.flushNodeSync(OWNER, WAIT);
        var perSeries = assertThrows(NgrrdClusterException.class,
                () -> dispatcher.flushSeriesSync("s", OWNER, WAIT));
        assertEquals(ErrorCode.QUARANTINED, perSeries.code());
        var admission = assertThrows(NgrrdClusterException.class, () -> dispatcher.enqueue(OWNER, write("s", 2)));
        assertEquals(ErrorCode.QUARANTINED, admission.code());

        WriteMarkResult result = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);
        assertEquals(Map.of(SeriesStatus.QUARANTINED, 1L), result.failuresByStatus());
    }

    @Test
    void modoBarrierMantemFalhaGrudentaERecusaMark() {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        newDispatcher(NgrrdClusterConfig.WriteFailureReporting.BARRIER, 1, Duration.ofMillis(10));
        dispatcher.enqueue(OWNER, write("s", 1));

        assertThrows(IllegalStateException.class, dispatcher::mark);
        assertThrows(NgrrdClusterException.class, dispatcher::flushAllSync);
        assertThrows(NgrrdClusterException.class, dispatcher::flushAllSync, "a falha continua visível");
    }

    @Test
    void redirectSoAtrasaAMarcaEPreservaAOrdemDaSerie() throws Exception {
        List<Long> arrivedAtB = new CopyOnWriteArrayList<>();
        rpc.writes = (owner, request) -> {
            if (owner.equals(OWNER)) {
                String key = request.writes().getFirst().seriesKey();
                return new WriteBatchResponse(Map.of(key, SeriesStatus.WRONG_OWNER), Map.of(key, "B"), Map.of());
            }
            request.writes().forEach(w -> arrivedAtB.add(w.tsEpochMs()));
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        for (long ts = 1; ts <= 5; ts++) {
            dispatcher.enqueue(OWNER, write("s", ts));
        }

        WriteMarkResult result = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);

        assertEquals(0L, result.samplesFailed());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), arrivedAtB);
    }

    @Test
    void geracaoSubstituidaComMarcaAbertaContaComoSeriesDeleted() throws Exception {
        var entered = gate();
        var release = gate();
        rpc.writes = (owner, request) -> {
            entered.countDown();
            awaitGate(release);
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.beginGeneration("s", "g1", OWNER);
        dispatcher.enqueue(OWNER, new SeriesWrite("s", "ds", 1, 1.0, "g1"));
        dispatcher.enqueue(OWNER, new SeriesWrite("s", "ds", 2, 2.0, "g1"));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        WriteMark mark = dispatcher.mark();

        dispatcher.beginGeneration("s", "g2", OWNER);

        WriteMarkResult result = mark.completion().get(10, TimeUnit.SECONDS);
        assertEquals(Map.of(SeriesStatus.SERIES_DELETED, 2L), result.failuresByStatus());
        release.countDown();
        assertEquals(0L, dispatcher.mark().completion().get(10, TimeUnit.SECONDS).samplesFailed(),
                "a resposta tardia da geração antiga não é contada de novo");
    }

    @Test
    void geracaoSubstituidaSemMarcaAbertaEColhidaPelaProximaMarca() throws Exception {
        var release = gate();
        rpc.writes = (owner, request) -> {
            awaitGate(release);
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.beginGeneration("s", "g1", OWNER);
        dispatcher.enqueue(OWNER, new SeriesWrite("s", "ds", 1, 1.0, "g1"));

        dispatcher.beginGeneration("s", "g2", OWNER);

        WriteMarkResult result = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);
        assertEquals(Map.of(SeriesStatus.SERIES_DELETED, 1L), result.failuresByStatus());
    }

    @Test
    void closeComMarcaPendenteFalhaComClosed() throws Exception {
        var release = gate();
        rpc.writes = (owner, request) -> {
            awaitGate(release);
            return respond(request, key -> SeriesStatus.OK);
        };
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        WriteMark mark = dispatcher.mark();
        CompletableFuture<WriteMarkResult> completion = mark.completion();

        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> dispatcher.close(Duration.ofMillis(100)));
        var failure = assertThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS));
        release.countDown();
        closing.get(10, TimeUnit.SECONDS);

        assertEquals(ErrorCode.CLOSED, assertInstanceOf(NgrrdClusterException.class, failure.getCause()).code());
        assertTrue(mark.isDone());
        assertTrue(mark.result().isEmpty());
        assertEquals(ErrorCode.CLOSED, assertThrows(NgrrdClusterException.class, dispatcher::mark).code());
    }

    @Test
    void amostraDeSeriesComFalhaELimitadaETruncada() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        marksDispatcher();
        int series = WriteMarkResult.MAX_FAILED_SERIES_SAMPLE + 50;
        for (int i = 0; i < series; i++) {
            dispatcher.enqueue(OWNER, write("s-" + i, 1));
        }

        WriteMarkResult result = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);

        assertEquals(series, result.samplesFailed());
        assertEquals(WriteMarkResult.MAX_FAILED_SERIES_SAMPLE, result.failedSeriesSample().size());
        assertTrue(result.failedSeriesTruncated());
    }

    @Test
    void callbackDaMarcaPodeEscreverEMarcarSemDeadlock() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.OK);
        marksDispatcher();
        dispatcher.enqueue(OWNER, write("s", 1));
        CompletableFuture<WriteMark> nested = dispatcher.mark().completion().thenApply(result -> {
            dispatcher.enqueue(OWNER, write("s", 2));
            return dispatcher.mark();
        });

        WriteMark next = nested.get(10, TimeUnit.SECONDS);

        assertEquals(0L, next.completion().get(10, TimeUnit.SECONDS).samplesFailed());
    }

    @Test
    void cadaFalhaEAtribuidaAExatamenteUmaMarcaSobConcorrencia() throws Exception {
        rpc.writes = (owner, request) -> respond(request,
                key -> ThreadLocalRandom.current().nextInt(10) == 0 ? SeriesStatus.ERROR : SeriesStatus.OK);
        newDispatcher(NgrrdClusterConfig.WriteFailureReporting.MARKS, 50, Duration.ofMillis(5));
        AtomicLong admitted = new AtomicLong();
        List<WriteMark> marks = new ArrayList<>();
        ExecutorService producers = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int p = 0; p < 4; p++) {
                int producer = p;
                tasks.add(producers.submit(() -> {
                    for (int i = 0; i < 2_000; i++) {
                        dispatcher.enqueue(OWNER, write("s-" + producer + "-" + (i % 37), i));
                        admitted.incrementAndGet();
                    }
                }));
            }
            while (!tasks.stream().allMatch(Future::isDone)) {
                marks.add(dispatcher.mark());
                Thread.sleep(2L);
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
        } finally {
            producers.shutdownNow();
        }
        marks.add(dispatcher.mark());

        long failed = 0L;
        long admittedByMarks = 0L;
        for (WriteMark mark : marks) {
            WriteMarkResult result = mark.completion().get(10, TimeUnit.SECONDS);
            failed += result.samplesFailed();
            admittedByMarks += result.samplesAdmitted();
        }
        assertEquals(dispatcher.samplesFailed(), failed);
        assertEquals(admitted.get(), admittedByMarks);
        assertEquals(admitted.get(), dispatcher.samplesSent() + dispatcher.samplesFailed());
    }

    // ------------------------------------------------------------- contrato de ciclo de vida no handle

    @Test
    void checkpointEmModoMarksNaoLancaPorErroDeEscrita() {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        marksDispatcher();
        RemoteSeriesHandle handle = openHandle();
        handle.write("ds", new Sample(1, 1));

        handle.checkpoint();
        handle.flush();

        assertEquals(1L, dispatcher.samplesFailed());
        assertTrue(rpc.commands.contains(Commands.CHECKPOINT + "@" + OWNER));
    }

    @Test
    void checkpointEmModoMarksLancaQuarantinedComoNgrrdClusterException() {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.QUARANTINED);
        marksDispatcher();
        RemoteSeriesHandle handle = openHandle();
        handle.write("ds", new Sample(1, 1));
        Await.untilTrue("QUARANTINED final", WAIT, () -> dispatcher.samplesFailed() == 1);

        var checkpoint = assertThrows(NgrrdClusterException.class, handle::checkpoint);
        var write = assertThrows(NgrrdClusterException.class, () -> handle.write("ds", new Sample(2, 2)));

        assertEquals(ErrorCode.QUARANTINED, checkpoint.code());
        assertEquals(ErrorCode.QUARANTINED, write.code());
    }

    @Test
    void escritaEmModoMarksLancaSeriesDeletedComoNgrrdClusterException() {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.SERIES_DELETED);
        marksDispatcher();
        RemoteSeriesHandle handle = openHandle();
        handle.write("ds", new Sample(1, 1));
        Await.untilTrue("SERIES_DELETED final", WAIT, () -> dispatcher.samplesFailed() == 1);

        var flush = assertThrows(NgrrdClusterException.class, handle::flush);
        var write = assertThrows(NgrrdClusterException.class, () -> handle.write("ds", new Sample(2, 2)));

        assertEquals(ErrorCode.SERIES_DELETED, flush.code());
        assertEquals(ErrorCode.SERIES_DELETED, write.code());
    }

    private RemoteSeriesHandle openHandle() {
        StaticPlacementLookup lookup = new StaticPlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(20));
        RemoteSeriesHandle handle = new RemoteSeriesHandle("s", "unused", "unused", Map.of(), null, lookup, rpc,
                dispatcher, retry, Duration.ofSeconds(3), Duration.ofSeconds(3), Clock.systemUTC(), (key, h) -> { },
                CapabilityFixtures.advertisingAll(), () -> false);
        handle.open();
        return handle;
    }

    // ------------------------------------------------------------- apoio

    private static SeriesWrite write(String seriesKey, long ts) {
        return new SeriesWrite(seriesKey, "ds", ts, ts);
    }

    private static WriteBatchResponse respond(WriteBatchRequest request,
            java.util.function.Function<String, SeriesStatus> statusOf) {
        Map<String, SeriesStatus> status = new LinkedHashMap<>();
        Map<String, String> errors = new LinkedHashMap<>();
        for (SeriesWrite write : request.writes()) {
            SeriesStatus value = status.computeIfAbsent(write.seriesKey(), statusOf);
            if (value != SeriesStatus.OK) {
                errors.put(write.seriesKey(), value + " simulado");
            }
        }
        return new WriteBatchResponse(status, Map.of(), errors);
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            assertTrue(gate.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class Rpc implements ClusterRpc {
        volatile BiFunction<String, WriteBatchRequest, WriteBatchResponse> writes =
                (owner, request) -> respond(request, key -> SeriesStatus.OK);
        final List<String> commands = new CopyOnWriteArrayList<>();
        final List<WriteBatchRequest> writeBatches = new CopyOnWriteArrayList<>();

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> type) {
            commands.add(command + "@" + target.value());
            if (command.equals(Commands.WRITE_BATCH)) {
                writeBatches.add((WriteBatchRequest) body);
                return type.cast(writes.apply(target.value(), (WriteBatchRequest) body));
            }
            return type.cast(new SeriesStatusResponse(SeriesStatus.OK, target.value(), null));
        }

        @Override
        public NodeId localId() {
            return NodeId.of("client");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.of(NodeId.of(OWNER));
        }
    }

    private static final class StaticPlacementLookup implements PlacementLookup {
        @Override
        public SeriesPlacement resolve(String key, String hash) {
            return SeriesPlacement.active(OWNER, 0);
        }

        @Override
        public SeriesPlacement resolveExisting(String key, Duration maxWait) {
            return resolve(key, null);
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String key, Duration maxWait) {
            return resolve(key, null);
        }

        @Override
        public Map<String, SeriesPlacement> resolveExistingAtLeader(Collection<String> keys, Duration maxWait) {
            Map<String, SeriesPlacement> found = new LinkedHashMap<>();
            keys.forEach(key -> found.put(key, resolve(key, null)));
            return found;
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String key) {
            return Optional.of(resolve(key, null));
        }

        @Override
        public void invalidate(String key) {
        }

        @Override
        public void noteOwner(String key, String owner) {
        }
    }
}
