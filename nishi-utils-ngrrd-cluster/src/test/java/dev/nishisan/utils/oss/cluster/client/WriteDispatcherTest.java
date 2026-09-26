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
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link WriteDispatcher} com {@link RecordingClusterRpc} fake: batching
 * por tamanho e por tempo, ordem preservada, re-roteio em {@code WRONG_OWNER},
 * reabertura em {@code NOT_OPEN}, contagem de falhas, backpressure
 * ({@code BLOCK}/{@code FAIL}), recuperação de falha de transporte e
 * drenagem no {@code close()}.
 */
class WriteDispatcherTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final NodeId OWNER_A = NodeId.of("storage-a");
    private static final NodeId OWNER_B = NodeId.of("storage-b");
    private static final NodeId OWNER_C = NodeId.of("storage-c");
    private static final NodeId OWNER_D = NodeId.of("storage-d");

    private RecordingClusterRpc rpc;
    private FakePlacementLookup placementLookup;
    private WriteDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private WriteDispatcher newDispatcher(int batchMaxSamples, Duration batchMaxDelay, long maxBufferedPerNode,
            NgrrdClusterConfig.BufferFullPolicy policy, Function<String, Boolean> reopener) {
        return newDispatcher(batchMaxSamples, batchMaxDelay, maxBufferedPerNode, policy, reopener,
                new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(200)));
    }

    private WriteDispatcher newDispatcher(int batchMaxSamples, Duration batchMaxDelay, long maxBufferedPerNode,
            NgrrdClusterConfig.BufferFullPolicy policy, Function<String, Boolean> reopener, RetryPolicy retry) {
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        placementLookup = new FakePlacementLookup();
        dispatcher = new WriteDispatcher(rpc, placementLookup, retry, batchMaxSamples, batchMaxDelay,
                maxBufferedPerNode, policy, Duration.ofSeconds(5), reopener, Clock.systemUTC());
        return dispatcher;
    }

    private static SeriesWrite write(String seriesKey, long ts, double value) {
        return new SeriesWrite(seriesKey, "in_octets", ts, value);
    }

    private static WriteBatchResponse okFor(WriteBatchRequest request) {
        Map<String, SeriesStatus> status = new LinkedHashMap<>();
        for (SeriesWrite w : request.writes()) {
            status.put(w.seriesKey(), SeriesStatus.OK);
        }
        return new WriteBatchResponse(status, Map.of(), Map.of());
    }

    @Test
    void fourSlowNodesCannotStarveAnotherNode() throws Exception {
        newDispatcher(1, Duration.ofMillis(10), 100, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        var started = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        rpc.respondDefault((cmd, body) -> {
            var request = (WriteBatchRequest) body;
            if (request.writes().getFirst().seriesKey().startsWith("slow")) {
                started.countDown();
                awaitLatch(release);
            }
            return okFor(request);
        });
        try {
            for (int i = 0; i < 4; i++) { dispatcher.enqueue("node-" + i, write("slow-" + i, 1, 1)); }
            assertTrue(started.await(2, TimeUnit.SECONDS));
            dispatcher.enqueue("healthy-node", write("healthy", 1, 1));
            dispatcher.flushSeriesSync("healthy", "healthy-node", Duration.ofSeconds(2));
            assertEquals(1, dispatcher.samplesSent());
        } finally { release.countDown(); }
        dispatcher.flushAllSync();
        assertEquals(5, dispatcher.samplesSent());
    }

    @Test
    void pausedSeriesStillCountsAgainstBufferCapacity() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        AtomicBoolean open = new AtomicBoolean();
        newDispatcher(1, Duration.ofMillis(10), 2, NgrrdClusterConfig.BufferFullPolicy.FAIL, key -> {
            entered.countDown(); awaitLatch(release); open.set(true); return true;
        });
        rpc.respondDefault((cmd, body) -> open.get() ? okFor((WriteBatchRequest) body)
                : new WriteBatchResponse(Map.of("s1", SeriesStatus.NOT_OPEN), Map.of(), Map.of()));
        try {
            dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            dispatcher.enqueue(OWNER_A.value(), write("s1", 2, 2));
            assertEquals(2L, dispatcher.bufferedSamples().get(OWNER_A.value()));
            assertEquals(ErrorCode.BUFFER_FULL, assertThrows(NgrrdClusterException.class,
                    () -> dispatcher.enqueue(OWNER_A.value(), write("s1", 3, 3))).code());
        } finally { release.countDown(); }
        dispatcher.flushAllSync();
        assertEquals(2, dispatcher.samplesSent());
    }

    @Test
    void redirectCannotWakeSeriesWhileItsTargetOpenIsStillPending() throws Exception {
        rpc = new RecordingClusterRpc(NodeId.of("client"));
        placementLookup = new FakePlacementLookup();
        var redirected = new CountDownLatch(1);
        var releaseRedirect = new CountDownLatch(1);
        var reopening = new CountDownLatch(1);
        var releaseOpen = new CountDownLatch(1);
        var open = new AtomicBoolean();
        var opens = new AtomicInteger();
        dispatcher = new WriteDispatcher(rpc, placementLookup,
                new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(100)),
                1, Duration.ofMillis(10), 100, NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5),
                key -> { opens.incrementAndGet(); reopening.countDown(); awaitLatch(releaseOpen); open.set(true); return true; },
                (key, owner) -> { redirected.countDown(); awaitLatch(releaseRedirect); }, Clock.systemUTC(), null, null);
        rpc.respondNext((cmd, body) -> new WriteBatchResponse(Map.of("s1", SeriesStatus.WRONG_OWNER),
                Map.of("s1", OWNER_B.value()), Map.of()));
        rpc.respondDefault((cmd, body) -> open.get() ? okFor((WriteBatchRequest) body)
                : new WriteBatchResponse(Map.of("s1", SeriesStatus.NOT_OPEN), Map.of(), Map.of()));
        try {
            dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
            assertTrue(redirected.await(2, TimeUnit.SECONDS));
            assertTrue(reopening.await(2, TimeUnit.SECONDS));
            releaseRedirect.countDown();
            Thread.sleep(150);
            assertEquals(1, opens.get(), "a delayed redirect must not overwrite the target's OPEN wait");
        } finally { releaseRedirect.countDown(); releaseOpen.countDown(); }
        dispatcher.flushAllSync();
        assertEquals(1, dispatcher.samplesSent());
    }

    @Test
    void migratingSeriesDoesNotBlockHealthySeriesAndRetainsItsFifo() throws Exception {
        newDispatcher(2, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicBoolean migrating = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        List<Long> acceptedMoving = new CopyOnWriteArrayList<>();
        rpc.respondDefault((cmd, body) -> {
            if (first.compareAndSet(true, false)) { started.countDown(); awaitLatch(release); }
            var req = (WriteBatchRequest) body;
            Map<String, SeriesStatus> statuses = new LinkedHashMap<>();
            for (var write : req.writes()) {
                boolean blocked = write.seriesKey().equals("moving") && migrating.get();
                statuses.put(write.seriesKey(), blocked ? SeriesStatus.MIGRATING : SeriesStatus.OK);
                if (write.seriesKey().equals("moving") && !blocked) { acceptedMoving.add(write.tsEpochMs()); }
            }
            return new WriteBatchResponse(statuses, Map.of(), Map.of());
        });
        try {
            dispatcher.enqueue(OWNER_A.value(), write("moving", 1, 1));
            dispatcher.enqueue(OWNER_A.value(), write("moving", 2, 2));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            for (int i = 1; i <= 100; i++) {
                dispatcher.enqueue(OWNER_A.value(), write("healthy", i, i));
                dispatcher.enqueue(OWNER_A.value(), write("moving", i + 2, i));
            }
            release.countDown();
            dispatcher.flushSeriesSync("healthy", OWNER_A.value(), Duration.ofSeconds(2));
            assertEquals(100, dispatcher.samplesSent(), "healthy series must drain before migration ends");
            assertEquals(0, dispatcher.samplesFailed());
            assertThrows(NgrrdClusterException.class, () -> dispatcher.flushSeriesSync(
                    "moving", OWNER_A.value(), Duration.ofMillis(30)), "unacknowledged writes cannot pass a checkpoint");
            migrating.set(false);
            dispatcher.flushAllSync();
            assertEquals(java.util.stream.LongStream.rangeClosed(1, 102).boxed().toList(), acceptedMoving);
            assertEquals(202, dispatcher.samplesSent());
        } finally { migrating.set(false); release.countDown(); }
    }

    @Test
    void slowReopenDoesNotOccupyTheNodeDrain() throws Exception {
        CountDownLatch reopening = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean open = new AtomicBoolean(false);
        newDispatcher(1, Duration.ofMillis(10), 100, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> {
            reopening.countDown(); awaitLatch(release); open.set(true); return true;
        });
        rpc.respondDefault((cmd, body) -> {
            var req = (WriteBatchRequest) body;
            return req.writes().getFirst().seriesKey().equals("cold") && !open.get()
                    ? new WriteBatchResponse(Map.of("cold", SeriesStatus.NOT_OPEN), Map.of(), Map.of()) : okFor(req);
        });
        try {
            dispatcher.enqueue(OWNER_A.value(), write("cold", 1, 1));
            assertTrue(reopening.await(2, TimeUnit.SECONDS));
            dispatcher.enqueue(OWNER_A.value(), write("healthy", 1, 1));
            dispatcher.flushSeriesSync("healthy", OWNER_A.value(), Duration.ofSeconds(2));
            assertEquals(1, dispatcher.samplesSent());
        } finally { release.countDown(); }
        dispatcher.flushAllSync();
        assertEquals(2, dispatcher.samplesSent());
    }

    @Test
    void enviaLotePorTamanhoAssimQueAtingeBatchMaxSamples() {
        newDispatcher(3, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 3L, 3.0));

        // Espera o EFEITO (samplesSent), não só a chamada ter sido registrada: countWriteBatchCalls()
        // conta a partir do momento em que RecordingClusterRpc.call é invocado, antes da resposta ser
        // processada por applyStatus — esperar só por isso é uma corrida (a asserção seguinte podia
        // rodar antes do samplesSentCount.add ter acontecido).
        Await.untilTrue("lote de 3 amostras confirmado", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 3L);
        assertEquals(1L, countWriteBatchCalls());
    }

    @Test
    void enviaLotePorTempoMesmoComLoteIncompleto() {
        newDispatcher(100, Duration.ofMillis(50), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0));

        Await.untilTrue("lote incompleto confirmado pelo tick", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 2L);
        assertTrue(countWriteBatchCalls() >= 1);
    }

    @Test
    void preservaAOrdemDasAmostrasDentroDoLote() {
        newDispatcher(5, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        for (int i = 0; i < 5; i++) {
            dispatcher.enqueue(OWNER_A.value(), write("s1", i, i));
        }

        Await.untilTrue("lote enviado", AWAIT_TIMEOUT, () -> countWriteBatchCalls() == 1);
        WriteBatchRequest sent = (WriteBatchRequest) rpc.calls().get(0).body();
        List<Double> values = sent.writes().stream().map(SeriesWrite::value).toList();
        assertEquals(List.of(0.0, 1.0, 2.0, 3.0, 4.0), values);
    }

    @Test
    void wrongOwnerReRoteiaParaONovoDonoENotificaOResolver() {
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondNext((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().get(0).seriesKey();
            return new WriteBatchResponse(Map.of(key, SeriesStatus.WRONG_OWNER),
                    Map.of(key, OWNER_B.value()), Map.of());
        });
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 42.0));

        Await.untilTrue("re-roteado e confirmado no novo dono", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertTrue(placementLookup.notedOwners.contains("s1=" + OWNER_B.value()));
        assertTrue(rpc.calls().stream().anyMatch(c -> c.target().equals(OWNER_B)));
    }

    @Test
    void wrongOwnerLevaJuntoOBacklogDaMesmaSerieEChegaAoNovoDonoEmOrdemCrescente() throws InterruptedException {
        // B4 (achado do Refuter): ao rerotear a série por WRONG_OWNER, TODAS as escritas da MESMA série
        // ainda na fila do dono antigo (backlog de lotes seguintes, ainda não enviados) devem seguir
        // junto — na ordem certa — para o novo dono. Sem a correção, esse backlog seria enviado depois,
        // ao dono ERRADO, e o novo dono receberia primeiro o que havia sido enfileirado por último.
        newDispatcher(1, Duration.ofMillis(50), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        rpc.respondDefault((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            SeriesWrite w = req.writes().get(0);
            if (firstCall.compareAndSet(true, false)) {
                // Segura a 1a chamada (o lote com ts=1) até o teste enfileirar as outras 2 escritas —
                // com batchMaxSamples=1 e o dono A ainda "em voo" (inFlight), elas ficam paradas na
                // fila de A em vez de disparar novos lotes, exatamente o backlog que o B4 precisa mover.
                try {
                    assertTrue(releaseFirstCall.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return new WriteBatchResponse(Map.of(w.seriesKey(), SeriesStatus.WRONG_OWNER),
                        Map.of(w.seriesKey(), OWNER_B.value()), Map.of());
            }
            return okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));
        Await.untilTrue("primeira chamada em andamento", AWAIT_TIMEOUT, () -> countWriteBatchCalls() >= 1);
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 3L, 3.0));
        releaseFirstCall.countDown();

        Await.untilTrue("as 3 amostras confirmadas no novo dono", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 3L);

        List<Long> timestampsAtNewOwner = rpc.calls().stream()
                .filter(c -> c.target().equals(OWNER_B) && c.command().equals(Commands.WRITE_BATCH))
                .map(c -> ((WriteBatchRequest) c.body()).writes().get(0).tsEpochMs())
                .toList();
        assertEquals(List.of(1L, 2L, 3L), timestampsAtNewOwner,
                "o novo dono deveria receber as amostras em ordem estritamente crescente de timestamp");
    }

    @Test
    void redirectSerializesNewAdmissionsAndIgnoresCapturedOldOwner() throws Exception {
        rpc = new RecordingClusterRpc(NodeId.of("client"));
        placementLookup = new FakePlacementLookup();
        var callbackEntered = new CountDownLatch(1);
        var releaseCallback = new CountDownLatch(1);
        var producerStarted = new CountDownLatch(1);
        dispatcher = new WriteDispatcher(rpc, placementLookup,
                new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(50)),
                1, Duration.ofMillis(20), 100, NgrrdClusterConfig.BufferFullPolicy.BLOCK,
                Duration.ofSeconds(5), key -> true, (key, owner) -> {
                    callbackEntered.countDown();
                    awaitLatch(releaseCallback);
                }, Clock.systemUTC(), null, null);
        rpc.respondNext((cmd, body) -> new WriteBatchResponse(Map.of("s1", SeriesStatus.WRONG_OWNER),
                Map.of("s1", OWNER_B.value()), Map.of()));
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        try {
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
            var producer = CompletableFuture.runAsync(() -> {
                producerStarted.countDown();
                dispatcher.enqueue(OWNER_B.value(), write("s1", 2, 2));
            });
            assertTrue(producerStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> producer.get(100, TimeUnit.MILLISECONDS),
                    "a troca de dono ainda não liberou a admissão da série");
            releaseCallback.countDown();
            producer.get(5, TimeUnit.SECONDS);
            // Simulates a producer that captured A before the redirect but enqueues afterwards.
            dispatcher.enqueue(OWNER_A.value(), write("s1", 3, 3));
            Await.untilTrue("três escritas confirmadas", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 3);
            assertEquals(List.of(1L, 2L, 3L), timestampsAt(OWNER_B));
            assertEquals(List.of(1L), timestampsAt(OWNER_A));
        } finally {
            releaseCallback.countDown();
        }
    }

    @Test
    void blockedProducerRechecksRouteAfterRedirectFreesOldBuffer() throws Exception {
        newDispatcher(1, Duration.ofMillis(20), 1, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        var firstEntered = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        rpc.respondNext((cmd, body) -> {
            firstEntered.countDown();
            awaitLatch(releaseFirst);
            return new WriteBatchResponse(Map.of("s1", SeriesStatus.WRONG_OWNER),
                    Map.of("s1", OWNER_B.value()), Map.of());
        });
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        try {
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
            dispatcher.enqueue(OWNER_A.value(), write("s1", 2, 2));
            var third = CompletableFuture.runAsync(() -> dispatcher.enqueue(OWNER_A.value(), write("s1", 3, 3)));
            assertThrows(TimeoutException.class, () -> third.get(100, TimeUnit.MILLISECONDS));
            releaseFirst.countDown();
            third.get(5, TimeUnit.SECONDS);
            Await.untilTrue("backlog e produtor drenados", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 3);
            assertEquals(List.of(1L, 2L, 3L), timestampsAt(OWNER_B));
        } finally {
            releaseFirst.countDown();
        }
    }

    private List<Long> timestampsAt(NodeId owner) {
        return rpc.calls().stream().filter(c -> c.target().equals(owner))
                .flatMap(c -> ((WriteBatchRequest) c.body()).writes().stream())
                .map(SeriesWrite::tsEpochMs).toList();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void wrongOwnerComDonoConhecidoNotificaOCallbackOwnerChanged() {
        // M3 (nota do Refuter do M1c): sem este callback, RemoteSeriesHandle.owner nunca mudava por
        // este caminho — verifica que o WriteDispatcher o chama com (seriesKey, novoDono) sempre que
        // WRONG_OWNER já traz o dono novo.
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        placementLookup = new FakePlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(200));
        List<String> ownerChangedCalls = new CopyOnWriteArrayList<>();
        BiConsumer<String, String> ownerChanged = (seriesKey, newOwner) ->
                ownerChangedCalls.add(seriesKey + "=" + newOwner);
        dispatcher = new WriteDispatcher(rpc, placementLookup, retry, 1, Duration.ofSeconds(30), 1_000,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5), key -> true, ownerChanged,
                Clock.systemUTC(), null, null);
        rpc.respondNext((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().get(0).seriesKey();
            return new WriteBatchResponse(Map.of(key, SeriesStatus.WRONG_OWNER),
                    Map.of(key, OWNER_B.value()), Map.of());
        });
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 42.0));

        Await.untilTrue("re-roteado e confirmado no novo dono", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertEquals(List.of("s1=" + OWNER_B.value()), ownerChangedCalls);
    }

    @Test
    void wrongOwnerSemDonoInformadoInvalidaEChamaOReopenerAntesDeReenviarAoMesmoNo() {
        // Regressão F1.1 (achado do Debugger): WRONG_OWNER sem ownerBySeries não pode virar um loop
        // silencioso que só reenvia pro mesmo nó sem NUNCA chamar o reopener — era exatamente esse o
        // sintoma que travava o WRITE_BATCH pós-restart por vários minutos.
        AtomicInteger reopenCalls = new AtomicInteger();
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> {
            reopenCalls.incrementAndGet();
            return true;
        });
        rpc.respondNext((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().get(0).seriesKey();
            return new WriteBatchResponse(Map.of(key, SeriesStatus.WRONG_OWNER), Map.of(), Map.of());
        });
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 3.0));

        Await.untilTrue("lote drena após WRONG_OWNER sem dono", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertEquals(1, reopenCalls.get(), "o reopener deveria ter sido chamado (mesmo caminho do NOT_OPEN)");
        assertTrue(placementLookup.invalidated.contains("s1"), "o placement conhecido deveria ter sido invalidado");
        assertTrue(rpc.calls().stream().allMatch(c -> c.target().equals(OWNER_A)),
                "sem dono novo informado, o reenvio é sempre ao MESMO nó");
    }

    @Test
    void notOpenChamaOReopenerEReenviaAoMesmoNo() {
        AtomicInteger reopenCalls = new AtomicInteger();
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> {
            reopenCalls.incrementAndGet();
            return true;
        });
        rpc.respondNext((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().get(0).seriesKey();
            return new WriteBatchResponse(Map.of(key, SeriesStatus.NOT_OPEN), Map.of(), Map.of());
        });
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 7.0));

        Await.untilTrue("reenviado após NOT_OPEN", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertEquals(1, reopenCalls.get());
        assertTrue(dispatcher.retriesByStatus().getOrDefault(SeriesStatus.NOT_OPEN, 0L) >= 1L);
    }

    @Test
    void errorContaSamplesFailedEDescartaOLote() {
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().get(0).seriesKey();
            return new WriteBatchResponse(Map.of(key, SeriesStatus.ERROR), Map.of(), Map.of(key, "boom"));
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));

        Await.untilTrue("amostra contabilizada como falha", AWAIT_TIMEOUT, () -> dispatcher.samplesFailed() == 1L);
        assertEquals(0L, dispatcher.samplesSent());
    }

    @Test
    void blockBloqueiaProdutorAteFlushLiberarEspacoNoBuffer() throws InterruptedException {
        newDispatcher(100, Duration.ofMillis(30), 2, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0));

        AtomicBoolean thirdEnqueued = new AtomicBoolean(false);
        Thread producer = new Thread(() -> {
            dispatcher.enqueue(OWNER_A.value(), write("s1", 3L, 3.0));
            thirdEnqueued.set(true);
        }, "test-blocked-producer");
        producer.start();

        Await.untilTrue("terceira amostra enfileirada após o flush liberar espaço", AWAIT_TIMEOUT,
                thirdEnqueued::get);
        producer.join(AWAIT_TIMEOUT.toMillis());
        Await.untilTrue("todas as amostras confirmadas", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 3L);
    }

    @Test
    void failLancaBufferFullImediatamenteQuandoOBufferEstaCheio() {
        newDispatcher(100, Duration.ofSeconds(30), 1, NgrrdClusterConfig.BufferFullPolicy.FAIL, key -> true);
        // Sem responder nada: a 1a amostra fica parada no buffer (batchMaxSamples=100, sem tick a tempo).
        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0)));
        assertEquals(ErrorCode.BUFFER_FULL, ex.code());
    }

    @Test
    void falhaDeTransporteDevolveOLoteAoBufferEReenviaDepoisDoBackoff() {
        newDispatcher(1, Duration.ofMillis(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);
        rpc.respondDefault((cmd, body) -> {
            if (firstAttempt.compareAndSet(true, false)) {
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado");
            }
            return okFor((WriteBatchRequest) body);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 9.0));

        Await.untilTrue("amostra reenviada com sucesso após a falha de transporte", AWAIT_TIMEOUT,
                () -> dispatcher.samplesSent() == 1L);
        assertEquals(0L, dispatcher.samplesFailed());
        assertTrue(countWriteBatchCalls() >= 2);
    }

    @Test
    void closeDrenaAsPendenciasAntesDeEncerrar() {
        newDispatcher(1_000, Duration.ofSeconds(30), 10_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));

        for (int i = 0; i < 10; i++) {
            dispatcher.enqueue(OWNER_A.value(), write("s1", i, i));
        }

        dispatcher.close();

        assertEquals(10L, dispatcher.samplesSent());
        assertEquals(0L, dispatcher.samplesFailed());
    }

    @Test
    void onClientMetricsEhChamadoPeriodicamentePeloTickLoop() {
        // M4 (achado do Refuter): batchMaxDelay bem curto -> METRICS_TICK_INTERVAL (50) ticks do
        // tickLoop acontecem em bem menos de um segundo, gatilho testável sem Thread.sleep fixo (só o
        // polling de Await, que já é o padrão desta suíte).
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        placementLookup = new FakePlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(200));
        AtomicInteger onClientMetricsCalls = new AtomicInteger();
        ClientMetricsSnapshot fixedSnapshot = new ClientMetricsSnapshot(0L, 0L, 0L, 0L, Map.of(), Map.of(), 0,
                LatencySnapshot.EMPTY, 0L);
        NgrrdClusterMetricsListener listener = new NgrrdClusterMetricsListener() {
            @Override
            public void onClientMetrics(ClientMetricsSnapshot snapshot) {
                onClientMetricsCalls.incrementAndGet();
            }
        };
        dispatcher = new WriteDispatcher(rpc, placementLookup, retry, 500, Duration.ofMillis(5), 100_000L,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5), key -> false, Clock.systemUTC(),
                listener, () -> fixedSnapshot);

        Await.untilTrue("onClientMetrics chamado ao menos uma vez", AWAIT_TIMEOUT,
                () -> onClientMetricsCalls.get() >= 1);
    }

    @Test
    void onClientMetricsQueLancaErrorNaoImpedeODispatcherDeContinuarDrenando() {
        // M3 (achado do Refuter): publishMetricsQuietly agora captura Throwable, não só
        // RuntimeException — o tickLoop chama esse método direto, sem outro try/catch em volta, então
        // um Error do listener escapando terminaria a thread ngrrd-write-dispatcher silenciosamente.
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        placementLookup = new FakePlacementLookup();
        rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(200));
        NgrrdClusterMetricsListener throwingListener = new NgrrdClusterMetricsListener() {
            @Override
            public void onClientMetrics(ClientMetricsSnapshot snapshot) {
                throw new Error("listener quebrado de propósito (M3)");
            }
        };
        ClientMetricsSnapshot fixedSnapshot = new ClientMetricsSnapshot(0L, 0L, 0L, 0L, Map.of(), Map.of(), 0,
                LatencySnapshot.EMPTY, 0L);
        dispatcher = new WriteDispatcher(rpc, placementLookup, retry, 500, Duration.ofMillis(5), 100_000L,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5), key -> false, Clock.systemUTC(),
                throwingListener, () -> fixedSnapshot);

        // Menos amostras que batchMaxSamples (500): só o flush por TEMPO do tickLoop as envia. Se o
        // Error escapado de onClientMetrics tivesse matado a thread ngrrd-write-dispatcher (o bug
        // antes desta correção), este lote incompleto nunca seria drenado.
        dispatcher.enqueue(OWNER_A.value(), write("s1", 1L, 1.0));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2L, 2.0));

        Await.untilTrue("lote incompleto drenado mesmo com o listener de métricas lançando Error repetidamente",
                AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 2L);
    }

    @Test
    void dicasAlternadasAeCDisparamConsultaAoLiderEConvergem() {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicInteger callsBeforeLookup = new AtomicInteger(-1);
        AtomicBoolean lookedUp = new AtomicBoolean();
        placementLookup.atLeader = keys -> {
            callsBeforeLookup.compareAndSet(-1, rpc.calls().size());
            lookedUp.set(true);
            return Map.of("s1", SeriesPlacement.active(OWNER_C.value(), 1L));
        };
        // storage-a (origem, já esquecida) aponta o destino; storage-c (réplica atrasada) aponta a origem —
        // o pingue-pongue da #177, que só termina quando o cliente pergunta ao líder.
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) {
                return moved(req, OWNER_C);
            }
            return lookedUp.get() ? okFor(req) : moved(req, OWNER_A);
        });

        for (int i = 1; i <= 5; i++) {
            dispatcher.enqueue(OWNER_A.value(), write("s1", i, i));
        }

        Await.untilTrue("todas as amostras confirmadas", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 5L);
        assertTrue(placementLookup.atLeaderCalls.get() >= 1, "deveria ter consultado o líder");
        assertEquals(0L, dispatcher.samplesFailed());
        List<RecordingClusterRpc.Recorded> afterLookup = rpc.calls().subList(callsBeforeLookup.get(), rpc.calls().size());
        assertTrue(afterLookup.stream().noneMatch(c -> c.target().equals(OWNER_A)),
                "depois da resposta do líder nenhum WRITE_BATCH volta a storage-a");
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), afterLookup.stream()
                .flatMap(c -> ((WriteBatchRequest) c.body()).writes().stream()).map(SeriesWrite::tsEpochMs).toList(),
                "a série chega ao dono confirmado em ordem");
        assertTrue(dispatcher.ownerLookups() >= 1);
        assertTrue(dispatcher.redirectCycles() >= 1);
    }

    @Test
    void wrongOwnerApontandoOProprioNoNaoGiraEmLoop() throws Exception {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        placementLookup.atLeader = keys -> Map.of("s1", SeriesPlacement.active(OWNER_A.value(), 1L));
        AtomicBoolean accept = new AtomicBoolean();
        rpc.respondByTarget((target, cmd, body) -> accept.get() ? okFor((WriteBatchRequest) body)
                : moved((WriteBatchRequest) body, OWNER_A));

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        Thread.sleep(500);

        long batches = countWriteBatchCalls();
        assertTrue(batches <= 8, "WRONG_OWNER apontando o próprio nó não pode virar laço quente: " + batches);
        assertTrue(placementLookup.atLeaderCalls.get() >= 1, "a contradição deveria consultar o líder");
        accept.set(true);
        dispatcher.flushAllSync();
        assertEquals(1L, dispatcher.samplesSent());
    }

    @Test
    void backoffCresceEntreSaltos() {
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true,
                new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(40), Duration.ofSeconds(2)));
        List<Long> sentAtNanos = new CopyOnWriteArrayList<>();
        rpc.respondByTarget((target, cmd, body) -> {
            sentAtNanos.add(System.nanoTime());
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_B);
            if (target.equals(OWNER_B)) return moved(req, OWNER_C);
            if (target.equals(OWNER_C)) return moved(req, OWNER_D);
            return okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        dispatcher.flushAllSync();

        assertEquals(1L, dispatcher.samplesSent());
        assertEquals(4, sentAtNanos.size());
        List<Long> gapsMs = new ArrayList<>();
        for (int i = 1; i < sentAtNanos.size(); i++) {
            gapsMs.add(Duration.ofNanos(sentAtNanos.get(i) - sentAtNanos.get(i - 1)).toMillis());
        }
        assertTrue(gapsMs.get(0) >= 35 && gapsMs.get(1) >= 75 && gapsMs.get(2) >= 155,
                "backoff por série deveria dobrar a cada salto (40/80/160 ms): " + gapsMs);
        assertEquals(0, placementLookup.atLeaderCalls.get(), "cadeia com nós distintos não consulta o líder");
    }

    @Test
    void consultaFalhaMantemSeriePausadaSemBloquearOutras() throws Exception {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        CountDownLatch lookupEntered = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        placementLookup.atLeader = keys -> {
            lookupEntered.countDown();
            awaitLatch(releaseLookup);
            throw new NgrrdClusterException(ErrorCode.NO_LEADER, "sem líder (simulado)");
        };
        AtomicBoolean stuckAccepted = new AtomicBoolean();
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            String key = req.writes().getFirst().seriesKey();
            return key.equals("stuck") && !stuckAccepted.get() ? moved(req, OWNER_A) : okFor(req);
        });
        try {
            dispatcher.enqueue(OWNER_A.value(), write("stuck", 1, 1));
            assertTrue(lookupEntered.await(5, TimeUnit.SECONDS), "a contradição deveria consultar o líder");
            dispatcher.enqueue(OWNER_A.value(), write("healthy", 1, 1));
            dispatcher.flushSeriesSync("healthy", OWNER_A.value(), Duration.ofSeconds(2));
            assertEquals(1L, dispatcher.samplesSent(), "a série saudável no mesmo nó é confirmada");
            assertEquals(1L, writeBatchesFor("stuck"), "com a consulta em andamento, a série fica pausada");
        } finally {
            releaseLookup.countDown();
        }
        // Falha da consulta: a série continua no dono atual, com backoff, e volta a tentar.
        Await.untilTrue("série volta a ser tentada após a falha da consulta", AWAIT_TIMEOUT,
                () -> writeBatchesFor("stuck") >= 2);
        stuckAccepted.set(true);
        dispatcher.flushAllSync();
        assertEquals(2L, dispatcher.samplesSent());
        assertEquals(0L, dispatcher.samplesFailed());
    }

    @Test
    void cadeiaLegitimaABCNaoConsultaOLider() {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_B);
            if (target.equals(OWNER_B)) return moved(req, OWNER_C);
            return okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));

        Await.untilTrue("confirmada em storage-c", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertEquals(0, placementLookup.atLeaderCalls.get());
        assertEquals(List.of(OWNER_A, OWNER_B, OWNER_C),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::target).toList());
    }

    @Test
    void okZeraOEpisodio() {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            long ts = req.writes().getFirst().tsEpochMs();
            if (target.equals(OWNER_A)) return ts == 1 ? moved(req, OWNER_C) : okFor(req);
            // Depois do OK em storage-c, uma migração legítima de volta a storage-a é um episódio novo.
            return ts == 1 ? okFor(req) : moved(req, OWNER_A);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        Await.untilTrue("primeira amostra confirmada em storage-c", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        dispatcher.enqueue(OWNER_A.value(), write("s1", 2, 2));
        Await.untilTrue("segunda amostra confirmada em storage-a", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 2L);

        assertEquals(0, placementLookup.atLeaderCalls.get(),
                "o OK encerra o episódio: voltar a storage-a não é contradição");
        assertEquals(List.of(OWNER_A, OWNER_C, OWNER_C, OWNER_A),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::target).toList());
    }

    @Test
    void redirecionamentoPreservaAsTentativasDeMigracaoDaSerie() {
        newDispatcher(1, Duration.ofSeconds(30), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true,
                new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(20), Duration.ofSeconds(2)));
        List<Long> sentAtNanos = new CopyOnWriteArrayList<>();
        AtomicInteger atA = new AtomicInteger();
        rpc.respondByTarget((target, cmd, body) -> {
            sentAtNanos.add(System.nanoTime());
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) {
                // Duas respostas MIGRATING (backoff por série 20 e 40 ms), depois a migração termina.
                return atA.incrementAndGet() <= 2
                        ? new WriteBatchResponse(Map.of("s1", SeriesStatus.MIGRATING), Map.of(), Map.of())
                        : moved(req, OWNER_B);
            }
            return sentAtNanos.size() <= 4
                    ? new WriteBatchResponse(Map.of("s1", SeriesStatus.MIGRATING), Map.of(), Map.of())
                    : okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        dispatcher.flushAllSync();

        assertEquals(1L, dispatcher.samplesSent());
        assertEquals(5, sentAtNanos.size());
        long lastGapMs = Duration.ofNanos(sentAtNanos.get(4) - sentAtNanos.get(3)).toMillis();
        // As tentativas da série em storage-a (2) acompanham o salto: o MIGRATING seguinte em storage-b é a
        // 3a tentativa (80 ms), não a 1a (20 ms) de um contador zerado.
        assertTrue(lastGapMs >= 75, "o backoff de MIGRATING não pode zerar a cada salto: " + lastGapMs + " ms");
    }

    @Test
    void consultaIndisponivelNaoPrendeASerieNoNoQueNaoEDono() {
        // Líder sem catalog.lookup (≤ 8.5) ou consulta falhando sempre. A série começa no dono real C, cuja
        // réplica está atrasada (aponta A) por 300 ms; A está em dia e aponta C. Sem autoridade, o cliente
        // segue a última dica com backoff — nunca fica parado em A, que jamais vai aceitar.
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        placementLookup.atLeader = keys -> {
            throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE, "líder sem catalog.lookup (simulado)");
        };
        long start = System.nanoTime();
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_C);
            boolean caughtUp = System.nanoTime() - start > Duration.ofMillis(300).toNanos();
            return caughtUp ? okFor(req) : moved(req, OWNER_A);
        });

        dispatcher.enqueue(OWNER_C.value(), write("s1", 1, 1));

        Await.untilTrue("amostra confirmada no dono real", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertTrue(placementLookup.atLeaderCalls.get() >= 1, "a contradição deveria ter tentado o líder");
    }

    @Test
    void errorNaConsultaNaoDeixaASeriePausadaParaSempre() {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicInteger lookups = new AtomicInteger();
        placementLookup.atLeader = keys -> {
            if (lookups.incrementAndGet() == 1) {
                throw new AssertionError("erro de linkagem simulado");
            }
            return Map.of("s1", SeriesPlacement.active(OWNER_A.value(), 1L));
        };
        AtomicInteger callsToA = new AtomicInteger();
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            return callsToA.incrementAndGet() == 1 ? moved(req, OWNER_A) : okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));

        Await.untilTrue("amostra confirmada apesar do Error na consulta", AWAIT_TIMEOUT,
                () -> dispatcher.samplesSent() == 1L);
    }

    @Test
    void migracaoLegitimaDepoisDoDonoConfirmadoConverge() {
        // O líder confirmou C; antes de qualquer OK, C migra de verdade para D e responde WRONG_OWNER(D).
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicInteger phase = new AtomicInteger();
        placementLookup.atLeader = keys -> Map.of("s1",
                SeriesPlacement.active(phase.get() == 0 ? OWNER_C.value() : OWNER_D.value(), 1L));
        AtomicInteger callsToC = new AtomicInteger();
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_C);
            if (target.equals(OWNER_C)) {
                if (callsToC.incrementAndGet() == 1) return moved(req, OWNER_A);   // réplica atrasada em C
                phase.set(1);
                return moved(req, OWNER_D);                                        // migrou de fato para D
            }
            return okFor(req);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));

        Await.untilTrue("amostra confirmada em storage-d", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertTrue(rpc.calls().stream().anyMatch(c -> c.target().equals(OWNER_D)));
    }

    @Test
    void muitasSeriesContraditoriasConvergemEmOrdem() {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        placementLookup.atLeader = keys -> {
            Map<String, SeriesPlacement> found = new LinkedHashMap<>();
            keys.forEach(key -> found.put(key, SeriesPlacement.active(OWNER_C.value(), 1L)));
            return found;
        };
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_C);
            return placementLookup.atLeaderCalls.get() > 0 ? okFor(req) : moved(req, OWNER_A);
        });
        int seriesCount = 50;
        int perSeries = 20;
        for (int i = 0; i < perSeries; i++) {
            for (int s = 0; s < seriesCount; s++) {
                dispatcher.enqueue(OWNER_A.value(), write("s" + s, i, i));
            }
        }

        dispatcher.flushAllSync();

        assertEquals((long) seriesCount * perSeries, dispatcher.samplesSent());
        assertTrue(placementLookup.atLeaderCalls.get() < seriesCount, "consultas coalescidas em lote: "
                + placementLookup.atLeaderCalls.get());
        for (int s = 0; s < seriesCount; s++) {
            String key = "s" + s;
            List<Long> atC = rpc.calls().stream().filter(c -> c.target().equals(OWNER_C))
                    .flatMap(c -> ((WriteBatchRequest) c.body()).writes().stream())
                    .filter(w -> w.seriesKey().equals(key)).map(SeriesWrite::tsEpochMs).distinct().toList();
            // Reenvios antes do OK são permitidos; a primeira entrega de cada amostra segue a ordem da série.
            assertEquals(java.util.stream.LongStream.range(0, perSeries).boxed().toList(), atC, key);
        }
    }

    @Test
    void pingPongSemLiderTemTaxaLimitada() throws Exception {
        // Discordância permanente entre storage-a e storage-c com a consulta ao líder sempre falhando: sem
        // autoridade, o cliente segue a última dica, mas com o backoff por série — a taxa de reenvios decai
        // até backoffMax (200 ms), nunca um laço quente.
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        placementLookup.atLeader = keys -> {
            throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE, "líder sem catalog.lookup (simulado)");
        };
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            return target.equals(OWNER_A) ? moved(req, OWNER_C) : moved(req, OWNER_A);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        Thread.sleep(1_000);
        long batchesFirstSecond = countWriteBatchCalls();
        int lookupsFirstSecond = placementLookup.atLeaderCalls.get();
        Thread.sleep(3_000);
        long batchesNextThreeSeconds = countWriteBatchCalls() - batchesFirstSecond;
        int lookupsNextThreeSeconds = placementLookup.atLeaderCalls.get() - lookupsFirstSecond;

        // 3 s a 200 ms por reenvio dão ~15 lotes; folga para o escalonamento.
        assertTrue(batchesNextThreeSeconds <= 20, "reenvios demais com backoff no teto: " + batchesNextThreeSeconds
                + " (primeiro segundo: " + batchesFirstSecond + ")");
        assertTrue(lookupsNextThreeSeconds <= 20, "consultas ao líder demais com backoff no teto: "
                + lookupsNextThreeSeconds + " (primeiro segundo: " + lookupsFirstSecond + ")");
        assertEquals(0L, dispatcher.samplesSent());
    }

    @Test
    void donoConfirmadoDepoisConsultaFalhaAindaConverge() {
        // O líder confirma storage-c uma vez e depois some (consultas falham). storage-c, com a réplica
        // atrasada por 1,5 s, ainda aponta storage-a: o cliente segue a dica velha, mas precisa voltar a
        // storage-c e convergir quando a réplica dele alcançar o líder.
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        AtomicInteger lookups = new AtomicInteger();
        placementLookup.atLeader = keys -> {
            if (lookups.incrementAndGet() == 1) {
                return Map.of("s1", SeriesPlacement.active(OWNER_C.value(), 1L));
            }
            throw new NgrrdClusterException(ErrorCode.NO_LEADER, "líder indisponível (simulado)");
        };
        long start = System.nanoTime();
        rpc.respondByTarget((target, cmd, body) -> {
            WriteBatchRequest req = (WriteBatchRequest) body;
            if (target.equals(OWNER_A)) return moved(req, OWNER_C);
            boolean caughtUp = System.nanoTime() - start > Duration.ofMillis(1_500).toNanos();
            return caughtUp ? okFor(req) : moved(req, OWNER_A);
        });

        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));

        Await.untilTrue("amostra confirmada em storage-c", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 1L);
        assertTrue(lookups.get() >= 2, "a consulta deveria ter falhado ao menos uma vez depois da confirmação");
        assertEquals(0L, dispatcher.samplesFailed());
    }

    @Test
    void consultaConcluidaDepoisDoCloseNaoMoveEscritas() throws Exception {
        newDispatcher(1, Duration.ofMillis(10), 1_000, NgrrdClusterConfig.BufferFullPolicy.BLOCK, key -> true);
        CountDownLatch lookupEntered = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        placementLookup.atLeader = keys -> {
            lookupEntered.countDown();
            // Consulta que ignora a interrupção do close() e só termina depois do descarte final.
            boolean interrupted = false;
            while (releaseLookup.getCount() > 0) {
                try {
                    releaseLookup.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return Map.of("s1", SeriesPlacement.active(OWNER_C.value(), 1L));
        };
        rpc.respondByTarget((target, cmd, body) -> moved((WriteBatchRequest) body, OWNER_A));
        dispatcher.enqueue(OWNER_A.value(), write("s1", 1, 1));
        assertTrue(lookupEntered.await(5, TimeUnit.SECONDS));

        dispatcher.close(Duration.ofMillis(50));
        assertEquals(1L, dispatcher.samplesFailed(), "a amostra pausada é descartada e contabilizada no close");
        releaseLookup.countDown();
        Thread.sleep(200);

        assertEquals(Map.of(OWNER_A.value(), 0L), dispatcher.bufferedSamples(),
                "consulta concluída após o close não cria buffer nem move escritas");
        assertTrue(rpc.calls().stream().noneMatch(c -> c.target().equals(OWNER_C)));
    }

    private long writeBatchesFor(String seriesKey) {
        return rpc.calls().stream().filter(c -> c.command().equals(Commands.WRITE_BATCH))
                .filter(c -> ((WriteBatchRequest) c.body()).writes().getFirst().seriesKey().equals(seriesKey))
                .count();
    }

    private static WriteBatchResponse moved(WriteBatchRequest request, NodeId newOwner) {
        Map<String, SeriesStatus> status = new LinkedHashMap<>();
        Map<String, String> owners = new LinkedHashMap<>();
        for (SeriesWrite w : request.writes()) {
            status.put(w.seriesKey(), SeriesStatus.WRONG_OWNER);
            owners.put(w.seriesKey(), newOwner.value());
        }
        return new WriteBatchResponse(status, owners, Map.of());
    }

    private long countWriteBatchCalls() {
        return rpc.calls().stream().filter(c -> c.command().equals(Commands.WRITE_BATCH)).count();
    }

    /**
     * {@link PlacementLookup} fake: grava as chamadas de {@code noteOwner}/{@code invalidate} e responde a
     * consulta em lote ao líder com {@link #atLeader} (contando as chamadas).
     */
    private static final class FakePlacementLookup implements PlacementLookup {
        final List<String> notedOwners = new CopyOnWriteArrayList<>();
        final List<String> invalidated = new CopyOnWriteArrayList<>();
        final AtomicInteger atLeaderCalls = new AtomicInteger();
        volatile Function<Collection<String>, Map<String, SeriesPlacement>> atLeader = keys -> {
            throw new UnsupportedOperationException("consulta ao líder não esperada neste teste");
        };

        @Override
        public Map<String, SeriesPlacement> resolveExistingAtLeader(Collection<String> seriesKeys, Duration maxWait) {
            atLeaderCalls.incrementAndGet();
            return atLeader.apply(seriesKeys);
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            throw new UnsupportedOperationException("não usado pelo WriteDispatcher");
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado pelo WriteDispatcher");
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado pelo WriteDispatcher");
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            throw new UnsupportedOperationException("não usado pelo WriteDispatcher");
        }

        @Override
        public void invalidate(String seriesKey) {
            invalidated.add(seriesKey);
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
            notedOwners.add(seriesKey + "=" + ownerNodeId);
        }
    }
}
