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
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        placementLookup = new FakePlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(200));
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

    private long countWriteBatchCalls() {
        return rpc.calls().stream().filter(c -> c.command().equals(Commands.WRITE_BATCH)).count();
    }

    /** {@link PlacementLookup} fake: só grava as chamadas de {@code noteOwner}/{@code invalidate}. */
    private static final class FakePlacementLookup implements PlacementLookup {
        final List<String> notedOwners = new CopyOnWriteArrayList<>();
        final List<String> invalidated = new CopyOnWriteArrayList<>();

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
