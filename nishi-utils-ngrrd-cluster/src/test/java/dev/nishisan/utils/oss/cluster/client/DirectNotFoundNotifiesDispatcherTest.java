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
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integra {@link RemoteSeriesHandle} e {@link WriteDispatcher} reais (com o mesmo wiring de
 * {@code DefaultNgrrdClusterClient}: mapa de handles, reopener e remoção condicional) para cobrir a
 * série confirmada inexistente fora da reabertura assíncrona do dispatcher: a marca da série falha as
 * escritas pendentes e as que chegam atrasadas, e um handle novo da mesma chave volta a escrever,
 * fazer checkpoint e {@code flushAll} normalmente.
 */
class DirectNotFoundNotifiesDispatcherTest {

    private static final String SERIES_KEY = "gone";
    private static final NodeId OWNER_A = NodeId.of("storage-a");
    private static final NodeId OWNER_B = NodeId.of("storage-b");
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final ViewQuery ANY_QUERY =
            new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);

    private final RecordingClusterRpc rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
    private final ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
    private final FakePlacementLookup lookup = new FakePlacementLookup(OWNER_A.value());
    private final RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(50));

    /** Mesmo wiring de {@code DefaultNgrrdClusterClient.connect()} para reopener e troca de dono. */
    private WriteDispatcher newDispatcher(int batchMaxSamples) {
        return new WriteDispatcher(rpc, lookup, retry, batchMaxSamples, Duration.ofSeconds(30), 1_000,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5),
                key -> {
                    RemoteSeriesHandle handle = handles.get(key);
                    return handle != null && handle.reopen();
                },
                (key, newOwner) -> {
                    RemoteSeriesHandle handle = handles.get(key);
                    if (handle != null) {
                        handle.ownerChanged(newOwner);
                    }
                }, Clock.systemUTC(), null, null);
    }

    private RemoteSeriesHandle newHandle(WriteBuffer buffer) {
        return newHandle(buffer, rpc, Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), Duration.ofSeconds(5));
    }

    private RemoteSeriesHandle newHandle(WriteBuffer buffer, RecordingClusterRpc handleRpc, Ngrrd.OpenOptions options,
            Duration closeTimeout) {
        return new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(), options, lookup, handleRpc,
                buffer, retry, Duration.ofSeconds(5), closeTimeout, Clock.systemUTC(), handles::remove);
    }

    @Test
    void readDescobreNotFoundDiretoFalhaEscritaNoBufferSemNovasTentativasEFlushAllDasDemaisSeriesSegue() {
        // batchMaxSamples alto e batchMaxDelay bem longo: a única escrita de "gone" fica no buffer, sem
        // ser drenada sozinha, até o read() síncrono descobrir a série inexistente.
        WriteDispatcher dispatcher = newDispatcher(10);
        try {
            RemoteSeriesHandle handle = newHandle(dispatcher);
            handles.put(SERIES_KEY, handle);

            rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
            handle.open();

            rpc.respondNext((cmd, body) -> new ReadResponse(SeriesStatus.NOT_OPEN, null, null, null));
            rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null));

            handle.write("in_octets", new Sample(1L, 1.0));

            SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                    () -> handle.read("in_octets", ANY_QUERY));
            assertEquals(SERIES_KEY, ex.seriesKey());

            assertNull(handles.get(SERIES_KEY), "handle deveria ter se removido do mapa do cliente");
            assertEquals(1L, dispatcher.samplesFailed(), "a escrita ainda no buffer deveria ter falhado");
            assertEquals(0L, dispatcher.samplesSent());
            assertTrue(rpc.calls().stream().noneMatch(c -> c.command().equals(Commands.WRITE_BATCH)),
                    "a escrita nunca deveria ter chegado a ser enviada — falhou direto no buffer");

            // flushAll de outras séries no mesmo dispatcher (mesmo nó) continua funcionando — a rota
            // marcada de "gone" não pode ter ficado presa em pendingRoutes nem no buffer do nó.
            rpc.respondDefault((cmd, body) -> okFor((WriteBatchRequest) body));
            dispatcher.enqueue(OWNER_A.value(), new SeriesWrite("healthy", "in_octets", 2L, 2.0));
            dispatcher.flushAllSync();

            assertEquals(1L, dispatcher.samplesSent());
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void writeQueCruzaComAMarcacaoDoHandleLancaSeriesNotFoundSemAdmissao() {
        // O write() já passou pelo ensureOpen() quando outra operação do mesmo handle confirma a série
        // inexistente; a escrita só chega ao dispatcher depois da marcação — e não pode ser admitida
        // (senão chegaria a NOT_OPEN sem handle nenhum para reabrir e ficaria em retentativa para sempre).
        WriteDispatcher dispatcher = newDispatcher(10);
        try {
            AtomicReference<Runnable> beforeEnqueue = new AtomicReference<>();
            RemoteSeriesHandle handle = newHandle(new InterceptingWriteBuffer(dispatcher, beforeEnqueue));
            handles.put(SERIES_KEY, handle);

            rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
            handle.open();

            rpc.respondNext((cmd, body) -> new ReadResponse(SeriesStatus.NOT_OPEN, null, null, null));
            rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null));
            beforeEnqueue.set(() -> assertThrows(SeriesNotFoundException.class,
                    () -> handle.read("in_octets", ANY_QUERY)));

            SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                    () -> handle.write("in_octets", new Sample(1L, 1.0)));
            assertEquals(SERIES_KEY, ex.seriesKey());

            assertNull(handles.get(SERIES_KEY), "a marcação aconteceu antes da escrita chegar ao dispatcher");
            assertEquals(0L, dispatcher.samplesEnqueued(), "a escrita atrasada não pode ter sido admitida");
            assertEquals(0L, dispatcher.samplesFailed());
            dispatcher.flushAllSync();
            assertTrue(rpc.calls().stream().noneMatch(c -> c.command().equals(Commands.WRITE_BATCH)));
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void handleNovoDaMesmaChaveAposMarcacaoComLoteEmVooEscreveFazCheckpointEFlushAllVolta() throws Exception {
        // batchMaxSamples=1: a escrita do handle A sai do buffer na hora e fica presa no RPC (em voo)
        // enquanto A é marcado inexistente e um handle B da MESMA chave abre (a série foi recriada) e
        // escreve. Quando o lote de A finalmente responde NOT_OPEN, só ele falha: nada de reabrir (nem
        // via B), e a escrita de B, o checkpoint de B e o flushAll seguem normalmente.
        WriteDispatcher dispatcher = newDispatcher(1);
        CountDownLatch batchInFlight = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        try {
            AtomicReference<SeriesStatus> openStatus = new AtomicReference<>(SeriesStatus.OK);
            rpc.respondDefault((cmd, body) -> {
                if (cmd.equals(Commands.WRITE_BATCH)) {
                    WriteBatchRequest request = (WriteBatchRequest) body;
                    if (request.writes().getFirst().tsEpochMs() == 1L) {
                        batchInFlight.countDown();
                        awaitLatch(releaseBatch);
                        return new WriteBatchResponse(Map.of(SERIES_KEY, SeriesStatus.NOT_OPEN), Map.of(), Map.of());
                    }
                    return okFor(request);
                }
                if (cmd.equals(Commands.READ)) {
                    return new ReadResponse(SeriesStatus.NOT_OPEN, null, null, null);
                }
                if (cmd.equals(Commands.OPEN)) {
                    return new SeriesStatusResponse(openStatus.get(), OWNER_A.value(), null);
                }
                return new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null);
            });

            RemoteSeriesHandle handleA = newHandle(dispatcher);
            handleA.open();
            handles.put(SERIES_KEY, handleA);

            openStatus.set(SeriesStatus.NOT_FOUND);
            handleA.write("in_octets", new Sample(1L, 1.0));
            assertTrue(batchInFlight.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "lote de A em voo");
            assertThrows(SeriesNotFoundException.class, () -> handleA.read("in_octets", ANY_QUERY));
            assertNull(handles.get(SERIES_KEY));

            // Escrita atrasada da geração antiga (ex.: um write() de A que passou pelo ensureOpen antes da
            // marcação): recusada, sem admissão.
            assertThrows(SeriesNotFoundException.class, () -> dispatcher.enqueue(OWNER_A.value(),
                    new SeriesWrite(SERIES_KEY, "in_octets", 99L, 9.0)));

            openStatus.set(SeriesStatus.OK);
            RemoteSeriesHandle handleB = newHandle(dispatcher);
            handleB.open();
            handles.put(SERIES_KEY, handleB);
            handleB.write("in_octets", new Sample(2L, 2.0));

            releaseBatch.countDown();
            handleB.checkpoint();
            dispatcher.flushAllSync();

            assertSame(handleB, handles.get(SERIES_KEY));
            assertEquals(1L, dispatcher.samplesFailed(), "só o lote em voo do handle A falha");
            assertEquals(1L, dispatcher.samplesSent(), "a escrita do handle B é entregue");
            assertEquals(2L, dispatcher.samplesEnqueued(), "a escrita atrasada da geração antiga nunca foi admitida");
            assertEquals(3L, rpc.calls().stream().filter(c -> c.command().equals(Commands.OPEN)).count(),
                    "OPEN de A, reabertura de A (NOT_FOUND) e OPEN de B — o lote de A nunca aciona reabertura");
            assertEquals(2L, rpc.calls().stream().filter(c -> c.command().equals(Commands.WRITE_BATCH)).count(),
                    "o lote de A nunca é reenviado");
        } finally {
            releaseBatch.countDown();
            dispatcher.close();
        }
    }

    @Test
    void closeEmAndamentoComNotFoundDescobertoPorOperacaoSincronaFalhaEscritasPendentesEConcluiOClose()
            throws Exception {
        // T1 fecha A (e espera o flush da escrita em voo); T2 é um read() de A que passou pelo
        // ensureOpen() antes do close e só descobre NOT_FOUND depois. O close é o dono do encerramento,
        // mas A precisa continuar visível ao reopener até terminar: quando o lote em voo responder
        // NOT_OPEN, o reopener acha A, que lança SeriesNotFoundException, e a rota é marcada. Sem isso o
        // reopener acharia null, a escrita ficaria em retentativa para sempre e o close consumiria todo
        // o orçamento (30 s aqui).
        WriteDispatcher dispatcher = newDispatcher(1);
        CountDownLatch batchInFlight = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        CountDownLatch readInFlight = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch flushEntered = new CountDownLatch(1);
        try {
            AtomicReference<SeriesStatus> openStatus = new AtomicReference<>(SeriesStatus.OK);
            rpc.respondDefault((cmd, body) -> {
                if (cmd.equals(Commands.WRITE_BATCH)) {
                    batchInFlight.countDown();
                    awaitLatch(releaseBatch);
                    return new WriteBatchResponse(Map.of(SERIES_KEY, SeriesStatus.NOT_OPEN), Map.of(), Map.of());
                }
                if (cmd.equals(Commands.READ)) {
                    readInFlight.countDown();
                    awaitLatch(releaseRead);
                    return new ReadResponse(SeriesStatus.NOT_OPEN, null, null, null);
                }
                if (cmd.equals(Commands.OPEN)) {
                    return new SeriesStatusResponse(openStatus.get(), OWNER_A.value(), null);
                }
                return new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null);
            });
            InterceptingWriteBuffer buffer = new InterceptingWriteBuffer(dispatcher, new AtomicReference<>());
            buffer.onFlushSeries = flushEntered::countDown;
            RemoteSeriesHandle handleA = newHandle(buffer, rpc, Ngrrd.OpenOptions.defaults().withCreateIfMissing(false),
                    Duration.ofSeconds(30));
            handleA.open();
            handles.put(SERIES_KEY, handleA);
            openStatus.set(SeriesStatus.NOT_FOUND);

            handleA.write("in_octets", new Sample(1L, 1.0));
            assertTrue(batchInFlight.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "lote de A em voo");

            AtomicReference<Throwable> readFailure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                try {
                    handleA.read("in_octets", ANY_QUERY);
                } catch (Throwable t) {
                    readFailure.set(t);
                }
            }, "test-read-A");
            reader.start();
            assertTrue(readInFlight.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "read de A em voo");

            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    handleA.close();
                } catch (Throwable t) {
                    closeFailure.set(t);
                }
            }, "test-close-A");
            closer.start();
            assertTrue(flushEntered.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "close de A no flush");

            releaseRead.countDown();
            reader.join(AWAIT_TIMEOUT.toMillis());
            assertTrue(readFailure.get() instanceof SeriesNotFoundException, "read: " + readFailure.get());
            assertSame(handleA, handles.get(SERIES_KEY), "com o close em andamento, A continua visível ao reopener");

            releaseBatch.countDown();
            closer.join(AWAIT_TIMEOUT.toMillis());

            assertTrue(!closer.isAlive(), "o close termina sem consumir o orçamento inteiro");
            assertNull(closeFailure.get());
            assertNull(handles.get(SERIES_KEY), "o próprio close tira A do mapa ao terminar");
            assertEquals(1L, dispatcher.samplesFailed(), "a escrita pendente de A falha");
            assertEquals(0L, dispatcher.samplesSent());
            assertEquals(1L, rpc.calls().stream().filter(c -> c.command().equals(Commands.WRITE_BATCH)).count(),
                    "sem retentativa da escrita de A");
            dispatcher.flushAllSync();
        } finally {
            releaseRead.countDown();
            releaseBatch.countDown();
            dispatcher.close();
        }
    }

    @Test
    void reaberturaDoHandleAntigoQueDescobreNotFoundDepoisDoHandleNovoAbrirEntregaAEscritaViaHandleNovo()
            throws Exception {
        // A (sem criar) está fechando com uma escrita pendente; o NOT_OPEN dela leva o reopener a chamar
        // A.reopen(), cujo OPEN fica em voo. Nesse meio tempo o usuário abre B na mesma chave
        // (createIfMissing=true, recria a série) e B escreve. Só depois o OPEN de A volta NOT_FOUND: a
        // descoberta é anterior à abertura de B e não pode marcar a rota — a série existe, a escrita de
        // A é retentada (o reopener agora acha B) e entregue antes das de B.
        WriteDispatcher dispatcher = newDispatcher(1);
        RecordingClusterRpc rpcA = new RecordingClusterRpc(NodeId.of("client-under-test"));
        RecordingClusterRpc rpcB = new RecordingClusterRpc(NodeId.of("client-under-test"));
        CountDownLatch reopenInFlight = new CountDownLatch(1);
        CountDownLatch releaseReopen = new CountDownLatch(1);
        AtomicBoolean seriesOnNode = new AtomicBoolean(false);
        List<Long> delivered = new CopyOnWriteArrayList<>();
        try {
            rpc.respondDefault((cmd, body) -> {
                WriteBatchRequest request = (WriteBatchRequest) body;
                if (!seriesOnNode.get()) {
                    return new WriteBatchResponse(Map.of(SERIES_KEY, SeriesStatus.NOT_OPEN), Map.of(), Map.of());
                }
                request.writes().forEach(w -> delivered.add(w.tsEpochMs()));
                return okFor(request);
            });
            rpcA.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
            rpcA.respondDefault((cmd, body) -> {
                if (cmd.equals(Commands.OPEN)) {
                    reopenInFlight.countDown();
                    awaitLatch(releaseReopen);
                    return new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null);
                }
                return new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null);
            });
            rpcB.respondDefault((cmd, body) -> {
                if (cmd.equals(Commands.OPEN)) {
                    seriesOnNode.set(true);
                }
                return new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null);
            });

            RemoteSeriesHandle handleA = newHandle(dispatcher, rpcA, Ngrrd.OpenOptions.defaults().withCreateIfMissing(false),
                    Duration.ofSeconds(30));
            handleA.open();
            handles.put(SERIES_KEY, handleA);
            handleA.write("in_octets", new Sample(1L, 1.0));
            assertTrue(reopenInFlight.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "reopener chamou A.reopen() e o OPEN está em voo");

            Thread closer = new Thread(handleA::close, "test-close-A");
            closer.start();
            Await.untilTrue("A não é mais reaproveitável", AWAIT_TIMEOUT, () -> !handleA.isOpen());

            // O que DefaultNgrrdClusterClient.open faz ao ver A fechado: abre um handle novo e o publica.
            RemoteSeriesHandle handleB = newHandle(dispatcher, rpcB, Ngrrd.OpenOptions.defaults(), Duration.ofSeconds(5));
            handleB.open();
            handles.put(SERIES_KEY, handleB);
            handleB.write("in_octets", new Sample(2L, 2.0));

            releaseReopen.countDown();
            Await.untilTrue("escritas de A e B entregues", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 2L);

            handleB.write("in_octets", new Sample(3L, 3.0));
            handleB.checkpoint();
            closer.join(AWAIT_TIMEOUT.toMillis());
            dispatcher.flushAllSync();

            assertTrue(!closer.isAlive(), "close de A terminou");
            assertSame(handleB, handles.get(SERIES_KEY));
            assertEquals(List.of(1L, 2L, 3L), delivered, "a escrita de A chega ao nó antes das de B");
            assertEquals(0L, dispatcher.samplesFailed(), "a rota nunca foi marcada");
            assertEquals(3L, dispatcher.samplesSent());
        } finally {
            releaseReopen.countDown();
            dispatcher.close();
        }
    }

    @Test
    void handleNovoComEscritaDoAntigoEmVooPreservaAOrdemQuandoOLoteVoltaNotOpen() throws Exception {
        assertOrderAcrossHandlesOfSameKey(SeriesStatus.NOT_OPEN, null);
    }

    @Test
    void handleNovoComEscritaDoAntigoEmVooPreservaAOrdemQuandoOLoteVoltaWrongOwnerParaOutroDono()
            throws Exception {
        assertOrderAcrossHandlesOfSameKey(SeriesStatus.WRONG_OWNER, OWNER_B.value());
    }

    /**
     * A escreve w1 e fecha: o flush do close envia w1 sozinho, preso no RPC (em voo). B abre a mesma
     * chave (createIfMissing=true) e escreve w2 enquanto w1 está em voo. w1 volta NOT_OPEN, o reopener
     * reabre via B e o lote seguinte leva [w1, w2] e recebe {@code retryStatus} (com {@code newOwner},
     * se houver). O nó tem de receber w1 antes de w2 — o contrário gera NaN silencioso no deriver.
     */
    private void assertOrderAcrossHandlesOfSameKey(SeriesStatus retryStatus, String newOwner) throws Exception {
        WriteDispatcher dispatcher = newDispatcher(2);
        RecordingClusterRpc rpcA = new RecordingClusterRpc(NodeId.of("client-under-test"));
        RecordingClusterRpc rpcB = new RecordingClusterRpc(NodeId.of("client-under-test"));
        CountDownLatch firstBatchInFlight = new CountDownLatch(1);
        CountDownLatch releaseFirstBatch = new CountDownLatch(1);
        AtomicInteger writeBatchCalls = new AtomicInteger();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        try {
            rpc.respondDefault((cmd, body) -> {
                WriteBatchRequest request = (WriteBatchRequest) body;
                int call = writeBatchCalls.incrementAndGet();
                if (call == 1) {
                    firstBatchInFlight.countDown();
                    awaitLatch(releaseFirstBatch);
                    return new WriteBatchResponse(Map.of(SERIES_KEY, SeriesStatus.NOT_OPEN), Map.of(), Map.of());
                }
                if (call == 2) {
                    return new WriteBatchResponse(Map.of(SERIES_KEY, retryStatus),
                            newOwner == null ? Map.of() : Map.of(SERIES_KEY, newOwner), Map.of());
                }
                request.writes().forEach(w -> delivered.add(w.tsEpochMs()));
                return okFor(request);
            });
            rpcA.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
            rpcB.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));

            RemoteSeriesHandle handleA = newHandle(dispatcher, rpcA, Ngrrd.OpenOptions.defaults(), Duration.ofSeconds(30));
            handleA.open();
            handles.put(SERIES_KEY, handleA);
            handleA.write("in_octets", new Sample(1L, 1.0));

            Thread closer = new Thread(handleA::close, "test-close-A");
            closer.start();
            assertTrue(firstBatchInFlight.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "o flush do close de A enviou w1, em voo");

            RemoteSeriesHandle handleB = newHandle(dispatcher, rpcB, Ngrrd.OpenOptions.defaults(), Duration.ofSeconds(5));
            handleB.open();
            handles.put(SERIES_KEY, handleB);
            handleB.write("in_octets", new Sample(2L, 2.0));

            releaseFirstBatch.countDown();
            Await.untilTrue("w1 e w2 entregues", AWAIT_TIMEOUT, () -> dispatcher.samplesSent() == 2L);
            handleB.checkpoint();
            closer.join(AWAIT_TIMEOUT.toMillis());
            dispatcher.flushAllSync();

            assertTrue(!closer.isAlive(), "close de A terminou");
            assertEquals(List.of(1L, 2L), delivered, "w1 (handle A) chega ao nó antes de w2 (handle B)");
            assertEquals(0L, dispatcher.samplesFailed());
            if (newOwner != null) {
                assertTrue(rpc.calls().stream()
                                .filter(c -> c.command().equals(Commands.WRITE_BATCH))
                                .skip(2)
                                .allMatch(c -> c.target().value().equals(newOwner)),
                        "o reenvio vai ao dono novo");
            }
        } finally {
            releaseFirstBatch.countDown();
            dispatcher.close();
        }
    }

    private static WriteBatchResponse okFor(WriteBatchRequest request) {
        Map<String, SeriesStatus> status = new LinkedHashMap<>();
        for (SeriesWrite write : request.writes()) {
            status.put(write.seriesKey(), SeriesStatus.OK);
        }
        return new WriteBatchResponse(status, Map.of(), Map.of());
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertTrue(latch.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /**
     * Delegação completa ao dispatcher real, com um gancho executado uma única vez logo antes de um
     * {@code enqueue} — o ponto exato entre o {@code ensureOpen()} e a admissão de {@code write()}.
     */
    private static final class InterceptingWriteBuffer implements WriteBuffer {
        private final WriteBuffer delegate;
        private final AtomicReference<Runnable> beforeEnqueue;
        /** Executado ao entrar em {@link #flushSeriesSync(String, String, Duration)}, antes de delegar. */
        volatile Runnable onFlushSeries = () -> { };

        InterceptingWriteBuffer(WriteBuffer delegate, AtomicReference<Runnable> beforeEnqueue) {
            this.delegate = delegate;
            this.beforeEnqueue = beforeEnqueue;
        }

        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            Runnable hook = beforeEnqueue.getAndSet(null);
            if (hook != null) {
                hook.run();
            }
            delegate.enqueue(ownerNodeId, write);
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
            delegate.flushNodeSync(ownerNodeId);
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
            delegate.flushNodeSync(ownerNodeId, maxWait);
        }

        @Override
        public void flushSeriesSync(String seriesKey, String ownerNodeId, Duration maxWait) {
            onFlushSeries.run();
            delegate.flushSeriesSync(seriesKey, ownerNodeId, maxWait);
        }

        @Override
        public void failSeries(String seriesKey, Throwable cause) {
            delegate.failSeries(seriesKey, cause);
        }

        @Override
        public void resetSeries(String seriesKey, String ownerNodeId) {
            delegate.resetSeries(seriesKey, ownerNodeId);
        }
    }

    /** {@link PlacementLookup} fake: sempre devolve o dono atual configurado, sem RPC ao líder. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private final String owner;

        FakePlacementLookup(String owner) {
            this.owner = owner;
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            return SeriesPlacement.active(owner, 0L);
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            return resolve(seriesKey, null);
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            return Optional.of(SeriesPlacement.active(owner, 0L));
        }

        @Override
        public void invalidate(String seriesKey) {
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
        }
    }
}
