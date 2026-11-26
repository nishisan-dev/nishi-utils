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
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig.BufferFullPolicy;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig.WriteFailureReporting;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.ReleaseOptions;
import dev.nishisan.utils.oss.cluster.api.ReleaseResult;
import dev.nishisan.utils.oss.cluster.api.ReleaseStatus;
import dev.nishisan.utils.oss.cluster.api.SeriesRelease;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code release(seriesKeys)} do cliente ({@link SeriesReleaser}) sobre um {@link WriteDispatcher} real, com RPC
 * falso, nos dois modos de reporte de falha: admissão encerrada sob o lock de abertura, dreno até a fronteira, colheita
 * das falhas, checkpoint e {@code CLOSE} opcionais e limpeza local da rota.
 */
class ReleaseTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String OWNER = "A";

    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private final Rpc rpc = new Rpc();
    private final Lookup lookup = new Lookup();
    private WriteDispatcher dispatcher;
    private SeriesHandleCache handles;
    private SeriesReleaser releaser;
    private volatile boolean clientClosed;

    @AfterEach
    void tearDown() {
        gates.forEach(CountDownLatch::countDown);
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.OK);
        rpc.checkpoints = (owner, body) -> new SeriesStatusResponse(SeriesStatus.OK, owner, null);
        if (releaser != null) {
            releaser.close();
        }
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private void start(WriteFailureReporting mode) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(20));
        handles = new SeriesHandleCache();
        dispatcher = new WriteDispatcher(rpc, lookup, retry, 1, Duration.ofMillis(10), 200_000, BufferFullPolicy.BLOCK,
                Duration.ofSeconds(3), key -> {
                    RemoteSeriesHandle handle = handles.get(key);
                    return handle != null && handle.reopen();
                }, (key, owner) -> { }, Clock.systemUTC(), null, null, mode);
        releaser = new SeriesReleaser(handles, dispatcher, lookup, Duration.ofSeconds(5), () -> clientClosed);
    }

    private RemoteSeriesHandle open(String key) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(20));
        return (RemoteSeriesHandle) handles.open(key, true, () -> {
            RemoteSeriesHandle handle = new RemoteSeriesHandle(key, "unused", "unused", Map.of(), null, lookup, rpc,
                    dispatcher, retry, Duration.ofSeconds(3), Duration.ofSeconds(3), Clock.systemUTC(), handles::remove,
                    CapabilityFixtures.advertisingAll(), () -> clientClosed);
            handle.open();
            return handle;
        });
    }

    private ReleaseResult release(ReleaseOptions options, String... keys) throws Exception {
        return releaser.release(List.of(keys), options).get(WAIT.toSeconds(), TimeUnit.SECONDS);
    }

    private CountDownLatch gate() {
        var gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseDrenaEscritasAdmitidasAntesDeConcluir(WriteFailureReporting mode) throws Exception {
        var entered = gate();
        var ack = gate();
        rpc.writes = (owner, request) -> {
            entered.countDown();
            awaitGate(ack);
            return respond(request, key -> SeriesStatus.OK);
        };
        start(mode);
        RemoteSeriesHandle handle = open("s");
        for (int i = 1; i <= 3; i++) {
            handle.write("ds", new Sample(i, i));
        }
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        CompletableFuture<ReleaseResult> future = releaser.release(List.of("s"), ReleaseOptions.defaults());
        Thread.sleep(200L);
        assertFalse(future.isDone(), "o release espera a resposta final das escritas admitidas");

        ack.countDown();
        ReleaseResult result = future.get(10, TimeUnit.SECONDS);

        assertEquals(ReleaseStatus.RELEASED, result.series().get("s").status());
        assertTrue(result.allReleased());
        assertEquals(3, dispatcher.samplesSent());
        assertFalse(handle.isOpen());
        assertNull(handles.get("s"));
        assertFalse(dispatcher.hasRoute("s"), "a rota ociosa sai do cliente");
        assertEquals(1L, dispatcher.routesRemoved());
    }

    @Test
    void releaseEntregaFalhasNaoReportadasUmaUnicaVez() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        start(WriteFailureReporting.MARKS);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));
        handle.write("ds", new Sample(2, 2));
        Await.untilTrue("falhas finais", WAIT, () -> dispatcher.samplesFailed() == 2);

        ReleaseResult result = release(ReleaseOptions.defaults(), "s");
        var mark = dispatcher.mark().completion().get(10, TimeUnit.SECONDS);

        SeriesRelease series = result.series().get("s");
        assertEquals(ReleaseStatus.WRITE_FAILURES, series.status());
        assertEquals(Map.of(SeriesStatus.ERROR, 2L), series.failedSamplesByStatus());
        assertEquals(2L, result.failedSamples());
        assertEquals(Set.of("s"), result.notReleased());
        assertFalse(result.allReleased());
        assertEquals(0L, mark.samplesFailed(), "a marca seguinte não vê as falhas entregues ao release");
        assertFalse(dispatcher.hasRoute("s"), "falha entregue: a rota sai do cliente");
    }

    @Test
    void marcaCriadaDuranteOReleaseNaoConsomeAsFalhasDaSerieLiberada() throws Exception {
        var entered = gate();
        var ack = gate();
        rpc.writes = (owner, request) -> {
            String key = request.writes().getFirst().seriesKey();
            if (key.equals("a")) {
                entered.countDown();
                awaitGate(ack);
                return respond(request, k -> SeriesStatus.OK);
            }
            return respond(request, k -> SeriesStatus.ERROR);
        };
        start(WriteFailureReporting.MARKS);
        RemoteSeriesHandle stuck = open("a");
        RemoteSeriesHandle failed = open("s");
        failed.write("ds", new Sample(1, 1));
        Await.untilTrue("falha final de s", WAIT, () -> dispatcher.samplesFailed() == 1);
        stuck.write("ds", new Sample(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        // O dreno para em "a" (dono parado); a marca nasce depois da admissão encerrada de "s".
        CompletableFuture<ReleaseResult> future = releaser.release(List.of("a", "s"), ReleaseOptions.defaults());
        Await.untilTrue("admissão encerrada", WAIT, () -> !stuck.isOpen() && !failed.isOpen());
        Thread.sleep(50L);
        var mark = dispatcher.mark();
        ack.countDown();
        ReleaseResult result = future.get(10, TimeUnit.SECONDS);
        var marked = mark.completion().get(10, TimeUnit.SECONDS);

        SeriesRelease series = result.series().get("s");
        assertEquals(ReleaseStatus.WRITE_FAILURES, series.status(), "a falha de s é do release, não da marca nova");
        assertEquals(Map.of(SeriesStatus.ERROR, 1L), series.failedSamplesByStatus());
        assertEquals(ReleaseStatus.RELEASED, result.series().get("a").status());
        assertEquals(0L, marked.samplesFailed(), "a marca criada durante o release não colhe a falha de s");
        assertFalse(dispatcher.hasRoute("s"));
    }

    @Test
    void releaseComBarrierRemoveRotaComFalha() throws Exception {
        rpc.writes = (owner, request) -> respond(request, key -> SeriesStatus.ERROR);
        start(WriteFailureReporting.BARRIER);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));
        Await.untilTrue("falha final", WAIT, () -> dispatcher.samplesFailed() == 1);

        ReleaseResult result = release(ReleaseOptions.defaults(), "s");

        SeriesRelease series = result.series().get("s");
        assertEquals(ReleaseStatus.WRITE_FAILURES, series.status());
        assertEquals(ErrorCode.REMOTE_ERROR, series.errorCode());
        assertTrue(series.message().contains("ERROR simulado"), series.message());
        assertFalse(dispatcher.hasRoute("s"), "a falha entregue ao release libera a rota");
        assertEquals(1L, dispatcher.routesRemoved());
        dispatcher.flushAllSync();
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseSemHandleDevolveNotHeld(WriteFailureReporting mode) throws Exception {
        start(mode);
        open("held");

        ReleaseResult result = release(ReleaseOptions.defaults(), "absent", "held", "absent");

        assertEquals(List.of("absent", "held"), List.copyOf(result.series().keySet()));
        assertEquals(ReleaseStatus.NOT_HELD, result.series().get("absent").status());
        assertEquals(ReleaseStatus.RELEASED, result.series().get("held").status());
        assertTrue(result.allReleased());
        assertEquals(0L, result.failedSamples());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseNaoMandaCloseNoNoPorPadrao(WriteFailureReporting mode) throws Exception {
        start(mode);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));

        ReleaseResult result = release(ReleaseOptions.defaults(), "s");

        assertEquals(ReleaseStatus.RELEASED, result.series().get("s").status());
        assertFalse(rpc.commands.contains(Commands.CLOSE + "@" + OWNER), rpc.commands.toString());
        assertFalse(rpc.commands.contains(Commands.CHECKPOINT + "@" + OWNER), rpc.commands.toString());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseComCloseOnNodeMandaClose(WriteFailureReporting mode) throws Exception {
        start(mode);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));

        ReleaseResult result = release(ReleaseOptions.defaults().withCloseOnNode(true), "s");

        assertEquals(ReleaseStatus.RELEASED, result.series().get("s").status());
        assertEquals(1L, rpc.commands.stream().filter((Commands.CLOSE + "@" + OWNER)::equals).count());
        assertFalse(dispatcher.hasRoute("s"));
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseComCheckpointFalhoDevolveCheckpointFailed(WriteFailureReporting mode) throws Exception {
        rpc.checkpoints = (owner, body) -> body.seriesKey().equals("bad")
                ? new SeriesStatusResponse(SeriesStatus.ERROR, owner, "disco cheio simulado")
                : new SeriesStatusResponse(SeriesStatus.OK, owner, null);
        start(mode);
        open("bad").write("ds", new Sample(1, 1));
        open("good").write("ds", new Sample(1, 1));

        ReleaseResult result = release(ReleaseOptions.defaults().withCheckpoint(true), "bad", "good");

        SeriesRelease bad = result.series().get("bad");
        assertEquals(ReleaseStatus.CHECKPOINT_FAILED, bad.status());
        assertEquals(ErrorCode.REMOTE_ERROR, bad.errorCode());
        assertEquals(ReleaseStatus.RELEASED, result.series().get("good").status());
        assertEquals(Set.of("bad"), result.notReleased());
        assertEquals(2L, rpc.commands.stream().filter((Commands.CHECKPOINT + "@" + OWNER)::equals).count());
        assertFalse(dispatcher.hasRoute("bad"), "checkpoint falho: a rota já drenada sai do cliente");
        assertFalse(dispatcher.hasRoute("good"));
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseComDonoParadoDevolveTimedOutEContinuaDrenando(WriteFailureReporting mode) throws Exception {
        var entered = gate();
        var ack = gate();
        rpc.writes = (owner, request) -> {
            entered.countDown();
            awaitGate(ack);
            return respond(request, key -> SeriesStatus.OK);
        };
        start(mode);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        ReleaseResult result = release(ReleaseOptions.defaults().withTimeout(Duration.ofMillis(200)), "s");

        SeriesRelease series = result.series().get("s");
        assertEquals(ReleaseStatus.TIMED_OUT, series.status());
        assertEquals(ErrorCode.TIMEOUT, series.errorCode());
        assertFalse(result.allReleased());
        assertFalse(handle.isOpen(), "o handle já está fechado");
        assertTrue(dispatcher.hasRoute("s"), "a escrita em voo segura a rota");
        assertEquals(0, dispatcher.routeHolders("s"), "a rota fica sem holder");

        ack.countDown();
        Await.untilTrue("rota removida quando a escrita termina", WAIT, () -> !dispatcher.hasRoute("s"));
        assertEquals(1, dispatcher.samplesSent());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void writeConcorrenteAoReleaseLancaClosed(WriteFailureReporting mode) throws Exception {
        start(mode);
        RemoteSeriesHandle handle = open("s");
        var writing = new CountDownLatch(1);
        AtomicReference<NgrrdClusterException> rejected = new AtomicReference<>();
        Thread writer = Thread.ofPlatform().start(() -> {
            for (long ts = 1; ; ts++) {
                try {
                    handle.write("ds", new Sample(ts, ts));
                } catch (NgrrdClusterException e) {
                    rejected.set(e);
                    return;
                }
                writing.countDown();
            }
        });
        assertTrue(writing.await(5, TimeUnit.SECONDS));

        ReleaseResult result = release(ReleaseOptions.defaults(), "s");
        writer.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(writer.isAlive());
        assertEquals(ErrorCode.CLOSED, rejected.get().code(), "nunca SERIES_DELETED: " + rejected.get());
        assertEquals(ReleaseStatus.RELEASED, result.series().get("s").status());
        assertEquals(dispatcher.samplesEnqueued(), dispatcher.samplesSent(),
                "toda escrita admitida antes da fronteira foi drenada");
        assertEquals(ErrorCode.CLOSED, assertThrows(NgrrdClusterException.class,
                () -> handle.write("ds", new Sample(0, 0))).code());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void openDepoisDoReleaseCriaHandleNovo(WriteFailureReporting mode) throws Exception {
        start(mode);
        RemoteSeriesHandle first = open("s");
        first.write("ds", new Sample(1, 1));
        release(ReleaseOptions.defaults(), "s");

        RemoteSeriesHandle second = open("s");
        second.write("ds", new Sample(2, 2));
        second.flush();

        assertNotSame(first, second);
        assertTrue(second.isOpen());
        assertEquals(1, dispatcher.routeHolders("s"));
        assertEquals(2, dispatcher.samplesSent());
        assertEquals(ErrorCode.CLOSED, assertThrows(NgrrdClusterException.class,
                () -> first.write("ds", new Sample(3, 3))).code());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseRespeitaMaxConcurrentPerNode(WriteFailureReporting mode) throws Exception {
        Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> peak = new ConcurrentHashMap<>();
        rpc.checkpoints = (owner, body) -> {
            int now = inFlight.computeIfAbsent(owner, o -> new AtomicInteger()).incrementAndGet();
            peak.computeIfAbsent(owner, o -> new AtomicInteger()).accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(30L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.get(owner).decrementAndGet();
            }
            return new SeriesStatusResponse(SeriesStatus.OK, owner, null);
        };
        lookup.ownerOf = key -> key.startsWith("a-") ? "A" : "B";
        start(mode);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            for (String prefix : List.of("a-", "b-")) {
                String key = prefix + i;
                open(key).write("ds", new Sample(1, 1));
                keys.add(key);
            }
        }

        ReleaseResult result = releaser.release(keys,
                ReleaseOptions.defaults().withCheckpoint(true).withMaxConcurrentPerNode(3)).get(10, TimeUnit.SECONDS);

        assertTrue(result.allReleased(), result.notReleased().toString());
        assertEquals(24L, rpc.commands.stream().filter(c -> c.startsWith(Commands.CHECKPOINT + "@")).count());
        for (String owner : List.of("A", "B")) {
            int max = peak.get(owner).get();
            assertTrue(max <= 3, "mais de 3 CHECKPOINT simultâneos em " + owner + ": " + max);
            assertTrue(max >= 2, "os CHECKPOINT de " + owner + " deveriam correr em paralelo: " + max);
        }
    }

    @Test
    void releaseDeCemMilSeriesNaoBloqueiaThreadDoChamador() throws Exception {
        start(WriteFailureReporting.MARKS);
        int count = 100_000;
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = "s-" + i;
            open(key);
            keys.add(key);
        }
        assertEquals(count, dispatcher.routes());

        long startedAt = System.nanoTime();
        CompletableFuture<ReleaseResult> future = releaser.release(keys, ReleaseOptions.defaults());
        long callMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        ReleaseResult result = future.get(10, TimeUnit.SECONDS);

        assertTrue(callMillis < 100, "release() bloqueou a thread do chamador por " + callMillis + " ms");
        assertTrue(result.allReleased());
        assertEquals(count, result.series().size());
        assertEquals(0, dispatcher.routes());
        assertEquals(0, handles.size());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void openConcorrenteAoReleaseNaoEntraNaFronteira(WriteFailureReporting mode) throws Exception {
        var firstEntered = gate();
        var firstAck = gate();
        var secondEntered = gate();
        var secondAck = gate();
        rpc.writes = (owner, request) -> {
            if (request.writes().getFirst().tsEpochMs() == 1) {
                firstEntered.countDown();
                awaitGate(firstAck);
            } else {
                secondEntered.countDown();
                awaitGate(secondAck);
            }
            return respond(request, key -> SeriesStatus.OK);
        };
        start(mode);
        RemoteSeriesHandle first = open("s");
        first.write("ds", new Sample(1, 1));
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));

        CompletableFuture<ReleaseResult> future = releaser.release(List.of("s"), ReleaseOptions.defaults());
        Await.untilTrue("admissão encerrada", WAIT, () -> !first.isOpen());
        // Outra thread abre a mesma chave logo depois do lock de abertura do release e escreve.
        CompletableFuture<RemoteSeriesHandle> reopened = CompletableFuture.supplyAsync(() -> {
            RemoteSeriesHandle handle = open("s");
            handle.write("ds", new Sample(2, 2));
            return handle;
        });
        RemoteSeriesHandle second = reopened.get(10, TimeUnit.SECONDS);
        firstAck.countDown();
        assertTrue(secondEntered.await(5, TimeUnit.SECONDS));

        ReleaseResult result = future.get(10, TimeUnit.SECONDS);

        assertEquals(ReleaseStatus.RELEASED, result.series().get("s").status(),
                "o dreno espera só a fronteira, não a escrita do handle novo");
        assertTrue(dispatcher.hasRoute("s"), "a rota religada não sai do cliente");
        assertEquals(1, dispatcher.routeHolders("s"));
        assertTrue(second.isOpen());
        secondAck.countDown();
        second.flush();
        assertEquals(2, dispatcher.samplesSent());
    }

    @ParameterizedTest
    @EnumSource(WriteFailureReporting.class)
    void releaseComConsultaDeDonoPendenteRemoveSoDepois(WriteFailureReporting mode) throws Exception {
        var lookupEntered = gate();
        var lookupAnswer = gate();
        AtomicInteger redirects = new AtomicInteger();
        // Dica que aponta o próprio nó: contraditória, a série pausa e consulta o líder (bloqueado).
        rpc.writes = (owner, request) -> redirects.getAndIncrement() == 0
                ? new WriteBatchResponse(Map.of("s", SeriesStatus.WRONG_OWNER), Map.of("s", OWNER), Map.of())
                : respond(request, key -> SeriesStatus.OK);
        lookup.atLeader = keys -> {
            lookupEntered.countDown();
            awaitGate(lookupAnswer);
            Map<String, SeriesPlacement> found = new LinkedHashMap<>();
            keys.forEach(key -> found.put(key, lookup.resolve(key, null)));
            return found;
        };
        start(mode);
        RemoteSeriesHandle handle = open("s");
        handle.write("ds", new Sample(1, 1));
        assertTrue(lookupEntered.await(5, TimeUnit.SECONDS));

        ReleaseResult result = release(ReleaseOptions.defaults().withTimeout(Duration.ofMillis(200)), "s");

        assertEquals(ReleaseStatus.TIMED_OUT, result.series().get("s").status());
        assertTrue(dispatcher.hasRoute("s"), "consulta de dono pendente: a rota fica");
        assertEquals(0, dispatcher.routeHolders("s"));

        lookupAnswer.countDown();
        Await.untilTrue("rota removida depois da consulta e do OK", WAIT, () -> !dispatcher.hasRoute("s"));
        assertEquals(1, dispatcher.samplesSent());
        assertEquals(1L, dispatcher.ownerLookups());
    }

    @Test
    void releaseComChavesNulasOuClienteFechadoFalhaAFuture() {
        start(WriteFailureReporting.MARKS);

        var nullKeys = assertThrows(ExecutionException.class,
                () -> releaser.release(null, ReleaseOptions.defaults()).get(5, TimeUnit.SECONDS));
        clientClosed = true;
        var closed = assertThrows(ExecutionException.class,
                () -> releaser.release(List.of("s"), ReleaseOptions.defaults()).get(5, TimeUnit.SECONDS));

        assertTrue(nullKeys.getCause() instanceof NullPointerException, String.valueOf(nullKeys.getCause()));
        assertEquals(ErrorCode.CLOSED, ((NgrrdClusterException) closed.getCause()).code());
    }

    // ------------------------------------------------------------- apoio

    private static WriteBatchResponse respond(WriteBatchRequest request, Function<String, SeriesStatus> statusOf) {
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

    /** RPC falso: {@code WRITE_BATCH} e {@code CHECKPOINT} programáveis; o resto responde {@code OK}. */
    private static final class Rpc implements ClusterRpc {
        volatile BiFunction<String, WriteBatchRequest, WriteBatchResponse> writes =
                (owner, request) -> respond(request, key -> SeriesStatus.OK);
        volatile BiFunction<String, SeriesCommandRequest, SeriesStatusResponse>
                checkpoints = (owner, body) -> new SeriesStatusResponse(SeriesStatus.OK, owner, null);
        final List<String> commands = new CopyOnWriteArrayList<>();

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> type) {
            if (!command.equals(Commands.OPEN)) {
                commands.add(command + "@" + target.value());
            }
            if (command.equals(Commands.WRITE_BATCH)) {
                return type.cast(writes.apply(target.value(), (WriteBatchRequest) body));
            }
            if (command.equals(Commands.CHECKPOINT)) {
                return type.cast(checkpoints.apply(target.value(),
                        (SeriesCommandRequest) body));
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

    /**
     * Placement estático (dono por chave, uma geração por chave: {@code SeriesPlacement.active} gera um token novo a
     * cada chamada), com a consulta em lote ao líder programável.
     */
    private static final class Lookup implements PlacementLookup {
        private final Map<String, SeriesPlacement> placements = new ConcurrentHashMap<>();
        volatile Function<String, String> ownerOf = key -> OWNER;
        volatile Function<Collection<String>, Map<String, SeriesPlacement>> atLeader = keys -> {
            Map<String, SeriesPlacement> found = new LinkedHashMap<>();
            keys.forEach(key -> found.put(key, resolve(key, null)));
            return found;
        };

        @Override
        public SeriesPlacement resolve(String key, String hash) {
            return placements.computeIfAbsent(key, k -> SeriesPlacement.active(ownerOf.apply(k), 0));
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
            return atLeader.apply(keys);
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
