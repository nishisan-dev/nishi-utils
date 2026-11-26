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

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o {@link LocalCheckpointer} sobre um {@link SeriesHandleRegistry} real (blob volume em
 * {@code @TempDir}): só séries sujas, série ocupada pulada e retomada, TTL de ociosidade intacto, teto de
 * checkpoints em voo, falha contada e retentada, encerramento e métricas. Os ciclos são disparados direto
 * por {@link LocalCheckpointer#runCycle()}, com intervalo curto para a cadência não atrasar o teste. Como no
 * nó, o registro mantém o índice incremental de sujas: {@code dirtySeries} é limite superior atrasado em um
 * ciclo, e {@code lastCycleSeries} conta candidatas.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LocalCheckpointerTest {

    private static final String VOLUME_NAME = "ngrrd";
    private static final long T0 = 1_700_000_100_000L;
    private static final LocalCheckpointSettings FAST = new LocalCheckpointSettings(true, Duration.ofMillis(100), 4);

    private String yaml;
    private BlobVolumeRegistry volumeRegistry;
    private BlobVolume volume;
    private MutableClock clock;
    private SeriesHandleRegistry registry;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        volumeRegistry = NgrrdBlob.registry().basePath(tempDir).volume(VOLUME_NAME).build();
        volume = volumeRegistry.require(VOLUME_NAME);
        clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        registry = new SeriesHandleRegistry(volume, VOLUME_NAME, Duration.ofMinutes(10), 10_000, clock);
        // Como no nó: com o checkpoint local ligado, o registro mantém o índice incremental de sujas.
        registry.trackDirtyIndex(true);
    }

    @AfterEach
    void tearDown() {
        registry.close();
        volumeRegistry.close();
    }

    @Test
    void soSeriesSujasGeramCheckpointESerieLimpaNaoGeraSync() throws Exception {
        open("dirty");
        open("clean");
        write("dirty", T0);
        AtomicInteger dirtySyncs = countCheckpoints("dirty");
        AtomicInteger cleanSyncs = countCheckpoints("clean");
        assertEquals(java.util.Set.of("dirty"), registry.dirtySeries());

        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);

            assertEquals(1, dirtySyncs.get());
            assertEquals(0, cleanSyncs.get());
            assertFalse(registry.isDirty("dirty"), "checkpoint local concluído marca a série como limpa");
            Map<String, Long> metrics = checkpointer.metrics();
            assertEquals(1L, metrics.get("localCheckpoint.enabled"));
            assertEquals(1L, metrics.get("localCheckpoint.cycles"));
            assertEquals(1L, metrics.get("localCheckpoint.lastCycleSeries"));
            assertEquals(1L, metrics.get("localCheckpoint.lastCycleStarted"));
            assertEquals(1L, metrics.get("localCheckpoint.checkpointed"));
            assertEquals(1L, metrics.get("localCheckpoint.dirtySeries"),
                    "limite superior atrasado em um ciclo: a série checkpointada só sai na próxima poda");
            assertEquals(0L, metrics.get("localCheckpoint.failures"));

            checkpointer.runCycle();
            assertEquals(1, dirtySyncs.get(), "sem escrita nova a série não gera checkpoint no ciclo seguinte");
            Map<String, Long> second = checkpointer.metrics();
            assertEquals(1L, second.get("localCheckpoint.lastCycleSeries"), "candidata limpa ainda no índice");
            assertEquals(0L, second.get("localCheckpoint.lastCycleStarted"));
            assertEquals(0L, second.get("localCheckpoint.dirtySeries"), "o ciclo a encontrou limpa e a podou");

            checkpointer.runCycle();
            assertEquals(0L, checkpointer.metrics().get("localCheckpoint.lastCycleSeries"));
        }
    }

    @Test
    void serieOcupadaEPuladaERetomadaNoCicloSeguinte() throws Exception {
        open("busy");
        write("busy", T0);
        AtomicInteger syncs = countCheckpoints("busy");
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            Future<?> holder = executor.submit(() -> registry.withSeries("busy", access -> {
                holding.countDown();
                awaitQuietly(release);
                return null;
            }));
            assertTrue(holding.await(10, TimeUnit.SECONDS));

            checkpointer.runCycle();
            assertEquals(0, syncs.get());
            assertEquals(1L, checkpointer.metrics().get("localCheckpoint.skippedBusy"));
            assertTrue(registry.isDirty("busy"));

            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            assertEquals(1, syncs.get());
            assertFalse(registry.isDirty("busy"));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void checkpointLocalNaoRenovaOTtlDeOciosidade() throws Exception {
        open("idle");
        write("idle", T0);
        clock.advance(Duration.ofMinutes(11));
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            assertEquals(1L, checkpointer.metrics().get("localCheckpoint.checkpointed"));
        }
        assertEquals(1, registry.closeIdle(), "o checkpoint de fundo não pode adiar o fechamento por ociosidade");
    }

    @Test
    void maxInFlightLimitaOsCheckpointsEmVoo() throws Exception {
        List<String> keys = List.of("s0", "s1", "s2", "s3", "s4");
        CompletableFuture<NgrrdHandle.CheckpointTiming> gate = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        for (String key : keys) {
            open(key);
            write(key, T0);
            wrapHandle(key, original -> delegating(original, method -> {
                if (!method.equals("checkpointAsyncTimed")) {
                    return null;
                }
                calls.incrementAndGet();
                return original.checkpointAsyncTimed().thenCompose(ignored -> gate);
            }));
        }
        LocalCheckpointSettings two = new LocalCheckpointSettings(true, Duration.ofMillis(100), 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, two)) {
            Future<?> cycle = executor.submit(() -> {
                checkpointer.runCycle();
                return null;
            });
            awaitTrue(() -> calls.get() == 2);
            Thread.sleep(200);
            assertEquals(2, calls.get(), "o ciclo deveria esperar vaga antes de enfileirar o terceiro");
            assertEquals(2, checkpointer.inFlightCount());
            assertFalse(cycle.isDone());

            gate.complete(null);
            cycle.get(10, TimeUnit.SECONDS);
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            assertEquals(5, calls.get());
            assertEquals(5L, checkpointer.metrics().get("localCheckpoint.checkpointed"));
            assertTrue(keys.stream().noneMatch(registry::isDirty));
            checkpointer.runCycle();
            assertEquals(5, calls.get(), "candidatas limpas não geram checkpoint");
            assertTrue(registry.dirtySeries().isEmpty(), "o ciclo seguinte poda as limpas do índice");
        } finally {
            gate.complete(null);
            executor.shutdownNow();
        }
    }

    @Test
    void falhaDeCheckpointEContadaEASerieSegueSujaParaOProximoCiclo() throws Exception {
        open("flaky");
        write("flaky", T0);
        AtomicBoolean failNext = new AtomicBoolean(true);
        wrapHandle("flaky", original -> delegating(original, method -> {
            if (method.equals("checkpointAsyncTimed") && failNext.getAndSet(false)) {
                return CompletableFuture.failedFuture(new IllegalStateException("fsync simulado falhou"));
            }
            return null;
        }));
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            assertEquals(1L, checkpointer.metrics().get("localCheckpoint.failures"));
            assertEquals(0L, checkpointer.metrics().get("localCheckpoint.checkpointed"));
            assertTrue(registry.isDirty("flaky"));

            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            assertEquals(1L, checkpointer.metrics().get("localCheckpoint.checkpointed"));
            assertFalse(registry.isDirty("flaky"));
        }
    }

    @Test
    void serieCongeladaPorMigracaoOuFechadaEPulada() throws Exception {
        open("frozen");
        write("frozen", T0);
        AtomicInteger syncs = countCheckpoints("frozen");
        registry.beginMigrationCopy("frozen");
        registry.markMigrating("frozen");
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
            assertEquals(0, syncs.get());
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.UNAVAILABLE,
                    registry.tryCheckpointAsync("frozen").outcome());
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.UNAVAILABLE,
                    registry.tryCheckpointAsync("absent").outcome());
        }
    }

    @Test
    void agendamentoRodaCiclosSemSobreposicaoEParaNoClose() throws Exception {
        open("scheduled");
        write("scheduled", T0);
        LocalCheckpointer checkpointer = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofMillis(50), 2));
        checkpointer.start();
        awaitTrue(() -> checkpointer.metrics().get("localCheckpoint.cycles") >= 2);
        // O ciclo termina ao enfileirar o checkpoint; a série só fica limpa quando ele conclui (assíncrono).
        awaitTrue(() -> !registry.isDirty("scheduled"));
        assertTrue(threadAlive("ngrrd-local-checkpoint"));

        checkpointer.close();
        long cyclesAtClose = checkpointer.metrics().get("localCheckpoint.cycles");
        awaitTrue(() -> !threadAlive("ngrrd-local-checkpoint"));
        Thread.sleep(200);
        assertEquals(cyclesAtClose, checkpointer.metrics().get("localCheckpoint.cycles"));
        checkpointer.close();
    }

    @Test
    void desligadoNaoCriaOComponenteEAsMetricasSaemZeradas() {
        assertThrows(IllegalArgumentException.class,
                () -> new LocalCheckpointer(registry, LocalCheckpointSettings.disabled()));
        // Como no nó com o checkpoint local desligado: registro sem índice de sujas.
        try (SeriesHandleRegistry untracked = new SeriesHandleRegistry(volume, VOLUME_NAME, Duration.ofMinutes(10),
                10_000, clock)) {
            untracked.open("pending", yaml, Ngrrd.OpenOptions.defaults());
            assertTrue(untracked.withSeries("pending", access -> {
                access.handle().write("in_octets", new Sample(T0, 1_000d));
                access.recordWrite();
                return true;
            }).orElse(false));
            Map<String, Long> metrics = LocalCheckpointer.disabledMetrics(untracked);
            assertEquals(0L, metrics.get("localCheckpoint.enabled"));
            assertEquals(1L, metrics.get("localCheckpoint.dirtySeries"));
            assertEquals(0L, metrics.get("localCheckpoint.lastCycleStarted"));
            assertEquals(List.of("localCheckpoint.enabled", "localCheckpoint.dirtySeries", "localCheckpoint.cycles",
                    "localCheckpoint.lastCycleMs", "localCheckpoint.lastCycleSeries", "localCheckpoint.checkpointed",
                    "localCheckpoint.skippedBusy", "localCheckpoint.failures", "localCheckpoint.overruns",
                    "localCheckpoint.queueWaitMsP99", "localCheckpoint.queueWaitMsMax", "localCheckpoint.forceMsP99",
                    "localCheckpoint.forceMsMax", "localCheckpoint.coalesced", "localCheckpoint.lastCycleStarted"),
                    List.copyOf(metrics.keySet()));
        }
    }

    @Test
    void checkpointsDeFechamentoEDeMigracaoTambemLimpamASerie() {
        open("snap");
        write("snap", T0);
        registry.beginMigrationCopy("snap");
        registry.migrationSnapshot("snap", () -> new byte[0]);
        assertFalse(registry.isDirty("snap"), "o checkpoint do snapshot de migração cobre as escritas anteriores");
        registry.clearMigrating("snap");
        write("snap", T0 + 300_000L);
        assertTrue(registry.isDirty("snap"));
        registry.discard("snap");
        assertFalse(registry.isDirty("snap"));
        assertEquals(0, registry.dirtyCount());
    }

    @Test
    void closeDuranteACadenciaDeUmCicloRetornaNaHora() throws Exception {
        for (String key : List.of("c0", "c1", "c2")) {
            open(key);
            write(key, T0);
        }
        // Intervalo grande: entre um disparo e o próximo a thread dorme ~16 s na cadência.
        LocalCheckpointer checkpointer = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofSeconds(60), 2));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> cycle = executor.submit(() -> {
                checkpointer.runCycle();
                return null;
            });
            awaitTrue(() -> checkpointer.metrics().get("localCheckpoint.checkpointed") == 1L);
            Thread.sleep(100);
            assertFalse(cycle.isDone(), "o ciclo deveria estar dormindo na cadência");

            long started = System.nanoTime();
            checkpointer.close();
            cycle.get(1, TimeUnit.SECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMs < 1_000, "close() levou " + elapsedMs + " ms");
            assertEquals(2L, Stream.of("c0", "c1", "c2").filter(registry::isDirty).count(),
                    "o restante do ciclo é abandonado no close");
        } finally {
            checkpointer.close();
            executor.shutdownNow();
        }
    }

    @Test
    void spreadFractionConfiguravelDefineAduracaoDaCadenciaDoCiclo() throws Exception {
        List<String> keys = List.of("p0", "p1", "p2");
        for (String key : keys) {
            open(key);
            write(key, T0);
        }
        // Intervalo de 3 s e fração 1,0: o disparo i de 3 sai em i × 1 s, então o ciclo dura ~2 s.
        try (LocalCheckpointer wide = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofSeconds(3), 3, 0, 1.0))) {
            wide.runCycle();
            awaitTrue(() -> wide.inFlightCount() == 0);
            assertTrue(wide.metrics().get("localCheckpoint.lastCycleMs") >= 1_900L,
                    "fração 1,0 espalha pelo intervalo inteiro: " + wide.metrics());
        }
        for (String key : keys) {
            write(key, T0 + 300_000L);
        }
        // Mesma entrada com fração 0,1: os disparos se espalham por 300 ms, e o ciclo dura ~200 ms.
        try (LocalCheckpointer narrow = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofSeconds(3), 3, 0, 0.1))) {
            narrow.runCycle();
            awaitTrue(() -> narrow.inFlightCount() == 0);
            long elapsed = narrow.metrics().get("localCheckpoint.lastCycleMs");
            assertTrue(elapsed < 1_000L, "fração 0,1 espalha por ~300 ms: " + elapsed + " ms");
            assertEquals(3L, narrow.metrics().get("localCheckpoint.checkpointed"));
        }
    }

    @Test
    void metricasDeFilaEForceContamSoOsCheckpointsLocaisQueForcaram() throws Exception {
        for (String key : List.of("forced-a", "forced-b", "coalesced", "unmeasured", "failing")) {
            open(key);
            write(key, T0);
        }
        stubTiming("forced-a", CompletableFuture.completedFuture(
                new NgrrdHandle.CheckpointTiming(TimeUnit.MILLISECONDS.toNanos(5), TimeUnit.MILLISECONDS.toNanos(12),
                        false)));
        stubTiming("forced-b", CompletableFuture.completedFuture(
                new NgrrdHandle.CheckpointTiming(TimeUnit.MILLISECONDS.toNanos(40), TimeUnit.MILLISECONDS.toNanos(7),
                        false)));
        stubTiming("coalesced", CompletableFuture.completedFuture(
                new NgrrdHandle.CheckpointTiming(TimeUnit.MILLISECONDS.toNanos(3), 0L, true)));
        stubTiming("unmeasured", CompletableFuture.completedFuture(NgrrdHandle.CheckpointTiming.NONE));
        stubTiming("failing", CompletableFuture.failedFuture(new IllegalStateException("force falhou")));

        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            Map<String, Long> before = checkpointer.metrics();
            assertEquals(0L, before.get("localCheckpoint.queueWaitMsMax"));
            assertEquals(0L, before.get("localCheckpoint.coalesced"));

            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);

            Map<String, Long> metrics = checkpointer.metrics();
            assertEquals(4L, metrics.get("localCheckpoint.checkpointed"));
            assertEquals(1L, metrics.get("localCheckpoint.failures"));
            assertEquals(1L, metrics.get("localCheckpoint.coalesced"),
                    "só o coalescido medido conta; NONE (sem medida) não conta em nada");
            assertEquals(40L, metrics.get("localCheckpoint.queueWaitMsP99"));
            assertEquals(40L, metrics.get("localCheckpoint.queueWaitMsMax"));
            assertEquals(12L, metrics.get("localCheckpoint.forceMsP99"));
            assertEquals(12L, metrics.get("localCheckpoint.forceMsMax"));
        }
    }

    @Test
    void checkpointLocalRealMedeFilaEForceSemExecutor() throws Exception {
        open("real");
        write("real", T0);
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
            awaitTrue(() -> checkpointer.inFlightCount() == 0);
            Map<String, Long> metrics = checkpointer.metrics();
            assertEquals(1L, metrics.get("localCheckpoint.checkpointed"));
            assertEquals(0L, metrics.get("localCheckpoint.coalesced"), "série suja força: não é coalescida");
            assertTrue(metrics.get("localCheckpoint.forceMsMax") >= 0L);
        }
    }

    @Test
    void serieLimpaOuIndisponivelDevolveAVagaDoSemaforo() throws Exception {
        open("clean");
        open("frozen");
        write("frozen", T0);
        registry.beginMigrationCopy("frozen");
        registry.markMigrating("frozen");
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofMillis(100), 1))) {
            checkpointer.checkpointOne("clean");
            assertEquals(0, checkpointer.inFlightCount(), "CLEAN devolve a vaga");
            checkpointer.checkpointOne("absent");
            checkpointer.checkpointOne("frozen");
            assertEquals(0, checkpointer.inFlightCount(), "UNAVAILABLE devolve a vaga");
            assertEquals(0L, checkpointer.metrics().get("localCheckpoint.failures"));
        }
    }

    @Test
    void excecaoAoEnfileirarOCheckpointDevolveAVagaEContaFalha() throws Exception {
        open("rejecting");
        write("rejecting", T0);
        wrapHandle("rejecting", original -> delegating(original, method -> {
            if (method.equals("checkpointAsyncTimed")) {
                throw new IllegalStateException("pool de writers recusou a tarefa");
            }
            return null;
        }));
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry,
                new LocalCheckpointSettings(true, Duration.ofMillis(100), 1))) {
            checkpointer.checkpointOne("rejecting");
            assertEquals(0, checkpointer.inFlightCount(), "a exceção não pode prender a vaga");
            assertEquals(1L, checkpointer.metrics().get("localCheckpoint.failures"));
            assertTrue(registry.isDirty("rejecting"));
        }
    }

    @Test
    void checkpointSincronoComFalhaNaoMarcaASerieComoLimpa() {
        open("sync-fail");
        write("sync-fail", T0);
        AtomicBoolean failCheckpoint = new AtomicBoolean(true);
        wrapHandle("sync-fail", original -> delegating(original, method -> {
            if (method.equals("checkpointAsync") && failCheckpoint.get()) {
                throw new IllegalStateException("fsync simulado falhou");
            }
            return null;
        }));
        registry.beginMigrationCopy("sync-fail");
        assertThrows(IllegalStateException.class, () -> registry.migrationSnapshot("sync-fail", () -> new byte[0]));
        assertTrue(registry.isDirty("sync-fail"), "snapshot de migração com falha deixa a série suja");

        failCheckpoint.set(false);
        registry.migrationSnapshot("sync-fail", () -> new byte[0]);
        assertFalse(registry.isDirty("sync-fail"), "o mesmo caminho com sucesso marca a série como limpa");
    }

    @Test
    void serieCongeladaComHandleAbertoNaoEnfileiraCheckpointLocal() throws Exception {
        open("frozen-open");
        write("frozen-open", T0);
        AtomicInteger asyncCalls = new AtomicInteger();
        AtomicBoolean failSyncCheckpoint = new AtomicBoolean(true);
        wrapHandle("frozen-open", original -> delegating(original, method -> {
            if (method.equals("checkpointAsyncTimed")) {
                asyncCalls.incrementAndGet();
            }
            if (method.equals("checkpointAsync") && failSyncCheckpoint.getAndSet(false)) {
                throw new IllegalStateException("checkpoint do cutover falhou");
            }
            return null;
        }));
        registry.beginMigrationCopy("frozen-open");
        // O checkpoint do cutover falha: a série fica congelada e o handle continua aberto.
        assertThrows(IllegalStateException.class, () -> registry.markMigrating("frozen-open"));
        assertTrue(registry.isMigrationFrozen("frozen-open"));
        assertEquals(1, registry.openCount());
        assertTrue(registry.isDirty("frozen-open"));

        assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.UNAVAILABLE,
                registry.tryCheckpointAsync("frozen-open").outcome());
        try (LocalCheckpointer checkpointer = new LocalCheckpointer(registry, FAST)) {
            checkpointer.runCycle();
        }
        assertEquals(0, asyncCalls.get(), "série congelada não recebe checkpoint local");
        registry.clearMigrating("frozen-open");
    }

    // ------------------------------------------------------------------ apoio

    private void open(String key) {
        registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
    }

    private void write(String key, long tsEpochMs) {
        assertTrue(registry.withSeries(key, access -> {
            access.handle().write("in_octets", new Sample(tsEpochMs, 1_000d));
            access.recordWrite();
            return true;
        }).orElse(false));
    }

    private AtomicInteger countCheckpoints(String key) {
        AtomicInteger counter = new AtomicInteger();
        wrapHandle(key, original -> delegating(original, method -> {
            if (method.equals("checkpointAsyncTimed")) {
                counter.incrementAndGet();
            }
            return null;
        }));
        return counter;
    }

    private void stubTiming(String key, CompletableFuture<NgrrdHandle.CheckpointTiming> timing) {
        wrapHandle(key, original -> delegating(original, method -> method.equals("checkpointAsyncTimed") ? timing : null));
    }

    private void wrapHandle(String key, java.util.function.UnaryOperator<NgrrdHandle> wrapper) {
        try {
            Field entriesField = SeriesHandleRegistry.class.getDeclaredField("entries");
            entriesField.setAccessible(true);
            Object entry = ((Map<?, ?>) entriesField.get(registry)).get(key);
            Field handleField = entry.getClass().getDeclaredField("handle");
            handleField.setAccessible(true);
            handleField.set(entry, wrapper.apply((NgrrdHandle) handleField.get(entry)));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Proxy que delega ao original, exceto quando {@code override} devolve um valor não nulo pelo nome do método. */
    private static NgrrdHandle delegating(NgrrdHandle original,
            java.util.function.Function<String, Object> override) {
        return (NgrrdHandle) Proxy.newProxyInstance(NgrrdHandle.class.getClassLoader(),
                new Class<?>[]{NgrrdHandle.class}, (proxy, method, args) -> {
                    Object overridden = override.apply(method.getName());
                    if (overridden != null) {
                        return overridden;
                    }
                    try {
                        return method.invoke(original, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condição não atingida em 10 s");
            }
            Thread.sleep(10);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean threadAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name) && t.isAlive());
    }

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
}
