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
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o ciclo de vida de {@link SeriesHandleRegistry} sobre um
 * {@link BlobVolume} real em {@code @TempDir}: abertura/reuso, fechamento por
 * ociosidade, eviction LRU, reabertura pelo cache de definições, bloqueio por
 * migração e durabilidade dos dados através de um ciclo close/reopen. Cobre também o índice incremental de
 * séries sujas (8.15.0): indexação, poda no ciclo e nos fechamentos, corridas de poda e escrita, e o modo
 * desligado.
 */
class SeriesHandleRegistryTest {

    private static final String VOLUME_NAME = "ngrrd";

    private String yaml;
    private BlobVolumeRegistry volumeRegistry;
    private BlobVolume volume;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        Path yamlPath = Path.of("src/test/resources/iface-traffic-blob.yaml");
        yaml = Files.readString(yamlPath, StandardCharsets.UTF_8);
        volumeRegistry = NgrrdBlob.registry().basePath(tempDir).volume(VOLUME_NAME).build();
        volume = volumeRegistry.require(VOLUME_NAME);
    }

    @AfterEach
    void tearDown() {
        volumeRegistry.close();
    }

    private SeriesHandleRegistry registry(Duration idleTtl, int maxOpenHandles, Clock clock) {
        return new SeriesHandleRegistry(volume, VOLUME_NAME, idleTtl, maxOpenHandles, clock);
    }

    @Test
    void mudancasDePosseAvisamOsListenersMesmoSemHandleAberto() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            List<String> notified = new ArrayList<>();
            registry.addOwnershipChangeListener(notified::add);
            registry.addOwnershipChangeListener(key -> {
                throw new IllegalStateException("listener com defeito não interrompe a operação");
            });

            registry.beginMigrationCopy("copy");
            registry.markMigrating("mark");
            registry.clearMigrating("clear");
            registry.discard("discard");
            registry.forget("forget");
            registry.open("closed", yaml, Ngrrd.OpenOptions.defaults());
            registry.close("closed");

            assertTrue(notified.containsAll(List.of("copy", "mark", "clear", "discard", "forget", "closed")),
                    "toda mudança de posse deveria ter sido avisada: " + notified);
            assertFalse(registry.isOpen("closed"));
        }
    }

    @Test
    void openReabreOMesmoHandleParaAMesmaChaveEExistingOEnxerga() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            NgrrdHandle first = registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());
            NgrrdHandle second = registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());

            assertTrue(first == second, "open da mesma chave deveria devolver o mesmo handle");
            assertEquals(1, registry.openCount());
            assertTrue(registry.existing("series-1").isPresent());
            assertTrue(registry.existing("series-1").get() == first);
            assertTrue(registry.existing("series-desconhecida").isEmpty());
        }
    }

    @Test
    void closeIdleFechaSoQuemPassouDoTtl() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            registry.open("series-fria", yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofMinutes(5));
            registry.open("series-quente", yaml, Ngrrd.OpenOptions.defaults());

            // Passa dos 10 min de TTL só para "series-fria" (aberta 5 min antes).
            clock.advance(Duration.ofMinutes(6));
            int closedCount = registry.closeIdle();

            assertEquals(1, closedCount);
            assertEquals(1, registry.openCount());
            assertTrue(registry.existing("series-fria").isEmpty());
            assertTrue(registry.existing("series-quente").isPresent());
        }
    }

    @Test
    void evictIfOverLimitFechaOMenosRecentementeUsadoQuandoExcedeOLimite() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofHours(1), 2, clock)) {
            registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofSeconds(1));
            registry.open("series-2", yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofSeconds(1));
            // Terceira abertura excede o limite de 2 -> evictIfOverLimit (chamado dentro de open)
            // deve fechar a menos recentemente usada, que é "series-1".
            registry.open("series-3", yaml, Ngrrd.OpenOptions.defaults());

            assertEquals(2, registry.openCount());
            assertTrue(registry.existing("series-1").isEmpty(), "series-1 (LRU) deveria ter sido fechada");
            assertTrue(registry.existing("series-2").isPresent());
            assertTrue(registry.existing("series-3").isPresent());
        }
    }

    @Test
    void reopenIfKnownReabreAposFechamentoPorOciosidadeSemPrecisarDoYamlDeNovo() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofMinutes(11));
            assertEquals(1, registry.closeIdle());
            assertTrue(registry.existing("series-1").isEmpty());

            Optional<NgrrdHandle> reopened = registry.reopenIfKnown("series-1");

            assertTrue(reopened.isPresent());
            assertEquals(1, registry.openCount());
        }
    }

    @Test
    void reopenIfKnownDevolveVazioParaChaveNuncaAberta() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            assertTrue(registry.reopenIfKnown("nunca-abri-essa").isEmpty());
        }
    }

    @Test
    void markMigratingBloqueiaOpenExistingEReopen() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());

            registry.markMigrating("series-1");

            assertTrue(registry.isMigrating("series-1"));
            assertTrue(registry.existing("series-1").isEmpty(), "handle deveria ter sido fechado ao marcar migrating");
            assertTrue(registry.reopenIfKnown("series-1").isEmpty());
            assertThrows(IllegalStateException.class,
                    () -> registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults()));

            registry.clearMigrating("series-1");
            assertFalse(registry.isMigrating("series-1"));
            assertTrue(registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults()) != null);
        }
    }

    @Test
    void failedCutoverCheckpointKeepsTheWriterForAbortRecovery() throws Exception {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            String key = "checkpoint-failure";
            NgrrdHandle original = registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
            var entriesField = SeriesHandleRegistry.class.getDeclaredField("entries");
            entriesField.setAccessible(true);
            Object entry = ((Map<?, ?>) entriesField.get(registry)).get(key);
            var handleField = entry.getClass().getDeclaredField("handle");
            handleField.setAccessible(true);
            var failCheckpoint = new AtomicBoolean(true);
            NgrrdHandle failing = (NgrrdHandle) java.lang.reflect.Proxy.newProxyInstance(
                    NgrrdHandle.class.getClassLoader(), new Class<?>[]{NgrrdHandle.class}, (proxy, method, args) -> {
                        if (method.getName().equals("checkpoint") && failCheckpoint.getAndSet(false)) {
                            throw new IllegalStateException("injected checkpoint failure");
                        }
                        try { return method.invoke(original, args); }
                        catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                    });
            handleField.set(entry, failing);
            registry.beginMigrationCopy(key);
            assertThrows(IllegalStateException.class, () -> registry.markMigrating(key));
            assertTrue(registry.isMigrationFrozen(key));
            assertEquals(1, registry.openCount(), "a failed checkpoint must not discard the writer");
            registry.clearMigrating(key);
            assertTrue(registry.withHandle(key, h -> {
                h.write("in_octets", new Sample(1_700_000_100_000L, 123));
                h.checkpoint();
                return true;
            }).orElse(false));
        }
    }

    @Test
    void closeDoRegistryFechaTodosOsHandlesAbertos() {
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC());
        registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());
        registry.open("series-2", yaml, Ngrrd.OpenOptions.defaults());
        assertEquals(2, registry.openCount());

        registry.close();

        assertEquals(0, registry.openCount());
        assertTrue(registry.openSeries().isEmpty());
    }

    @Test
    void dadosEscritosAntesDoFechamentoPorOciosidadeSaoLidosAposReabrir() {
        // Fechamento por ociosidade (não por CLOSE explícito do cliente) -> reopenIfKnown reabre
        // sozinho; é exatamente o caminho de auto-cura que write/checkpoint/flush usam.
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock);
        String seriesKey = "device:r1/iface:eth0";
        long baseStepMs = 300_000L;
        long t0 = (System.currentTimeMillis() / baseStepMs) * baseStepMs - 10 * baseStepMs;

        NgrrdHandle handle = registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        handle.write("in_octets", new Sample(t0, 1_000d));
        handle.write("in_octets", new Sample(t0 + baseStepMs, 1_500d));
        handle.checkpoint();

        clock.advance(Duration.ofMinutes(11));
        assertEquals(1, registry.closeIdle());
        assertTrue(registry.existing(seriesKey).isEmpty());

        Optional<NgrrdHandle> reopened = registry.reopenIfKnown(seriesKey);
        assertTrue(reopened.isPresent());

        SeriesResult result = reopened.get().read("in_bps",
                new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 500),
                t0 + 2 * baseStepMs);
        assertFalse(result.points().isEmpty());
        double expectedBps = 500.0 * 8 / 300.0;
        boolean foundExpectedRate = result.points().stream()
                .anyMatch(point -> !Double.isNaN(point.value()) && Math.abs(point.value() - expectedBps) < 0.5);
        assertTrue(foundExpectedRate,
                "esperava achar a taxa derivada (~" + expectedBps + " bit/s) entre os pontos após reabrir: "
                        + result.points());

        registry.close();
    }

    @Test
    void closeExplicitoDoClienteImpedeReopenIfKnownAteNovoOpen() {
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC());
        String seriesKey = "series-fechada-pelo-cliente";
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());

        registry.close(seriesKey);

        assertTrue(registry.reopenIfKnown(seriesKey).isEmpty(),
                "reopenIfKnown não deveria reabrir uma série fechada explicitamente pelo cliente");

        // Um novo OPEN explícito limpa a marca e volta a permitir reopenIfKnown.
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        assertTrue(registry.existing(seriesKey).isPresent());

        registry.close();
    }

    @Test
    void closeAposFechamentoPorOciosidadeTambemImpedeReopenIfKnown() {
        // O CLOSE do cliente pode chegar depois de o handle já ter sido fechado por ociosidade: mesmo sem
        // handle aberto, a reabertura automática precisa ficar bloqueada até um novo open explícito.
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            String seriesKey = "series-fechada-depois-de-ociosa";
            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofMinutes(11));
            assertEquals(1, registry.closeIdle());

            registry.close(seriesKey);

            assertTrue(registry.reopenIfKnown(seriesKey).isEmpty(),
                    "reopenIfKnown não deveria reabrir uma série fechada pelo cliente depois da ociosidade");
        }
    }

    @Test
    void closeDescartaADefinicaoEmCacheEOpenARestaura() {
        // O fechamento explícito não deixa marca por série: a própria definição em cache sai, e é a
        // ausência dela que impede a reabertura automática — nada cresce com o número de séries fechadas.
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            String seriesKey = "series-definicao-descartada";
            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            assertTrue(registry.cachedYaml(seriesKey).isPresent());

            registry.close(seriesKey);
            assertTrue(registry.cachedYaml(seriesKey).isEmpty(), "close() deveria descartar a definição em cache");

            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            assertTrue(registry.cachedYaml(seriesKey).isPresent(), "open() deveria voltar a cachear a definição");
        }
    }

    @Test
    void closeConcorrenteComReopenIfKnownNuncaDeixaASerieAberta() throws Exception {
        // A reconferência da definição sob o lock da entrada substitui a antiga marca closedByClient: não
        // importa a ordem entre close() e reopenIfKnown(), a série termina fechada.
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            for (int i = 0; i < 100; i++) {
                String seriesKey = "series-corrida-close-reopen-" + i;
                registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
                clock.advance(Duration.ofMinutes(11));
                registry.closeIdle();
                CountDownLatch start = new CountDownLatch(1);
                Future<?> reopen = executor.submit(() -> {
                    start.await();
                    return registry.reopenIfKnown(seriesKey);
                });
                Future<?> close = executor.submit(() -> {
                    start.await();
                    registry.close(seriesKey);
                    return null;
                });
                start.countDown();
                reopen.get(10, TimeUnit.SECONDS);
                close.get(10, TimeUnit.SECONDS);

                assertTrue(registry.existing(seriesKey).isEmpty(), "a série deveria terminar fechada: " + seriesKey);
                assertTrue(registry.reopenIfKnown(seriesKey).isEmpty());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void forgetMarcaASerieComoEsquecidaEFechaOHandleSemMantelaAberta() {
        // MIGRATE_FINISH (M3): a origem chama forget() depois de apagar a imagem local — a série deve
        // sair do registry (como discard) e ficar marcada isForgotten até um novo open() legítimo.
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC());
        String seriesKey = "device:rb1/iface:eth0";
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());

        registry.forget(seriesKey);

        assertTrue(registry.isForgotten(seriesKey), "forget() deveria marcar a série como esquecida");
        assertTrue(registry.existing(seriesKey).isEmpty(), "forget() deveria soltar o handle aberto");
        assertTrue(registry.reopenIfKnown(seriesKey).isEmpty(),
                "reopenIfKnown não deveria reabrir sozinho uma série esquecida");

        registry.close();
    }

    @Test
    void pruneForgottenDescartaSoAsMarcasIndicadasPeloPredicado() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            registry.forget("series-convergida");
            registry.forget("series-ainda-atrasada");
            assertEquals(2, registry.forgottenCount());

            int pruned = registry.pruneForgotten("series-convergida"::equals);

            assertEquals(1, pruned);
            assertFalse(registry.isForgotten("series-convergida"));
            assertTrue(registry.isForgotten("series-ainda-atrasada"));
            assertEquals(1, registry.forgottenCount());
        }
    }

    @Test
    void dropForgottenDescartaAMarcaSemReabrirASerie() {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC())) {
            String seriesKey = "series-marca-descartada";
            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            registry.forget(seriesKey);

            registry.dropForgotten(seriesKey);

            assertFalse(registry.isForgotten(seriesKey));
            assertTrue(registry.reopenIfKnown(seriesKey).isEmpty(),
                    "descartar a marca não devolve a definição esquecida");
        }
    }

    @Test
    void openAposForgetLimpaAMarcaDeEsquecida() {
        // "confirmação forte" no StorageRequestHandler só chama registry.open() depois de o líder
        // confirmar ACTIVE(self) — é esse open() legítimo que precisa limpar isForgotten, senão a
        // série ficaria presa no caminho de placementStrong para sempre, mesmo já reaberta de verdade.
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, Clock.systemUTC());
        String seriesKey = "device:rb1/iface:eth1";
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        registry.forget(seriesKey);
        assertTrue(registry.isForgotten(seriesKey));

        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());

        assertFalse(registry.isForgotten(seriesKey), "open() deveria limpar a marca de esquecida");
        assertTrue(registry.existing(seriesKey).isPresent());

        registry.close();
    }

    @Test
    void closeIdleConcorrenteComWithHandleNuncaFechaHandleEmUso() throws InterruptedException {
        // B1+B2: closeIdle() e withHandle() disputam o MESMO lock de entrada — enquanto o
        // checkpoint roda dentro de withHandle, closeIdle deve pular a entrada (tryLock falha),
        // nunca bloquear nem fechar o handle por baixo (o que faria o checkpoint responder OK sem
        // fazer nada, já que NgrrdWriter.sync() é um no-op silencioso quando fechado).
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock);
        String seriesKey = "series-em-uso";
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        clock.advance(Duration.ofMinutes(11)); // torna a entrada elegível para closeIdle

        CountDownLatch insideWithHandle = new CountDownLatch(1);
        CountDownLatch releaseWithHandle = new CountDownLatch(1);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        AtomicBoolean checkpointRan = new AtomicBoolean(false);

        Thread user = new Thread(() -> registry.withHandle(seriesKey, handle -> {
            insideWithHandle.countDown();
            try {
                assertTrue(releaseWithHandle.await(5, TimeUnit.SECONDS), "sinal de liberação não chegou a tempo");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                handle.checkpoint();
                checkpointRan.set(true);
            } catch (RuntimeException e) {
                writerError.set(e);
            }
            return null;
        }), "series-user");
        user.start();

        assertTrue(insideWithHandle.await(5, TimeUnit.SECONDS), "withHandle não entrou a tempo");
        int closedWhileBusy = registry.closeIdle();
        assertEquals(0, closedWhileBusy, "closeIdle não deveria conseguir fechar uma entrada em uso por withHandle");

        releaseWithHandle.countDown();
        user.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(user.isAlive(), "thread de uso não terminou a tempo");

        assertTrue(checkpointRan.get(), "checkpoint deveria ter rodado sem exceção dentro de withHandle");
        assertNull(writerError.get(), "checkpoint não deveria falhar com o handle fechado por baixo");

        // withHandle atualiza lastAccess ao usar o handle (comportamento correto: uso não deveria
        // deixar a série "aparentemente ociosa") — avança o relógio de novo antes de reconferir que,
        // com o lock livre, closeIdle agora consegue fechar de verdade.
        clock.advance(Duration.ofMinutes(11));
        assertEquals(1, registry.closeIdle());
        registry.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concorrenciaDeAberturaEEvictionPorDoisSegundosSemDeadlockNemUsoAposFechamento() throws InterruptedException {
        // B1+B2: 8 threads disputando 8 séries com maxOpenHandles=1 (toda abertura evict-a a
        // anterior) por >= 2s. Uma thread vigia usa ThreadMXBean.findDeadlockedThreads() — o
        // desenho antigo tinha um AB-BA entre o lock de open() e o de evictIfOverLimit() chamado
        // por dentro dele; o novo nunca segura dois locks de entrada ao mesmo tempo.
        try (SeriesHandleRegistry registry = registry(Duration.ofHours(1), 1, Clock.systemUTC())) {
            int threadCount = 8;
            int seriesCount = 8;
            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread watchdog = new Thread(() -> {
                ThreadMXBean bean = ManagementFactory.getThreadMXBean();
                while (!stop.get()) {
                    long[] deadlocked = bean.findDeadlockedThreads();
                    if (deadlocked != null && deadlocked.length > 0) {
                        failure.compareAndSet(null,
                                new AssertionError("Deadlock detectado entre threads: " + Arrays.toString(deadlocked)));
                        stop.set(true);
                        return;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "deadlock-watchdog");
            watchdog.setDaemon(true);
            watchdog.start();

            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            long testDeadline = System.currentTimeMillis() + 2_000L;
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    Random random = new Random();
                    while (System.currentTimeMillis() < testDeadline && !stop.get()) {
                        String seriesKey = "series-" + random.nextInt(seriesCount);
                        try {
                            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
                            registry.withHandle(seriesKey, handle -> {
                                handle.checkpoint();
                                return null;
                            });
                            registry.closeIdle();
                        } catch (IllegalStateException e) {
                            if (!"MIGRATING".equals(e.getMessage())) {
                                failure.compareAndSet(null, e);
                                stop.set(true);
                            }
                        } catch (RuntimeException e) {
                            failure.compareAndSet(null, e);
                            stop.set(true);
                        }
                    }
                }));
            }

            pool.shutdown();
            boolean terminated = pool.awaitTermination(20, TimeUnit.SECONDS);
            stop.set(true);
            watchdog.join(TimeUnit.SECONDS.toMillis(2));

            for (Future<?> future : futures) {
                assertTrue(future.isDone(), "uma thread de trabalho não terminou — possível deadlock");
            }
            assertTrue(terminated, "threads não terminaram a tempo — possível deadlock não pego pelo watchdog");
            Throwable observed = failure.get();
            if (observed != null) {
                throw new AssertionError("Falha durante a rajada de concorrência: " + observed, observed);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @SuppressWarnings("unchecked")
    void closeIdleToleraAtualizacaoConcorrenteDosHorariosEmMuitasEntradas() throws Exception {
        // Large cache without allocating thousands of on-disk series. Null handles are
        // legitimate entries whose OPEN is still in progress; sorting also includes them.
        try (SeriesHandleRegistry registry = registry(Duration.ofHours(1), 65_536,
                Clock.fixed(Instant.ofEpochMilli(1_000_000), ZoneOffset.UTC))) {
            var entriesField = SeriesHandleRegistry.class.getDeclaredField("entries");
            entriesField.setAccessible(true);
            Map<String, Object> entries = (Map<String, Object>) entriesField.get(registry);
            Class<?> entryType = Class.forName(SeriesHandleRegistry.class.getName() + "$HandleEntry");
            var constructor = entryType.getDeclaredConstructor();
            constructor.setAccessible(true);
            var lastAccess = entryType.getDeclaredField("lastAccessMs");
            lastAccess.setAccessible(true);
            Object[] values = new Object[32_768];
            Random initial = new Random(721);
            for (int i = 0; i < values.length; i++) {
                values[i] = constructor.newInstance();
                lastAccess.setLong(values[i], initial.nextInt(1_000_000));
                entries.put("concurrent-" + i, values[i]);
            }

            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch started = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> touches = executor.submit(() -> {
                Random random = new Random(918);
                long now = 1_000_000;
                try {
                    while (running.get()) {
                        lastAccess.setLong(values[random.nextInt(values.length)], ++now);
                        started.countDown();
                    }
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            });
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                for (int i = 0; i < 100; i++) {
                    assertEquals(0, registry.closeIdle());
                }
                assertEquals(values.length, entries.size());
            } finally {
                running.set(false);
                executor.shutdownNow();
                touches.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void openConcorrenteComCloseNaMesmaChaveNaoVazaHandleNemDevolveHandleFechado() throws InterruptedException {
        // A2 (TOCTOU): open() e close(key) disputando a MESMA chave em loop apertado. Sem a
        // reconferência de `entries.get(key) == entry` sob o lock, open() podia travar/operar numa
        // entrada já removida por um close() concorrente e devolver `entry.handle` já fechado como
        // se fosse válido. A janela é estreita — o teste cobre o comportamento observável ao final
        // de 1000 iterações: nunca mais de um handle aberto para a chave, e o estado de `isOpen`
        // sempre coerente com `openCount` (não há UB do tipo "aberto mas openCount==0").
        try (SeriesHandleRegistry registry = registry(Duration.ofHours(1), 10, Clock.systemUTC())) {
            String seriesKey = "series-disputada";
            int iterations = 1_000;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            Thread opener = new Thread(() -> {
                try {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
                        // Uso só via withHandle — nunca sobre o retorno bruto de open(): se a série
                        // ainda estiver aberta com este handle no instante do lock, o checkpoint deve
                        // rodar sem "Writer já fechado"; se um close() concorrente já a fechou,
                        // withHandle simplesmente não executa nada (Optional vazio), o que é correto.
                        registry.withHandle(seriesKey, handle -> {
                            handle.checkpoint();
                            return null;
                        });
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "opener");

            Thread closer = new Thread(() -> {
                try {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        registry.close(seriesKey);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "closer");

            opener.start();
            closer.start();
            assertTrue(ready.await(5, TimeUnit.SECONDS), "threads não ficaram prontas a tempo");
            start.countDown();
            opener.join(TimeUnit.SECONDS.toMillis(20));
            closer.join(TimeUnit.SECONDS.toMillis(20));

            assertFalse(opener.isAlive(), "opener não terminou a tempo — possível deadlock/livelock");
            assertFalse(closer.isAlive(), "closer não terminou a tempo — possível deadlock/livelock");

            Throwable observed = failure.get();
            if (observed != null) {
                throw new AssertionError("Falha durante a disputa open/close: " + observed, observed);
            }

            int openCount = registry.openCount();
            assertTrue(openCount == 0 || openCount == 1,
                    "openCount deveria ser 0 ou 1 (sem vazamento de handle), foi " + openCount);
            assertEquals(openCount == 1, registry.isOpen(seriesKey),
                    "isOpen deveria ser coerente com openCount para a única chave em disputa");
        }
    }

    @Test
    void reopenIfKnownAposObjetoRemovidoDevolveVazioENaoRecriaOObjeto() {
        // reopenIfKnown nunca deve recriar o objeto físico, mesmo que a definição cacheada (por hash de
        // YAML) tenha createIfMissing=true por ter sido a opção usada no open() original desta MESMA série.
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            String seriesKey = "series-objeto-removido";
            registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofMinutes(11));
            assertEquals(1, registry.closeIdle());
            assertTrue(registry.existing(seriesKey).isEmpty());

            String objectKey = SeriesObjectKeys.objectKey("series", seriesKey);
            assertTrue(volume.storage().exists(objectKey), "setup deveria ter deixado o objeto físico no volume");
            volume.storage().delete(objectKey);
            assertFalse(volume.storage().exists(objectKey));

            Optional<NgrrdHandle> reopened = registry.reopenIfKnown(seriesKey);

            assertTrue(reopened.isEmpty(), "reopenIfKnown não deveria recriar a série cujo objeto sumiu");
            assertFalse(volume.storage().exists(objectKey), "reopenIfKnown NUNCA deveria recriar o objeto físico");
        }
    }

    @Test
    void reopenIfKnownNuncaCriaMesmoComDefinicaoCompartilhadaPorOutraSerieComCreateIfMissingTrue() {
        // definitionByHash é compartilhado por YAML, não por seriesKey — uma série aberta com
        // createIfMissing=false não pode ser recriada pela auto-cura só porque outra série com o MESMO
        // YAML foi aberta depois com createIfMissing=true (options.createIfMissing sobrescrito no
        // DefinitionRecord compartilhado).
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            String seriesFalse = "series-sem-criar-compartilhada";
            String seriesTrue = "series-com-criar-compartilhada";

            // series-sem-criar precisa existir fisicamente ANTES de abrir com createIfMissing=false —
            // cria normalmente e fecha explicitamente para deixar só o objeto no disco.
            registry.open(seriesFalse, yaml, Ngrrd.OpenOptions.defaults());
            registry.close(seriesFalse);
            String objectKey = SeriesObjectKeys.objectKey("series", seriesFalse);
            assertTrue(volume.storage().exists(objectKey));

            registry.open(seriesFalse, yaml, Ngrrd.OpenOptions.defaults().withCreateIfMissing(false));
            clock.advance(Duration.ofMinutes(1));
            // Mesmo YAML (mesmo hash) -> sobrescreve o DefinitionRecord compartilhado com createIfMissing=true.
            registry.open(seriesTrue, yaml, Ngrrd.OpenOptions.defaults());

            clock.advance(Duration.ofMinutes(11));
            assertEquals(2, registry.closeIdle());
            assertTrue(registry.existing(seriesFalse).isEmpty());

            volume.storage().delete(objectKey);
            assertFalse(volume.storage().exists(objectKey));

            Optional<NgrrdHandle> reopened = registry.reopenIfKnown(seriesFalse);

            assertTrue(reopened.isEmpty(),
                    "reopenIfKnown não deveria recriar series-sem-criar mesmo com a definição compartilhada "
                            + "cacheada por createIfMissing=true (de series-com-criar)");
            assertFalse(volume.storage().exists(objectKey));
        }
    }

    @Test
    void reopenIfKnownComObjetoPresenteContinuaSeAutoCurandoNormalmente() {
        // A auto-cura normal (objeto físico presente) não pode quebrar com createIfMissing=false forçado
        // na reabertura — createIfMissing só impede CRIAR, nunca impede abrir um objeto que já existe.
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(10), 10, clock)) {
            String seriesKey = "series-autocura-normal";
            NgrrdHandle handle = registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
            handle.write("in_octets", new Sample(1_700_000_100_000L, 1_000d));
            handle.checkpoint();

            clock.advance(Duration.ofMinutes(11));
            assertEquals(1, registry.closeIdle());
            assertTrue(registry.existing(seriesKey).isEmpty());

            Optional<NgrrdHandle> reopened = registry.reopenIfKnown(seriesKey);

            assertTrue(reopened.isPresent(),
                    "reopenIfKnown deveria continuar reabrindo normalmente quando o objeto físico existe");
            assertEquals(1, registry.openCount());
        }
    }

    // ------------------------------------------------------------------ índice incremental de sujas (8.15.0)

    @Test
    void indiceContemSeriesEscritas() {
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            for (String key : List.of("a", "b", "c")) {
                registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
            }
            recordWrite(registry, "a");
            recordWrite(registry, "b");
            recordWrite(registry, "b");

            assertEquals(Set.of("a", "b"), registry.dirtySeries());
            assertEquals(2, registry.dirtyCount());
        }
    }

    @Test
    void checkpointLimpaPodaIndice() {
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            registry.open("a", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "a");

            SeriesHandleRegistry.CheckpointAttempt attempt = registry.tryCheckpointAsync("a");
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.STARTED, attempt.outcome());
            attempt.future().join();
            assertFalse(registry.isDirty("a"));
            assertEquals(Set.of("a"), registry.dirtySeries(),
                    "a conclusão do checkpoint não poda: a série sai só quando o ciclo seguinte a encontra limpa");

            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.CLEAN,
                    registry.tryCheckpointAsync("a").outcome());
            assertTrue(registry.dirtySeries().isEmpty());
            assertEquals(0, registry.dirtyCount());

            recordWrite(registry, "a");
            assertEquals(Set.of("a"), registry.dirtySeries(), "escrita depois da poda volta a indexar a série");
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void escritaConcorrenteAoCheckpointNaoSaiDoIndice() throws Exception {
        String key = "race";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            entries(registry).put(key, newEntry(FAKE_HANDLE));
            CyclicBarrier start = new CyclicBarrier(2);
            for (int i = 0; i < 10_000; i++) {
                Future<?> writer = pool.submit(() -> {
                    start.await();
                    recordWrite(registry, key);
                    return null;
                });
                Future<?> checkpointer = pool.submit(() -> {
                    start.await();
                    // Dois disparos: o primeiro costuma enfileirar o checkpoint, o segundo costuma achar a
                    // série limpa e podar, ambos concorrendo com a escrita.
                    for (int round = 0; round < 2; round++) {
                        SeriesHandleRegistry.CheckpointAttempt attempt = registry.tryCheckpointAsync(key);
                        if (attempt.outcome() == SeriesHandleRegistry.CheckpointAttempt.Outcome.STARTED) {
                            attempt.future().join();
                        }
                    }
                    return null;
                });
                writer.get(10, TimeUnit.SECONDS);
                checkpointer.get(10, TimeUnit.SECONDS);
                if (registry.isDirty(key)) {
                    assertTrue(registry.dirtySeries().contains(key), "série suja fora do índice na iteração " + i);
                }
            }
            recordWrite(registry, key);
            assertTrue(registry.dirtySeries().contains(key));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void entradaFechadaEReabertaMantemIndice() {
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            registry.open("a", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "a");
            registry.discard("a");
            assertFalse(registry.dirtySeries().contains("a"), "o fechamento checkpointa e poda");

            assertTrue(registry.reopenIfKnown("a").isPresent());
            recordWrite(registry, "a");
            assertEquals(Set.of("a"), registry.dirtySeries(), "a entrada nova se indexa na primeira escrita");

            // Entrada antiga indexada e substituída por uma nova antes da poda dela: a escrita na nova
            // encontra a chave já no índice (inserção sem efeito), e a poda atrasada da antiga não pode
            // tirá-la de lá.
            entries(registry).put("b", newEntry(FAKE_HANDLE));
            recordWrite(registry, "b");
            entries(registry).put("b", newEntry(FAKE_HANDLE));
            recordWrite(registry, "b");
            registry.pruneIndex("b");
            assertTrue(registry.dirtySeries().contains("b"));
        }
    }

    @Test
    void dirtySeriesNaoVarreEntradas() throws Exception {
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 200_000, Clock.systemUTC())) {
            ScanCountingMap<String, Object> counting = new ScanCountingMap<>();
            Field entriesField = SeriesHandleRegistry.class.getDeclaredField("entries");
            entriesField.setAccessible(true);
            entriesField.set(registry, counting);

            for (int i = 0; i < 10; i++) {
                registry.open("dirty-" + i, yaml, Ngrrd.OpenOptions.defaults());
                recordWrite(registry, "dirty-" + i);
            }
            for (int i = 0; i < 100_000; i++) {
                counting.put("clean-" + i, newEntry(FAKE_HANDLE));
            }
            registry.openCount();
            assertTrue(counting.scans.get() > 0, "o mapa instrumentado deveria contar uma varredura");

            counting.scans.set(0);
            Set<String> dirty = registry.dirtySeries();
            int dirtyCount = registry.dirtyCount();

            assertTrue(dirty.size() <= 10, "dirtySeries devolveu " + dirty.size());
            assertEquals(10, dirty.size());
            assertEquals(10, dirtyCount);
            assertEquals(0, counting.scans.get(), "dirtySeries/dirtyCount não podem varrer as entradas");
        }
    }

    @Test
    void indiceNaoExisteComCheckpointLocalDesligado() throws Exception {
        try (SeriesHandleRegistry registry = registry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            assertFalse(registry.dirtyIndexTracked(), "o padrão é sem índice");
            registry.open("a", yaml, Ngrrd.OpenOptions.defaults());
            registry.open("b", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "a");

            assertEquals(Set.of("a"), registry.dirtySeries());
            assertEquals(1, registry.dirtyCount());
            assertTrue(dirtyIndex(registry).isEmpty(), "recordWrite não toca índice nenhum");

            SeriesHandleRegistry.CheckpointAttempt attempt = registry.tryCheckpointAsync("a");
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.STARTED, attempt.outcome());
            attempt.future().join();
            assertTrue(registry.dirtySeries().isEmpty(), "sem índice, a varredura vê a série limpa na hora");
            assertEquals(0, registry.dirtyCount());
            assertTrue(dirtyIndex(registry).isEmpty());

            assertThrows(IllegalStateException.class, () -> registry.trackDirtyIndex(true),
                    "ligar o índice com entradas abertas perderia as escritas anteriores");
        }
    }

    @Test
    void fechamentoPorOciosidadeEDescartePodamIndice() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(10), 10, clock)) {
            registry.open("idle", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "idle");
            clock.advance(Duration.ofMinutes(11));
            for (String key : List.of("discarded", "client-closed", "deleted", "migrated")) {
                registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
                recordWrite(registry, key);
            }
            assertEquals(Set.of("idle", "discarded", "client-closed", "deleted", "migrated"),
                    registry.dirtySeries());

            assertEquals(1, registry.closeIdle());
            assertFalse(registry.dirtySeries().contains("idle"), "closeIdle poda");
            registry.discard("discarded");
            assertFalse(registry.dirtySeries().contains("discarded"), "discard poda");
            registry.close("client-closed");
            assertFalse(registry.dirtySeries().contains("client-closed"), "close(seriesKey) poda (via discard)");
            registry.closeForDeletion("deleted");
            assertFalse(registry.dirtySeries().contains("deleted"), "closeForDeletion poda");
            registry.markMigrating("migrated");
            assertFalse(registry.dirtySeries().contains("migrated"), "markMigrating poda");
            registry.clearMigrating("migrated");
            assertTrue(registry.dirtySeries().isEmpty());
        }

        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(10), 1, clock)) {
            registry.open("evicted", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "evicted");
            clock.advance(Duration.ofMinutes(1));
            registry.open("kept", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "kept");
            assertEquals(Set.of("kept"), registry.dirtySeries(), "evictIfOverLimit poda");
        }

        SeriesHandleRegistry closing = trackedRegistry(Duration.ofMinutes(10), 10, clock);
        closing.open("open-at-close", yaml, Ngrrd.OpenOptions.defaults());
        recordWrite(closing, "open-at-close");
        closing.close();
        assertTrue(closing.dirtySeries().isEmpty(), "close() do registro poda");
    }

    @Test
    void congeladoPorMigracaoNaoPoda() {
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            registry.open("frozen", yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, "frozen");
            registry.tryCheckpointAsync("frozen").future().join();
            assertFalse(registry.isDirty("frozen"));
            assertEquals(Set.of("frozen"), registry.dirtySeries(), "limpa, mas ainda no índice até a poda");

            AtomicBoolean failCheckpoint = new AtomicBoolean(true);
            wrapHandle(registry, "frozen", original -> delegating(original, method -> {
                if (method.equals("checkpoint") && failCheckpoint.getAndSet(false)) {
                    throw new IllegalStateException("checkpoint do cutover falhou");
                }
                return null;
            }));
            registry.beginMigrationCopy("frozen");
            assertThrows(IllegalStateException.class, () -> registry.markMigrating("frozen"));
            assertTrue(registry.isMigrationFrozen("frozen"));

            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.UNAVAILABLE,
                    registry.tryCheckpointAsync("frozen").outcome());
            assertEquals(Set.of("frozen"), registry.dirtySeries(),
                    "congelada por migração não poda: a migração pode abortar e a série voltar suja");

            registry.clearMigrating("frozen");
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.CLEAN,
                    registry.tryCheckpointAsync("frozen").outcome());
            assertTrue(registry.dirtySeries().isEmpty());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void podaConcorrenteComEscritaNaoPerdeChave() throws Exception {
        String key = "prune-race";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            CyclicBarrier start = new CyclicBarrier(2);
            for (int i = 0; i < 10_000; i++) {
                // A entrada antiga deixou a chave no índice e foi substituída (fechada e reaberta) antes de a
                // poda dela rodar: a poda atrasada concorre com a primeira escrita na entrada nova.
                entries(registry).put(key, newEntry(FAKE_HANDLE));
                recordWrite(registry, key);
                entries(registry).put(key, newEntry(FAKE_HANDLE));

                Future<?> pruner = pool.submit(() -> {
                    start.await();
                    registry.pruneIndex(key);
                    return null;
                });
                Future<?> writer = pool.submit(() -> {
                    start.await();
                    recordWrite(registry, key);
                    return null;
                });
                pruner.get(10, TimeUnit.SECONDS);
                writer.get(10, TimeUnit.SECONDS);
                assertTrue(registry.isDirty(key));
                assertTrue(registry.dirtySeries().contains(key), "chave perdida do índice na iteração " + i);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------ apoio do índice de sujas

    /** Handle falso: checkpoints concluem (o assíncrono em outra thread) e o resto não faz nada. */
    private static final NgrrdHandle FAKE_HANDLE = (NgrrdHandle) Proxy.newProxyInstance(
            NgrrdHandle.class.getClassLoader(), new Class<?>[]{NgrrdHandle.class}, (proxy, method, args) ->
                    switch (method.getName()) {
                        case "checkpointAsyncTimed" ->
                                CompletableFuture.supplyAsync(() -> NgrrdHandle.CheckpointTiming.NONE);
                        case "checkpointAsync" -> CompletableFuture.completedFuture(null);
                        case "toString" -> "fake-handle";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> null;
                    });

    private SeriesHandleRegistry trackedRegistry(Duration idleTtl, int maxOpenHandles, Clock clock) {
        SeriesHandleRegistry registry = registry(idleTtl, maxOpenHandles, clock);
        registry.trackDirtyIndex(true);
        return registry;
    }

    private static void recordWrite(SeriesHandleRegistry registry, String key) {
        assertTrue(registry.withSeries(key, access -> {
            access.recordWrite();
            return true;
        }).orElse(false), "série " + key + " deveria estar aberta");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> entries(SeriesHandleRegistry registry) {
        try {
            Field field = SeriesHandleRegistry.class.getDeclaredField("entries");
            field.setAccessible(true);
            return (Map<String, Object>) field.get(registry);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> dirtyIndex(SeriesHandleRegistry registry) {
        try {
            Field field = SeriesHandleRegistry.class.getDeclaredField("dirtyIndex");
            field.setAccessible(true);
            return (Set<String>) field.get(registry);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Entrada nova (limpa, não indexada) do registro, com o handle dado. */
    private static Object newEntry(NgrrdHandle handle) {
        try {
            Class<?> type = Class.forName(SeriesHandleRegistry.class.getName() + "$HandleEntry");
            Constructor<?> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object entry = constructor.newInstance();
            Field handleField = type.getDeclaredField("handle");
            handleField.setAccessible(true);
            handleField.set(entry, handle);
            return entry;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void wrapHandle(SeriesHandleRegistry registry, String key, UnaryOperator<NgrrdHandle> wrapper) {
        try {
            Object entry = entries(registry).get(key);
            Field handleField = entry.getClass().getDeclaredField("handle");
            handleField.setAccessible(true);
            handleField.set(entry, wrapper.apply((NgrrdHandle) handleField.get(entry)));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Proxy que delega ao original, exceto quando {@code override} devolve um valor não nulo pelo nome do método. */
    private static NgrrdHandle delegating(NgrrdHandle original, Function<String, Object> override) {
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

    /** Mapa que conta toda varredura (chaves, valores, entradas, {@code forEach}). */
    private static final class ScanCountingMap<K, V> extends ConcurrentHashMap<K, V> {
        private final AtomicInteger scans = new AtomicInteger();

        @Override
        public KeySetView<K, V> keySet() {
            scans.incrementAndGet();
            return super.keySet();
        }

        @Override
        public Collection<V> values() {
            scans.incrementAndGet();
            return super.values();
        }

        @Override
        public Set<Map.Entry<K, V>> entrySet() {
            scans.incrementAndGet();
            return super.entrySet();
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            scans.incrementAndGet();
            super.forEach(action);
        }
    }

    /** {@link Clock} determinístico para controlar {@code lastAccess} nos testes. */
    private static final class MutableClock extends Clock {
        private Instant instant;

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
