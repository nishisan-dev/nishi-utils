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
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o ciclo de vida de {@link SeriesHandleRegistry} sobre um
 * {@link BlobVolume} real em {@code @TempDir}: abertura/reuso, fechamento por
 * ociosidade, eviction LRU, reabertura pelo cache de definições, bloqueio por
 * migração e durabilidade dos dados através de um ciclo close/reopen.
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
