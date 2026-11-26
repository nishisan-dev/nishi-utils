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
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.writer.WriterAbandonedException;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    private SeriesHandleRegistry registryWithStepFactor(Duration idleTtl, int stepFactor, Clock clock) {
        return new SeriesHandleRegistry(volume, VOLUME_NAME, idleTtl, stepFactor, 10, clock);
    }

    /** Variante do YAML de teste com outro passo base; o passo do RRA acompanha para a definição continuar válida. */
    private String yamlWithBaseStep(int baseStepSec) {
        String changed = yaml
                .replace("baseStepSec: 300", "baseStepSec: " + baseStepSec)
                .replace("        stepSec: 300\n", "        stepSec: " + baseStepSec + "\n")
                .replace("targetStepSec: 300", "targetStepSec: " + baseStepSec);
        assertTrue(!changed.equals(yaml), "o YAML de teste deveria ter sido alterado");
        return changed;
    }

    @Test
    void ttlEfetivoUsaPassoDaDefinicao() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registryWithStepFactor(Duration.ofMinutes(15), 2, clock)) {
            registry.open("series-passo", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());
            registry.open("series-curta", yaml, Ngrrd.OpenOptions.defaults());

            clock.advance(Duration.ofMinutes(16));
            assertEquals(1, registry.closeIdle(), "só a série de passo curto (TTL efetivo 15 min) deveria fechar");
            // openCount não toca lastAccess; isOpen passaria por withHandle e renovaria a ociosidade.
            assertEquals(1, registry.openCount(), "passo de 900 s com k=2 mantém a série aberta com 16 min");

            clock.advance(Duration.ofMinutes(15));
            assertEquals(1, registry.closeIdle(), "com 31 min de ociosidade passa dos 30 min (2 x 900 s)");
            assertEquals(0, registry.openCount());
        }
    }

    @Test
    void fatorZeroMantemTtlGlobal() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registryWithStepFactor(Duration.ofMinutes(15), 0, clock)) {
            registry.open("series-passo", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());

            clock.advance(Duration.ofMinutes(16));

            assertEquals(1, registry.closeIdle());
            assertEquals(0, registry.openCount());
            assertEquals(0L, registry.handleMetrics().get("handles.idleTtlExtended"));
        }
    }

    @Test
    void definicaoParseadaUmaVezPorHash() {
        try (SeriesHandleRegistry registry = registryWithStepFactor(Duration.ofMinutes(15), 2, Clock.systemUTC())) {
            String stepYaml = yamlWithBaseStep(900);
            registry.open("series-a", stepYaml, Ngrrd.OpenOptions.defaults());
            registry.open("series-a", stepYaml, Ngrrd.OpenOptions.defaults());
            registry.open("series-b", stepYaml, Ngrrd.OpenOptions.defaults());
            registry.open("series-c", stepYaml, Ngrrd.OpenOptions.durability(Durability.FSYNC));
            assertEquals(1, registry.baseStepParseCount(), "mesmo hash: um parse só, inclusive com opções diferentes");

            registry.open("series-d", yaml, Ngrrd.OpenOptions.defaults());
            assertEquals(2, registry.baseStepParseCount(), "outro hash: mais um parse");
        }
    }

    @Test
    void contadorIdleTtlExtendedAcompanhaAberturaEFechamento() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registryWithStepFactor(Duration.ofMinutes(15), 2, clock)) {
            assertEquals(0L, registry.handleMetrics().get("handles.idleTtlExtended"));

            registry.open("series-passo", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());
            registry.open("series-passo", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());
            registry.open("series-curta", yaml, Ngrrd.OpenOptions.defaults());
            assertEquals(1L, registry.handleMetrics().get("handles.idleTtlExtended"),
                    "só a série cujo TTL efetivo supera o global conta, e uma só vez");

            registry.close("series-passo");
            assertEquals(0L, registry.handleMetrics().get("handles.idleTtlExtended"));
        }
    }

    @Test
    void reaberturaPelaAutocuraRecalculaOTtlEfetivoEOContador() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = registryWithStepFactor(Duration.ofMinutes(15), 2, clock)) {
            registry.open("series-passo", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofMinutes(31));
            assertEquals(1, registry.closeIdle());
            assertEquals(0L, registry.handleMetrics().get("handles.idleTtlExtended"));

            assertTrue(registry.reopenIfKnown("series-passo").isPresent());
            assertEquals(1L, registry.handleMetrics().get("handles.idleTtlExtended"));

            clock.advance(Duration.ofMinutes(16));
            assertEquals(0, registry.closeIdle(), "a série reaberta herda o TTL efetivo da definição");
            assertEquals(1, registry.openCount());
        }
    }

    @Test
    void despejoPorMaxOpenHandlesIgnoraOTtlEstendidoEDecrementaOContador() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (SeriesHandleRegistry registry = new SeriesHandleRegistry(volume, VOLUME_NAME, Duration.ofMinutes(15), 2, 1,
                clock)) {
            registry.open("series-1", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());
            clock.advance(Duration.ofSeconds(1));
            registry.open("series-2", yamlWithBaseStep(900), Ngrrd.OpenOptions.defaults());

            assertEquals(1, registry.openCount(), "o teto de maxOpenHandles continua valendo");
            assertEquals(1L, registry.handleMetrics().get("handles.idleTtlExtended"));
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
                        if (method.getName().equals("checkpointAsync") && failCheckpoint.getAndSet(false)) {
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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void openRecusadoPorCorteConcorrenteDeixaAEntradaParaOMarkMigrating() throws Exception {
        String key = "open-vs-cutover";
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
            recordWrite(registry, key);
            List<String> calls = recordHandleCalls(registry, key);
            ReentrantLock lock = entryLock(registry, key);
            AtomicReference<Throwable> openFailure = new AtomicReference<>();
            AtomicReference<Throwable> cutoverFailure = new AtomicReference<>();
            Thread opener = new Thread(() -> runCapturing(openFailure,
                    () -> registry.open(key, yaml, Ngrrd.OpenOptions.defaults())), "racing-open");
            Thread cutover = new Thread(() -> runCapturing(cutoverFailure,
                    () -> registry.markMigrating(key)), "racing-cutover");

            lock.lock();
            try {
                // O open() passa da checagem fora do lock (ainda sem migração) e para no lock da entrada.
                opener.start();
                awaitQueued(lock, opener);
                // O corte marca a migração, acha a entrada no mapa e para no mesmo lock, atrás do open().
                cutover.start();
                awaitQueued(lock, cutover);
                assertTrue(registry.isMigrating(key));
            } finally {
                // Threads já enfileiradas adquirem em ordem: o open() entra primeiro e vê a migração sob o lock.
                lock.unlock();
            }
            opener.join();
            cutover.join();

            assertMigratingRefusal(openFailure.get());
            assertNull(cutoverFailure.get(), "o corte não pode falhar");
            assertEquals(1, calls.stream().filter(call -> call.startsWith("close@")).count(),
                    "o handle tem que ser fechado exatamente uma vez: " + calls);
            assertTrue(calls.stream().allMatch(call -> call.endsWith("@racing-cutover")),
                    "só o markMigrating fecha e faz checkpoint do handle, nunca o open() recusado: " + calls);
            assertTrue(calls.contains("checkpointAsync@racing-cutover"),
                    "a escrita confirmada tem que chegar à imagem antes do corte: " + calls);
            assertEquals("close@racing-cutover", calls.get(calls.size() - 1), "checkpoint antes do close: " + calls);
            assertFalse(entries(registry).containsKey(key), "o markMigrating remove a entrada");
            assertEquals(0, registry.openCount());
            assertFalse(registry.dirtySeries().contains(key), "o markMigrating poda o índice de sujas");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void openRecusadoPorCopiaOnlineConcorrenteMantemOHandleSemAbrirOutro() throws Exception {
        String key = "open-vs-copy";
        try (SeriesHandleRegistry registry = trackedRegistry(Duration.ofMinutes(15), 10, Clock.systemUTC())) {
            registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
            List<String> calls = recordHandleCalls(registry, key);
            NgrrdHandle current = registry.existing(key).orElseThrow();
            ReentrantLock lock = entryLock(registry, key);
            AtomicReference<Throwable> openFailure = new AtomicReference<>();
            Thread opener = new Thread(() -> runCapturing(openFailure,
                    () -> registry.open(key, yaml, Ngrrd.OpenOptions.defaults())), "racing-open");

            lock.lock();
            try {
                opener.start();
                awaitQueued(lock, opener);
                registry.beginMigrationCopy(key);
            } finally {
                lock.unlock();
            }
            opener.join();

            assertMigratingRefusal(openFailure.get());
            assertTrue(calls.isEmpty(), "o open() recusado não fecha nem faz checkpoint do handle: " + calls);
            assertTrue(entries(registry).containsKey(key), "na cópia online a entrada segue no mapa até o corte");
            recordWrite(registry, key);
            assertSame(current, registry.reopenIfKnown(key).orElseThrow(),
                    "a reabertura tem que devolver o handle vivo, não abrir um segundo sobre o mesmo objeto");
            assertEquals(1, registry.openCount());

            registry.markMigrating(key);

            assertEquals(1, calls.stream().filter(call -> call.startsWith("close@")).count(),
                    "o corte fecha o handle exatamente uma vez: " + calls);
            assertFalse(entries(registry).containsKey(key));
            assertFalse(registry.dirtySeries().contains(key));
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
                if (method.equals("checkpointAsync") && failCheckpoint.getAndSet(false)) {
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

    // ------------------------------------------------------------------ writer abandonado e falhas de fechamento

    private static final Clock LATE_CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000_000L), ZoneOffset.UTC);

    /** Handle falso cujo {@code close()} lança o que {@code closeFailure} devolver (nada, se {@code null}). */
    private static NgrrdHandle closeFailingHandle(java.util.function.Supplier<RuntimeException> closeFailure) {
        return (NgrrdHandle) Proxy.newProxyInstance(NgrrdHandle.class.getClassLoader(),
                new Class<?>[]{NgrrdHandle.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        RuntimeException failure = closeFailure.get();
                        if (failure != null) {
                            throw failure;
                        }
                        return null;
                    }
                    return switch (method.getName()) {
                        case "checkpointAsyncTimed" ->
                                CompletableFuture.completedFuture(NgrrdHandle.CheckpointTiming.NONE);
                        case "checkpointAsync" -> CompletableFuture.completedFuture(null);
                        case "toString" -> "closing-handle";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> null;
                    };
                });
    }

    private static RuntimeException abandonedFailure() {
        return new WriterAbandonedException("force preso", new RuntimeException("cause"));
    }

    private static RuntimeException plainFailure() {
        return new IllegalStateException("close falhou");
    }

    private SeriesHandleRegistry lateRegistry(int maxOpenHandles) {
        SeriesHandleRegistry registry = registry(Duration.ofMinutes(1), maxOpenHandles, LATE_CLOCK);
        registry.trackDirtyIndex(true);
        return registry;
    }

    private static long abandonedCount(SeriesHandleRegistry registry) {
        return registry.handleMetrics().get("handles.writerAbandoned");
    }

    @Test
    void discardComWriterAbandonadoRemoveAEntradaConteELancaSemMarcarLimpa() {
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
            recordWrite(registry, "stuck");
            assertTrue(registry.dirtySeries().contains("stuck"));

            assertThrows(WriterAbandonedException.class, () -> registry.discard("stuck"));

            assertFalse(entries(registry).containsKey("stuck"), "a entrada abandonada tem que sair do mapa");
            assertFalse(registry.dirtySeries().contains("stuck"), "o índice de sujas é podado depois do unlock");
            assertEquals(1L, abandonedCount(registry));
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.UNAVAILABLE,
                    registry.tryCheckpointAsync("stuck").outcome(),
                    "checkpoint de série abandonada não pode reportar sucesso");
            assertFalse(registry.isOpen("stuck"));
        }
    }

    @Test
    void markMigratingComWriterAbandonadoRemoveAEntradaELanca() {
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));

            assertThrows(WriterAbandonedException.class, () -> registry.markMigrating("stuck"));

            assertFalse(entries(registry).containsKey("stuck"));
            assertTrue(registry.isMigrating("stuck"));
            assertEquals(1L, abandonedCount(registry));
        }
    }

    @Test
    void markMigratingComFalhaComumMantemAEntradaEPropaga() {
        AtomicBoolean fail = new AtomicBoolean(true);
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("flaky", newEntry(closeFailingHandle(
                    () -> fail.get() ? plainFailure() : null)));

            assertThrows(IllegalStateException.class, () -> registry.markMigrating("flaky"));

            assertTrue(entries(registry).containsKey("flaky"), "falha comum mantém o comportamento: a entrada fica");
            assertEquals(0L, abandonedCount(registry));
            fail.set(false);
        }
    }

    @Test
    void closeIdleRemoveAbandonadaESegueParaAsDemais() {
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("a-stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
            entries(registry).put("b-ok", newEntry(FAKE_HANDLE));

            assertEquals(1, registry.closeIdle(), "só o fechamento normal conta como ocioso fechado");

            assertTrue(entries(registry).isEmpty());
            assertEquals(1L, abandonedCount(registry));
            assertEquals(1L, registry.handleMetrics().get("handles.idleClosed"));
        }
    }

    @Test
    void closeIdleComFalhaComumNaoAbortaAVarreduraEMantemAEntrada() {
        AtomicBoolean fail = new AtomicBoolean(true);
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("a-flaky", newEntry(closeFailingHandle(
                    () -> fail.get() ? plainFailure() : null)));
            entries(registry).put("b-ok", newEntry(FAKE_HANDLE));

            assertEquals(1, registry.closeIdle());

            assertEquals(Set.of("a-flaky"), entries(registry).keySet());
            fail.set(false);
            assertEquals(1, registry.closeIdle(), "a próxima rodada tenta de novo e converge");
            assertTrue(entries(registry).isEmpty());
        }
    }

    @Test
    void openDeOutraSerieNaoFalhaPorCausaDoDespejoDeUmaSeriePresa() {
        AtomicBoolean fail = new AtomicBoolean(true);
        try (SeriesHandleRegistry registry = lateRegistry(1)) {
            entries(registry).put("stuck", newEntry(closeFailingHandle(
                    () -> fail.get() ? plainFailure() : null)));

            NgrrdHandle opened = registry.open("fresh", yaml, Ngrrd.OpenOptions.defaults());

            assertTrue(opened != null);
            assertTrue(entries(registry).containsKey("stuck"));
            assertTrue(registry.isOpen("fresh"),
                    "a série recém-aberta não pode ser despejada no lugar da que está presa");
            fail.set(false);
        }
    }

    @Test
    void despejoRemoveSerieAbandonadaDentroDoOpenDeOutra() {
        try (SeriesHandleRegistry registry = lateRegistry(1)) {
            entries(registry).put("stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));

            registry.open("fresh", yaml, Ngrrd.OpenOptions.defaults());

            assertFalse(entries(registry).containsKey("stuck"));
            assertTrue(registry.isOpen("fresh"));
            assertEquals(1L, abandonedCount(registry));
            assertEquals(0L, registry.handleMetrics().get("handles.evicted"));
        }
    }

    @Test
    void closeDoRegistryAgregaFalhasEFechaAsDemais() {
        SeriesHandleRegistry registry = lateRegistry(10);
        AtomicBoolean fail = new AtomicBoolean(true);
        NgrrdHandle failing = closeFailingHandle(() -> fail.get() ? plainFailure() : null);
        entries(registry).put("a-fail", newEntry(failing));
        entries(registry).put("b-abandoned",
                newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
        entries(registry).put("c-fail", newEntry(failing));
        AtomicInteger closedOk = new AtomicInteger();
        entries(registry).put("d-ok", newEntry(closeFailingHandle(() -> {
            closedOk.incrementAndGet();
            return null;
        })));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, registry::close);

        assertEquals(2, thrown.getSuppressed().length, "uma falha comum por série presa: " + thrown);
        assertEquals(1, closedOk.get(), "as demais séries continuam sendo fechadas");
        assertEquals(Set.of("a-fail", "c-fail"), entries(registry).keySet());
        assertEquals(1L, abandonedCount(registry));
        fail.set(false);
        registry.close();
        assertTrue(entries(registry).isEmpty());
    }

    @Test
    void writerAbandonadoLogaSevereComMarcador() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SeriesHandleRegistry.class.getName());
        List<java.util.logging.LogRecord> records = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) { records.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(capture);
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
            registry.closeIdle();
        } finally {
            logger.removeHandler(capture);
        }
        assertTrue(records.stream().anyMatch(record -> record.getLevel() == java.util.logging.Level.SEVERE
                        && record.getMessage().contains("NGRRD_WRITER_ABANDONED seriesKey=stuck")),
                "esperava o marcador SEVERE: " + records);
    }

    // ------------------------------------------------------------------ force preso com writer real

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeIdleComForcePresoTerminaERemoveAEntradaComoAbandonada(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            fixture.openStuck("stuck");
            List<java.util.logging.LogRecord> records = new CopyOnWriteArrayList<>();
            java.util.logging.Handler capture = capturing(records);
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SeriesHandleRegistry.class.getName());
            logger.addHandler(capture);
            try {
                fixture.clock.advance(Duration.ofMinutes(2));
                assertEquals(0, fixture.registry.closeIdle(), "o abandono não conta como fechamento por ociosidade");
            } finally {
                logger.removeHandler(capture);
            }

            assertFalse(entries(fixture.registry).containsKey("stuck"), "a entrada abandonada tem que sair do mapa");
            assertEquals(1L, abandonedCount(fixture.registry));
            assertEquals(0L, fixture.registry.handleMetrics().get("handles.idleClosed"));
            assertTrue(records.stream().anyMatch(record -> record.getLevel() == java.util.logging.Level.SEVERE
                            && record.getMessage().contains("NGRRD_WRITER_ABANDONED seriesKey=stuck")),
                    "esperava o marcador SEVERE: " + records);
            assertTrue(fixture.registry.consumeAbandonedCheckpoint("stuck"),
                    "o primeiro checkpoint remoto depois do abandono tem que ser recusado");
            assertFalse(fixture.registry.consumeAbandonedCheckpoint("stuck"), "só o primeiro");
        }
    }

    @Test
    void marcaDeAbandonoSaiNoPrimeiroConsumoENaDelecaoENaoAfetaOutrasSeries() {
        try (SeriesHandleRegistry registry = lateRegistry(10)) {
            entries(registry).put("a-stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
            entries(registry).put("b-stuck",
                    newEntry(closeFailingHandle(SeriesHandleRegistryTest::abandonedFailure)));
            entries(registry).put("c-ok", newEntry(FAKE_HANDLE));
            registry.closeIdle();
            assertEquals(2L, abandonedCount(registry));

            assertFalse(registry.consumeAbandonedCheckpoint("c-ok"), "fechamento normal não marca a série");
            registry.closeForDeletion("b-stuck");
            assertFalse(registry.consumeAbandonedCheckpoint("b-stuck"), "a deleção descarta a marca");
            assertTrue(registry.consumeAbandonedCheckpoint("a-stuck"));
            assertFalse(registry.consumeAbandonedCheckpoint("a-stuck"));
            assertEquals(1L, registry.handleMetrics().get("handles.abandonedCheckpointRejected"));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void discardComForcePresoTerminaELancaOAbandono(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            fixture.openStuck("stuck");

            assertThrows(WriterAbandonedException.class, () -> fixture.registry.discard("stuck"));

            assertFalse(entries(fixture.registry).containsKey("stuck"));
            assertFalse(fixture.registry.dirtySeries().contains("stuck"), "a poda roda depois do unlock");
            assertEquals(1L, abandonedCount(fixture.registry));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void markMigratingComForcePresoAbortaERemoveAEntradaAbandonada(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            fixture.openStuck("stuck");
            fixture.registry.beginMigrationCopy("stuck");

            assertThrows(WriterAbandonedException.class, () -> fixture.registry.markMigrating("stuck"),
                    "a migração tem que abortar");

            assertTrue(fixture.registry.isMigrating("stuck"));
            assertFalse(entries(fixture.registry).containsKey("stuck"));
            assertEquals(1L, abandonedCount(fixture.registry));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void migrationSnapshotComForcePresoFalhaEmTempoFinitoEMantemOHandle(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            fixture.openStuck("stuck");
            fixture.registry.beginMigrationCopy("stuck");

            assertThrows(IllegalStateException.class,
                    () -> fixture.registry.migrationSnapshot("stuck", () -> new byte[0]));

            assertTrue(entries(fixture.registry).containsKey("stuck"),
                    "o snapshot só falha: quem fecha a série presa é o caminho de fechamento");
            assertTrue(fixture.registry.isDirty("stuck"), "checkpoint não concluído não marca a série como limpa");
            fixture.registry.clearMigrating("stuck");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeDoRegistryComForcePresoTerminaEAbandonaTodasAsPresas(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            // Todo force deste volume fica preso: as duas séries são abandonadas, uma depois da outra.
            fixture.openStuck("stuck-a");
            fixture.openStuck("stuck-b");

            fixture.registry.close();

            assertTrue(entries(fixture.registry).isEmpty(), "o abandono da primeira não impede o fechamento da segunda");
            assertEquals(2L, abandonedCount(fixture.registry));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void openDeOutraSerieNaoTravaNoDespejoDeUmaSerieComForcePreso(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 1)) {
            fixture.openStuck("stuck");

            fixture.registry.open("fresh", yaml, Ngrrd.OpenOptions.defaults());

            assertFalse(entries(fixture.registry).containsKey("stuck"), "o despejo abandona a série presa");
            assertTrue(fixture.registry.isOpen("fresh"));
            assertEquals(1L, abandonedCount(fixture.registry));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void outraSerieSegueOperandoEnquantoOFechamentoDaPresaEspera(@TempDir Path dir) throws Exception {
        try (StuckForceFixture fixture = new StuckForceFixture(dir, 10)) {
            fixture.openStuck("stuck");
            ExecutorService closer = Executors.newSingleThreadExecutor();
            try {
                ReentrantLock stuckLock = entryLock(fixture.registry, "stuck");
                Future<?> discard = closer.submit(() -> {
                    try {
                        fixture.registry.discard("stuck");
                    } catch (WriterAbandonedException expected) {
                        // desfecho esperado: o writer da série presa é abandonado
                    }
                });
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!stuckLock.isLocked() && !discard.isDone()) {
                    assertTrue(System.nanoTime() < deadline, "o discard não chegou ao lock da série presa");
                    Thread.sleep(1);
                }

                fixture.registry.open("other", yaml, Ngrrd.OpenOptions.defaults());
                assertTrue(fixture.registry.withHandle("other", handle -> {
                    handle.write("in_octets", new Sample(1_700_000_100_000L, 1_000d));
                    return true;
                }).orElse(false), "outra série tem que seguir gravável enquanto a presa fecha");

                discard.get(20, TimeUnit.SECONDS);
                assertFalse(entries(fixture.registry).containsKey("stuck"));
                assertTrue(fixture.registry.isOpen("other"));
            } finally {
                closer.shutdownNow();
            }
        }
    }

    private static java.util.logging.Handler capturing(List<java.util.logging.LogRecord> records) {
        return new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) { records.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
    }

    /**
     * Volume real cujo executor de force prende toda fase B até o fim do teste: um checkpoint com amostra nova
     * destaca o force, que nunca termina. O registro usa espera curta pelo checkpoint de fechamento e o writer de
     * cada série presa usa teto curto no Shutdown ({@link #shortenWriterShutdownWait}), para o abandono sair em
     * frações de segundo em vez dos 20 s padrão.
     */
    private final class StuckForceFixture implements AutoCloseable {
        private final CountDownLatch gate = new CountDownLatch(1);
        private final ExecutorService forcePool = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "stuck-force");
            thread.setDaemon(true);
            return thread;
        });
        private final BlobVolumeRegistry volumes;
        private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        private final SeriesHandleRegistry registry;

        StuckForceFixture(Path dir, int maxOpenHandles) {
            java.util.concurrent.Executor gated = task -> forcePool.execute(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            });
            volumes = NgrrdBlob.registry().basePath(dir.resolve("stuck-force")).volume(VOLUME_NAME)
                    .checkpointForceExecutor(gated).build();
            registry = new SeriesHandleRegistry(volumes.require(VOLUME_NAME), VOLUME_NAME, Duration.ofMinutes(1),
                    maxOpenHandles, clock);
            registry.trackDirtyIndex(true);
            registry.closeCheckpointWaitForTests(Duration.ofMillis(200));
        }

        /** Abre a série, escreve e enfileira um checkpoint cujo force destacado fica preso no portão. */
        void openStuck(String key) {
            registry.open(key, yaml, Ngrrd.OpenOptions.defaults());
            assertTrue(registry.withSeries(key, access -> {
                access.handle().write("in_octets", new Sample(1_700_000_100_000L, 1_000d));
                access.recordWrite();
                return true;
            }).orElse(false));
            shortenWriterShutdownWait(registry, key, Duration.ofMillis(300));
            SeriesHandleRegistry.CheckpointAttempt attempt = registry.tryCheckpointAsync(key);
            assertEquals(SeriesHandleRegistry.CheckpointAttempt.Outcome.STARTED, attempt.outcome());
        }

        @Override
        public void close() throws InterruptedException {
            gate.countDown();
            try {
                registry.close();
            } catch (RuntimeException ignored) {
                // o registro pode já estar fechado pelo teste, ou guardar falhas que o teste já conferiu
            }
            forcePool.shutdown();
            assertTrue(forcePool.awaitTermination(10, TimeUnit.SECONDS), "os forces liberados têm que terminar");
            volumes.close();
        }
    }

    /**
     * Reduz o teto de espera do Shutdown do writer real da série pelos forces em voo (20 s no oss). O método é
     * package-private do pacote {@code writer}, só para testes: acessado por reflexão a partir do handle aberto.
     */
    private static void shortenWriterShutdownWait(SeriesHandleRegistry registry, String key, Duration wait) {
        try {
            NgrrdHandle handle = registry.existing(key).orElseThrow();
            Field writerField = handle.getClass().getDeclaredField("writer");
            writerField.setAccessible(true);
            Object writer = writerField.get(handle);
            java.lang.reflect.Method shorten = writer.getClass().getDeclaredMethod("shutdownForceWaitForTests",
                    long.class);
            shorten.setAccessible(true);
            shorten.invoke(writer, wait.toNanos());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
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

    /**
     * Envolve o handle da série num proxy que registra cada {@code checkpointAsync}/{@code close} como
     * {@code "método@thread"}, na ordem das chamadas.
     */
    private static List<String> recordHandleCalls(SeriesHandleRegistry registry, String key) {
        List<String> calls = new CopyOnWriteArrayList<>();
        wrapHandle(registry, key, original -> delegating(original, method -> {
            if (method.equals("checkpointAsync") || method.equals("close")) {
                calls.add(method + "@" + Thread.currentThread().getName());
            }
            return null;
        }));
        return calls;
    }

    /** Lock da entrada corrente da série: segurá-lo enfileira as threads de uma corrida numa ordem conhecida. */
    private static ReentrantLock entryLock(SeriesHandleRegistry registry, String key) {
        try {
            Object entry = entries(registry).get(key);
            Field lockField = entry.getClass().getDeclaredField("lock");
            lockField.setAccessible(true);
            return (ReentrantLock) lockField.get(entry);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Espera {@code thread} ficar enfileirada no {@code lock}, sem depender de tempo fixo. */
    private static void awaitQueued(ReentrantLock lock, Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!lock.hasQueuedThread(thread)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("a thread " + thread.getName() + " não chegou ao lock da entrada");
            }
            Thread.sleep(1);
        }
    }

    private static void runCapturing(AtomicReference<Throwable> failure, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static void assertMigratingRefusal(Throwable failure) {
        assertTrue(failure instanceof IllegalStateException && "MIGRATING".equals(failure.getMessage()),
                "o open() concorrente tem que ser recusado com MIGRATING: " + failure);
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
