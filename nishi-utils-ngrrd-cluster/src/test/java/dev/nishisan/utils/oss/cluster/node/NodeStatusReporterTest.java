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

import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre {@link NodeStatusReporter#metricsSnapshot()} sobre um {@link BlobVolume} REAL em
 * {@code @TempDir} (não um fake) — é justamente {@code volume.stats()} que se quer ver refletido
 * corretamente no {@link NodeMetricsSnapshot} — e a notificação periódica de
 * {@link dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener#onNodeMetrics}.
 */
class NodeStatusReporterTest {

    private static final String VOLUME_NAME = "ngrrd";

    private NGridCluster cluster;
    private BlobVolumeRegistry volumeRegistry;
    private BlobVolume volume;
    private SeriesHandleRegistry registry;
    private String yaml;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        volumeRegistry = NgrrdBlob.registry().basePath(tempDir).volume(VOLUME_NAME).build();
        volume = volumeRegistry.require(VOLUME_NAME);
        registry = new SeriesHandleRegistry(volume, VOLUME_NAME, Duration.ofMinutes(15), 10_000, Clock.systemUTC());
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
    }

    @AfterEach
    void tearDown() throws Exception {
        registry.close();
        volumeRegistry.close();
        cluster.close();
    }

    private NodeStatusReporter newReporter(Clock clock, NgrrdClusterMetricsListener listener, Duration interval) {
        NGridNode node = cluster.node(0);
        CatalogService catalog = CatalogService.from(node);
        StorageRequestHandler.StorageHandlerMetrics handlerMetrics = new StorageRequestHandler.StorageHandlerMetrics(
                7L, 42L, 1L, 3L, 2L, 1L, Map.of(SeriesStatus.ERROR, 1L), LatencySnapshot.EMPTY, LatencySnapshot.EMPTY,
                LatencySnapshot.EMPTY);
        return new NodeStatusReporter(catalog, volume, registry, "storage-real", 1_000_000L, interval, clock,
                () -> handlerMetrics, () -> true, listener);
    }

    @Test
    void metricsSnapshotReflecteVolumeRegistryELiderancaReais() {
        NgrrdHandle handle = registry.open("series-1", yaml, Ngrrd.OpenOptions.defaults());
        handle.write("in_octets", new Sample(1_700_000_000_000L, 1_000d));
        handle.checkpoint();

        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        NodeStatusReporter reporter = newReporter(clock, null, Duration.ofSeconds(10));

        NodeMetricsSnapshot snapshot = reporter.metricsSnapshot();

        assertEquals("storage-real", snapshot.nodeId());
        assertEquals(clock.millis(), snapshot.capturedAtEpochMs());
        assertTrue(snapshot.leader());
        assertEquals(1L, snapshot.seriesCount(), "catalogEntryCount real do BlobVolume após 1 série aberta");
        assertEquals(1L, snapshot.openHandles());
        assertEquals(1_000_000L, snapshot.capacityBytes());
        // Passa through direto de StorageHandlerMetrics.
        assertEquals(7L, snapshot.writeBatches());
        assertEquals(42L, snapshot.samplesWritten());
        assertEquals(1L, snapshot.samplesFailed());
        assertEquals(3L, snapshot.reads());
        assertEquals(2L, snapshot.checkpoints());
        assertEquals(1L, snapshot.flushes());
        assertEquals(1L, snapshot.errorsByStatus().get(SeriesStatus.ERROR));
        assertEquals(0L, snapshot.migrationsIn());
        assertEquals(0L, snapshot.migrationsOut());

        BlobVolumeSummary blobStats = snapshot.blobStats();
        assertEquals(1, blobStats.catalogEntryCount());
        assertTrue(blobStats.usedBytes() > 0, "série com amostra escrita deveria ocupar bytes reais no volume");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void tickPeriodicoNotificaOListenerDeMetricas() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<NodeMetricsSnapshot> lastSnapshot = new AtomicReference<>();
        NgrrdClusterMetricsListener listener = new NgrrdClusterMetricsListener() {
            @Override
            public void onNodeMetrics(NodeMetricsSnapshot snapshot) {
                lastSnapshot.set(snapshot);
                received.countDown();
            }
        };
        NodeStatusReporter reporter = newReporter(Clock.systemUTC(), listener, Duration.ofMillis(50));
        try {
            reporter.start();
            assertTrue(received.await(5, TimeUnit.SECONDS), "listener não foi notificado a tempo");
            assertEquals("storage-real", lastSnapshot.get().nodeId());
        } finally {
            reporter.close();
        }
    }

    // ---------------------------------------------------------------- MÉDIO-3: leitura forte antes de publicar

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publicaDrainingSegundoALeituraForteMesmoSemReplicaLocalAlgumaAindaConhecida() throws InterruptedException {
        CatalogViewFake catalog = new CatalogViewFake();
        // "Líder diz DRAINING": é isto que nodeStatusStrong devolve — o relatório nunca consulta outra
        // fonte (a antiga réplica "local" nem existe mais neste desenho; ver Javadoc de #report()).
        catalog.nodeStatuses.put("storage-real", new StorageNodeStatus("storage-real", NodeState.DRAINING, 3, 0, 0, 500L));
        NodeStatusReporter reporter = reporterWithFakeCatalog(catalog, Duration.ofMillis(30));
        try {
            reporter.start();
            awaitTrue("status DRAINING deveria ter sido publicado", () -> !catalog.published.isEmpty()
                    && catalog.published.get(catalog.published.size() - 1).state() == NodeState.DRAINING);
        } finally {
            reporter.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void primeiroTickAposRestartPublicaDrainedSegundoOLider() throws InterruptedException {
        CatalogViewFake catalog = new CatalogViewFake();
        // Simula um restart: nenhuma réplica local própria ainda, mas o líder (leitura forte) já sabe
        // que este nó está DRAINED (persistido de antes do restart).
        catalog.nodeStatuses.put("storage-real", new StorageNodeStatus("storage-real", NodeState.DRAINED, 0, 0, 0, 500L));
        NodeStatusReporter reporter = reporterWithFakeCatalog(catalog, Duration.ofMillis(30));
        try {
            reporter.start();
            awaitTrue("status DRAINED deveria ter sido publicado logo no primeiro tick", () -> !catalog.published.isEmpty()
                    && catalog.published.get(catalog.published.size() - 1).state() == NodeState.DRAINED);
        } finally {
            reporter.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void semLiderNaoPublicaNadaNesteTick() throws InterruptedException {
        CatalogViewFake catalog = new CatalogViewFake();
        catalog.nodeStatusStrongThrows = true;
        NodeStatusReporter reporter = reporterWithFakeCatalog(catalog, Duration.ofMillis(20));
        try {
            reporter.start();
            // Espera tempo suficiente para vários ticks terem rodado — nenhum deveria ter publicado.
            Thread.sleep(300L);
            assertTrue(catalog.published.isEmpty(), "sem líder, nenhum tick deveria publicar: " + catalog.published);
        } finally {
            reporter.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void leituraForteFalhaNaPrimeiraTentativaEPublicaNaSegunda() throws InterruptedException {
        // MÉDIO-B do Refuter: a falha agora PROPAGA de report() para reportWithRetry, que aciona o
        // backoff (200ms na 1ª retentativa) — um intervalo de tick bem mais longo garante que a
        // publicação observada só pode ter vindo da retentativa, não de um novo tick regular.
        CatalogViewFake catalog = new CatalogViewFake();
        catalog.nodeStatuses.put("storage-real", new StorageNodeStatus("storage-real", NodeState.ACTIVE, 1, 0, 0, 500L));
        catalog.nodeStatusStrongFailuresRemaining.set(1);
        NodeStatusReporter reporter = reporterWithFakeCatalog(catalog, Duration.ofSeconds(30));
        try {
            reporter.start();
            awaitTrue("status deveria ter sido publicado após a retentativa", () -> !catalog.published.isEmpty());
            assertEquals(NodeState.ACTIVE, catalog.published.get(0).state());
        } finally {
            reporter.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void schedulerDeManutencaoContinuaTicandoEnquantoPublicacaoBloqueada() throws InterruptedException {
        // MÉDIO-B do Refuter: publicação isolada no seu próprio executor — mesmo com catalog.putNodeStatus
        // preso (ver Javadoc de NodeStatusReporter: pode bloquear até 5×requestTimeout sem líder), o
        // scheduler de manutenção continua chamando publishMetrics()/registry.closeIdle() no seu ritmo.
        CatalogViewFake catalog = new CatalogViewFake();
        catalog.nodeStatuses.put("storage-real", new StorageNodeStatus("storage-real", NodeState.ACTIVE, 1, 0, 0, 500L));
        CountDownLatch releasePut = new CountDownLatch(1);
        catalog.putBlocksUntil = releasePut;
        AtomicInteger metricsTicks = new AtomicInteger();
        NgrrdClusterMetricsListener listener = new NgrrdClusterMetricsListener() {
            @Override
            public void onNodeMetrics(NodeMetricsSnapshot snapshot) {
                metricsTicks.incrementAndGet();
            }
        };
        NodeStatusReporter reporter = reporterWithFakeCatalog(catalog, Duration.ofMillis(30), listener);
        try {
            reporter.start();
            awaitTrue("o scheduler de manutenção deveria continuar tickando mesmo com o put bloqueado",
                    () -> metricsTicks.get() >= 3);
            assertTrue(catalog.published.isEmpty(), "put ainda bloqueado — nada deveria ter sido publicado ainda");
        } finally {
            releasePut.countDown();
            reporter.close();
        }
    }

    private NodeStatusReporter reporterWithFakeCatalog(CatalogView catalog, Duration interval) {
        return reporterWithFakeCatalog(catalog, interval, null);
    }

    private NodeStatusReporter reporterWithFakeCatalog(CatalogView catalog, Duration interval,
            NgrrdClusterMetricsListener listener) {
        StorageRequestHandler.StorageHandlerMetrics handlerMetrics = new StorageRequestHandler.StorageHandlerMetrics(
                0L, 0L, 0L, 0L, 0L, 0L, Map.of(), LatencySnapshot.EMPTY, LatencySnapshot.EMPTY, LatencySnapshot.EMPTY);
        return new NodeStatusReporter(catalog, volume, registry, "storage-real", 1_000_000L, interval,
                Clock.systemUTC(), () -> handlerMetrics, () -> true, listener);
    }

    private static void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo: " + description);
        }
    }

    /** {@link CatalogView} fake: controla a leitura FORTE do estado do próprio nó e grava o que é publicado. */
    private static final class CatalogViewFake implements CatalogView {
        final Map<String, StorageNodeStatus> nodeStatuses = new ConcurrentHashMap<>();
        final List<StorageNodeStatus> published = new CopyOnWriteArrayList<>();
        volatile boolean nodeStatusStrongThrows;
        /** MÉDIO-B: falha as N primeiras chamadas de {@link #nodeStatusStrong}, depois passa a funcionar. */
        final AtomicInteger nodeStatusStrongFailuresRemaining = new AtomicInteger(0);
        /** MÉDIO-B: {@link #putNodeStatus} bloqueia até este latch contar — simula um put preso sem líder. */
        volatile CountDownLatch putBlocksUntil;

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.empty();
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            if (nodeStatusStrongThrows) {
                throw new IllegalStateException("sem líder eleito (simulado)");
            }
            if (nodeStatusStrongFailuresRemaining.get() > 0) {
                nodeStatusStrongFailuresRemaining.decrementAndGet();
                throw new IllegalStateException("leitura forte falhou (simulado, transitório)");
            }
            return Optional.ofNullable(nodeStatuses.get(nodeId));
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodeStatuses.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.of();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado por NodeStatusReporter");
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            CountDownLatch latch = putBlocksUntil;
            if (latch != null) {
                try {
                    latch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            published.add(status);
        }
    }

    /** {@link Clock} determinístico, mesmo padrão dos demais testes do módulo. */
    private static final class MutableClock extends Clock {
        private final Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
