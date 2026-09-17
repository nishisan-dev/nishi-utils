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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
