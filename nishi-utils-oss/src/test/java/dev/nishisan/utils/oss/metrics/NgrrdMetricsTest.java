package dev.nishisan.utils.oss.metrics;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.writer.NgrrdWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NgrrdMetricsTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;

    private NgrrdDefinition loadDefinition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-errors-v1.yaml")) {
            return NgrrdYamlLoader.load(in, k -> null);
        }
    }

    @Test
    void counterResetIncrementaContador(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        NgrrdMetrics metrics = new NgrrdMetrics();
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", metrics)) {
            writer.write("in_octets", new Sample(BLOCK_START_MS + 0L * STEP_MS, 1_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + 1L * STEP_MS, 2_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + 2L * STEP_MS, 100L)); // RESET grande
            writer.flush();
        }
        assertEquals(1, metrics.counterResetCount());
        assertEquals(0, metrics.wrapDetectedCount());
    }

    @Test
    void wrapDetectadoIncrementaContador(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        NgrrdMetrics metrics = new NgrrdMetrics();
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", metrics)) {
            // in_errors é COUNTER de 32 bits — wrap em 2^32 = 4_294_967_296.
            long max32 = 1L << 32;
            writer.write("in_errors", new Sample(BLOCK_START_MS, max32 - 100));
            writer.write("in_errors", new Sample(BLOCK_START_MS + STEP_MS, 50)); // overflow plausível
            writer.flush();
        }
        assertEquals(1, metrics.wrapDetectedCount());
        assertEquals(0, metrics.counterResetCount());
    }

    @Test
    void blockClosedRegistraMissingRatio(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        NgrrdMetrics metrics = new NgrrdMetrics();
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", metrics)) {
            writer.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 2_000_000L));
            writer.flush();
        }
        // Block tem 72 slots de 5min em 6h; só 2 amostras = 1 PDP válido após derivação,
        // logo missingRatio >> 0.
        assertTrue(metrics.lastMissingRatio() > 0.5,
                "missingRatio esperado alto, obteve " + metrics.lastMissingRatio());
    }

    @Test
    void lateSampleIncrementaContador(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        NgrrdMetrics metrics = new NgrrdMetrics();
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", metrics)) {
            // Primeiro: amostra futura (após blockEnd) força rollover.
            writer.write("in_octets", new Sample(BLOCK_START_MS + 6L * 3600L * 1000L, 1_000_000L));
            // Agora amostra anterior ao novo bloco — é late.
            writer.write("in_octets", new Sample(BLOCK_START_MS, 999_000L));
            writer.flush();
        }
        assertEquals(1, metrics.lateSampleCount());
    }

    @Test
    void emiteIngestLagApenasParaLagPositivo(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        List<Long> lags = new CopyOnWriteArrayList<>();
        List<String> keys = new CopyOnWriteArrayList<>();
        NgrrdMetricsListener listener = new NgrrdMetricsListener() {
            @Override
            public void onIngestLag(String seriesKey, String dsName, long lagSec) {
                keys.add(seriesKey);
                lags.add(lagSec);
            }
        };
        long now = BLOCK_START_MS + 10_000L;
        LongSupplier clock = () -> now;
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", listener,
                new ReentrantReadWriteLock(), Durability.FSYNC, OnGeometryChange.FAIL, clock)) {
            writer.write("in_octets", new Sample(BLOCK_START_MS + 5_000L, 1_000_000L)); // lag = 5s
            writer.flush();
            writer.write("in_octets", new Sample(now + 5_000L, 2_000_000L));            // futura: sem emit
            writer.flush();
        }
        assertEquals(List.of(5L), lags);
        assertEquals(List.of("device:r1/iface:eth0"), keys);
    }

    @Test
    void checkpointOciosoIncrementaCoalesced(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        List<String> keys = new CopyOnWriteArrayList<>();
        NgrrdMetrics metrics = new NgrrdMetrics(new NgrrdMetricsListener() {
            @Override
            public void onCheckpointCoalesced(String seriesKey) {
                keys.add(seriesKey);
            }
        });
        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0", metrics)) {
            writer.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 2_000_000L));
            writer.flush(); // dado novo desde a criação: força, não coalesce
            assertEquals(0, metrics.checkpointCoalescedCount());

            writer.flush(); // ocioso: nenhuma amostra nova -> coalescido
            writer.flush(); // ocioso: coalescido de novo
            assertEquals(2, metrics.checkpointCoalescedCount());
            assertEquals(2, keys.size());
            assertTrue(keys.stream().allMatch("device:r1/iface:eth0"::equals),
                    "coalescing deve propagar o seriesKey: " + keys);
        }
        // Nota: o close() dispara um Command.Shutdown que, estando ocioso, também
        // é coalescido (contador sobe para 3) — comportamento esperado.
    }

    @Test
    void formaCanonicaPropagaSeriesKeyAoForward() {
        List<String> seriesKeys = new ArrayList<>();
        NgrrdMetricsListener forward = new NgrrdMetricsListener() {
            @Override
            public void onCounterReset(String seriesKey, String dsName) {
                seriesKeys.add(seriesKey);
            }
        };
        NgrrdMetrics metrics = new NgrrdMetrics(forward);
        metrics.onCounterReset("device:r1/iface:eth0", "in_octets");
        assertEquals(1, metrics.counterResetCount());
        assertEquals(List.of("device:r1/iface:eth0"), seriesKeys);
    }

    @Test
    void listenerLegadoRecebeEventosViaDelegacaoDaFormaCanonica() {
        // Um listener que só conhece a forma antiga (sem seriesKey) continua
        // recebendo eventos quando o produtor chama a forma canônica.
        long[] resets = {0};
        NgrrdMetricsListener legacy = new NgrrdMetricsListener() {
            @Override
            public void onCounterReset(String dsName) {
                resets[0]++;
            }
        };
        legacy.onCounterReset("series-x", "in_octets"); // canônico -> delega ao legado
        assertEquals(1, resets[0]);
    }

    @Test
    void listenerEncadeiaEventosParaForward() {
        long[] resets = {0};
        long[] wraps = {0};
        NgrrdMetricsListener forward = new NgrrdMetricsListener() {
            @Override
            public void onCounterReset(String dsName) {
                resets[0]++;
            }

            @Override
            public void onWrapDetected(String dsName) {
                wraps[0]++;
            }
        };
        NgrrdMetrics metrics = new NgrrdMetrics(forward);
        metrics.onCounterReset("in_octets");
        metrics.onWrapDetected("in_errors");
        assertEquals(1, metrics.counterResetCount());
        assertEquals(1, metrics.wrapDetectedCount());
        assertEquals(1, resets[0]);
        assertEquals(1, wraps[0]);
    }
}
