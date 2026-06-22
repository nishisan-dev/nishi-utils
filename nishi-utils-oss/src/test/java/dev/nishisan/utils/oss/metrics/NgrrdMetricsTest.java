package dev.nishisan.utils.oss.metrics;

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
