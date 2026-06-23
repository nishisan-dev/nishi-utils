package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O listener de qualidade default do volume é propagado a cada handle aberto via
 * {@link Ngrrd#open}, permitindo coleta central com atribuição por {@code seriesKey}
 * — sem fiar um listener por série.
 */
class QualityPropagationTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;

    private static String blobYaml() throws Exception {
        try (InputStream in = QualityPropagationTest.class.getResourceAsStream("/iface-traffic-blob.yaml")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void qualityListenerDoVolumePropagaParaHandlesComSeriesKey(@TempDir Path base) throws Exception {
        List<String> resets = new CopyOnWriteArrayList<>(); // seriesKeys que sofreram counter reset
        NgrrdMetricsListener spy = new NgrrdMetricsListener() {
            @Override
            public void onCounterReset(String seriesKey, String dsName) {
                resets.add(seriesKey);
            }
        };
        String yaml = blobYaml();
        try (BlobVolumeRegistry registry = NgrrdBlob.registry()
                .basePath(base).shardCount(4).segmentBytes(1L << 20)
                .qualityListener(spy)
                .volume("ifaceStats")
                .build()) {
            triggerReset(registry, "device:r1/iface:eth0", yaml);
            triggerReset(registry, "device:r2/iface:eth1", yaml);
        }
        // coleta central, com a série de origem distinguível pelo seriesKey
        assertEquals(2, resets.size(), () -> "resets observados: " + resets);
        assertTrue(resets.contains("device:r1/iface:eth0"));
        assertTrue(resets.contains("device:r2/iface:eth1"));
    }

    private static void triggerReset(BlobVolumeRegistry registry, String seriesPath, String yaml) {
        try (NgrrdHandle h = Ngrrd.open(registry,
                NgrrdUri.parse("ngrrd://ifaceStats/" + seriesPath), yaml)) {
            h.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            h.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 2_000_000L));
            h.write("in_octets", new Sample(BLOCK_START_MS + 2L * STEP_MS, 100L)); // reset grande
            h.flush();
        }
    }
}
