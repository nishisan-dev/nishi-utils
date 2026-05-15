package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.format.NgrrdManifest;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.reader.ViewExecutor;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NgrrdWriterE2ETest {

    private static final long BLOCK_START_SEC = 1_747_339_200L; // 2025-05-15 16:00 UTC (alinhado)
    private static final long BLOCK_START_MS = BLOCK_START_SEC * 1000L;
    private static final int STEP_MS = 300_000;

    private NgrrdDefinition loadDefinition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-errors-v1.yaml")) {
            return NgrrdYamlLoader.load(in, k -> null);
        }
    }

    private Map<String, String> sampleTags() {
        return Map.of(
                "deviceId", "r1",
                "interfaceId", "eth0",
                "region", "br-sp",
                "vendor", "x",
                "role", "core");
    }

    @Test
    void ingestDeUmaHoraGeraBlocoFechadoEDailyPresetRecuperaPontos(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        String seriesKey = "device:r1/iface:eth0";

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey)) {
            long octets = 0L;
            for (int i = 0; i < 12; i++) {
                octets += 100_000L; // counter crescente
                writer.write("in_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
                writer.write("out_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
            }
            writer.flush();

            assertFalse(writer.persistedBlocks().isEmpty(),
                    "Bloco devia ter sido persistido após flush");
            ManifestUpdater updater = new ManifestUpdater(writer, storage, "abc123", 60);
            NgrrdManifest snapshot = updater.writeSnapshot();
            assertEquals(1, snapshot.version());
        }

        long endMs = BLOCK_START_MS + 12 * STEP_MS + 1000L;
        ViewExecutor viewer = new ViewExecutor(def, storage);
        Map<String, SeriesResult> result = viewer.run("daily", sampleTags(), endMs);

        SeriesResult inBps = result.get("in_bps");
        assertNotNull(inBps);
        assertEquals("rra_5m_30d", inBps.rraName());
        assertEquals(ConsolidationFunction.AVERAGE, inBps.cf());
        // 11 pontos com valor (a 1ª amostra é FIRST_SAMPLE → derivedNaN).
        long nonNaN = inBps.points().stream().filter(p -> !Double.isNaN(p.value())).count();
        assertTrue(nonNaN >= 10, "esperava pelo menos 10 pontos não-NaN, obteve " + nonNaN);
        // 100k bytes / 300 s * 8 = 2666.67 bit/s
        double firstValid = inBps.points().stream()
                .filter(p -> !Double.isNaN(p.value()))
                .findFirst()
                .orElseThrow()
                .value();
        assertEquals(8 * 100_000.0 / 300.0, firstValid, 1e-6);
    }

    @Test
    void counterResetProduzPontoNaNNoBpsDerivado(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        String seriesKey = "device:r1/iface:eth0";

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey)) {
            // 4 amostras crescentes
            writer.write("in_octets", new Sample(BLOCK_START_MS + 0L * STEP_MS, 1_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + 1L * STEP_MS, 2_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + 2L * STEP_MS, 3_000_000L));
            // RESET grande: queda de 3M para 100
            writer.write("in_octets", new Sample(BLOCK_START_MS + 3L * STEP_MS, 100L));
            // depois volta a crescer
            writer.write("in_octets", new Sample(BLOCK_START_MS + 4L * STEP_MS, 200_000L));
            writer.flush();
            new ManifestUpdater(writer, storage, "abc123", 60).writeSnapshot();
        }

        NgrrdReader reader = new NgrrdReader(def, storage, seriesKey);
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        long endMs = BLOCK_START_MS + 5L * STEP_MS;
        SeriesResult result = reader.read("in_bps", query, endMs);

        List<Double> values = result.points().stream().map(p -> p.value()).toList();
        // Esperamos: slot1 OK, slot2 OK, slot3 NaN (reset → onReset=unknown=NaN), slot4 OK.
        long nanCount = values.stream().filter(v -> Double.isNaN(v)).count();
        assertTrue(nanCount >= 1, "esperava ao menos 1 ponto NaN no reset, obteve valores=" + values);
    }

    @Test
    void manifestUpdaterDescobreProximaVersaoAposRestart(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = loadDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        String seriesKey = "device:r1/iface:eth0";

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey)) {
            writer.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            writer.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 2_000_000L));
            writer.flush();
            new ManifestUpdater(writer, storage, "h1", 60).writeSnapshot(); // v1
            new ManifestUpdater(writer, storage, "h1", 60).writeSnapshot(); // v2
        }

        // simula restart: novo writer + novo updater devem começar em v3.
        try (NgrrdWriter writer2 = new NgrrdWriter(def, storage, seriesKey)) {
            writer2.write("in_octets", new Sample(BLOCK_START_MS + 2L * STEP_MS, 3_000_000L));
            writer2.flush();
            ManifestUpdater updater = new ManifestUpdater(writer2, storage, "h1", 60);
            NgrrdManifest m = updater.writeSnapshot();
            assertEquals(3, m.version(), "manifesto após restart deveria começar em v3");
        }

        NgrrdReader reader = new NgrrdReader(def, storage, seriesKey);
        Optional<NgrrdManifest> latest = reader.loadLatestManifest();
        assertTrue(latest.isPresent());
        assertEquals(3, latest.get().version());
    }
}
