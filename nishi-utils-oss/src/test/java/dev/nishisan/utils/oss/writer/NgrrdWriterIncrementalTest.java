package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdDefinitionValidator;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a persistência incremental (rrdtool-like) sobre o objeto único NGRR: a
 * reabertura reidrata o {@code counterPrev} e os acumuladores; o
 * {@code checkpoint()} torna o CDP em progresso legível antes do passo do ring
 * fechar; e não há perda de dados ao reabrir o handle no meio de uma janela.
 */
class NgrrdWriterIncrementalTest {

    private static final long START_SEC = 1_747_339_200L; // alinhado a 6h
    private static final long START_MS = START_SEC * 1000L;
    private static final int STEP_MS = 300_000;
    private static final double EXPECTED_BPS = 8 * 100_000.0 / 300.0; // 2666.67 bit/s
    private static final String SERIES_KEY = "device:r1/iface:eth0";

    private NgrrdDefinition definition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            NgrrdDefinition def = NgrrdYamlLoader.parse(
                    new String(in.readAllBytes(), StandardCharsets.UTF_8), k -> null);
            NgrrdDefinitionValidator.validate(def);
            return def;
        }
    }

    private List<Double> readInBps(NgrrdDefinition def, NgrrdStorage storage, long endMs) {
        NgrrdReader reader = new NgrrdReader(def, storage, SERIES_KEY);
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        SeriesResult result = reader.read("in_bps", query, endMs);
        return result.points().stream().map(p -> p.value())
                .filter(v -> !Double.isNaN(v)).toList();
    }

    @Test
    void reaberturaReidrataCounterPrevEDerivaNaPrimeiraAmostra(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        // Writer A: 1ª amostra (FIRST_SAMPLE → NaN) mas persiste o counterPrev.
        try (NgrrdWriter a = new NgrrdWriter(def, storage, SERIES_KEY)) {
            a.write("in_octets", new Sample(START_MS, 1_000_000L));
            a.checkpoint();
        }

        // Writer B: reabre, reidrata o counterPrev → 2ª amostra deriva (não NaN).
        try (NgrrdWriter b = new NgrrdWriter(def, storage, SERIES_KEY)) {
            b.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            b.checkpoint();
        }

        List<Double> values = readInBps(def, storage, START_MS + 2L * STEP_MS);
        assertTrue(values.size() >= 1, "esperava ao menos 1 ponto derivado após a reabertura: " + values);
        assertEquals(EXPECTED_BPS, values.getFirst(), 1e-6);
    }

    @Test
    void checkpointTornaCdpLegivelAntesDoProximoPasso(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter w = new NgrrdWriter(def, storage, SERIES_KEY)) {
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            w.checkpoint(); // sem cruzar para o próximo passo do RRA

            assertTrue(temArquivoNgrr(tempDir), "esperava 1 objeto .ngrr em disco após checkpoint");
            // O CDP em progresso (parcial) já deve estar legível.
            List<Double> values = readInBps(def, storage, START_MS + 2L * STEP_MS);
            assertTrue(values.contains(EXPECTED_BPS),
                    "checkpoint deveria tornar o CDP parcial legível: " + values);
        }
    }

    @Test
    void naoPerdeDadosAoReabrirNoMeioDaJanela(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        // Writer A: preenche até o slot1 (a 1ª amostra é FIRST_SAMPLE).
        try (NgrrdWriter a = new NgrrdWriter(def, storage, SERIES_KEY)) {
            a.write("in_octets", new Sample(START_MS, 1_000_000L));
            a.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L)); // slot1 = 2666.67
            a.checkpoint();
        }

        // Writer B: reabre no meio e preenche o slot2; sem reidratação o estado
        // anterior se perderia.
        try (NgrrdWriter b = new NgrrdWriter(def, storage, SERIES_KEY)) {
            b.write("in_octets", new Sample(START_MS + 2L * STEP_MS, 1_200_000L)); // slot2 = 2666.67
            b.checkpoint();
        }

        List<Double> values = readInBps(def, storage, START_MS + 3L * STEP_MS);
        assertEquals(2, values.size(),
                "esperava 2 pontos (slot1 do writer A + slot2 do writer B), sem perda: " + values);
        values.forEach(v -> assertEquals(EXPECTED_BPS, v, 1e-6));
    }

    private static boolean temArquivoNgrr(Path root) throws Exception {
        try (var files = Files.walk(root)) {
            return files.anyMatch(p -> p.getFileName().toString().endsWith(".ngrr"));
        }
    }
}
