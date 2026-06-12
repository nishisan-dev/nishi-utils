package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.StorageSpec;
import dev.nishisan.utils.oss.storage.S3Settings;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a superfície de durabilidade na façade {@link Ngrrd}: precedência de
 * resolução (override de abertura {@literal >} default do YAML {@literal >} FSYNC),
 * o fail-fast de {@code OS_CACHE} com backend {@code OBJECT_STORAGE} e o caminho
 * fim-a-fim de {@code OS_CACHE} em disco local.
 */
class NgrrdDurabilityFacadeTest {

    private static final long START_SEC = 1_747_339_200L;
    private static final long START_MS = START_SEC * 1000L;
    private static final int STEP_MS = 300_000;
    private static final double EXPECTED_BPS = 8 * 100_000.0 / 300.0;

    private String yaml(String resource) throws Exception {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static StorageSpec specWithDurability(Durability durability) {
        return new StorageSpec(null, null, null, durability);
    }

    @Test
    void resolveDurabilityRespeitaPrecedencia() {
        // YAML ausente + sem override → FSYNC (default).
        assertEquals(Durability.FSYNC,
                Ngrrd.resolveDurability(Ngrrd.OpenOptions.defaults(), specWithDurability(null)));

        // YAML define OS_CACHE + sem override → OS_CACHE.
        assertEquals(Durability.OS_CACHE,
                Ngrrd.resolveDurability(Ngrrd.OpenOptions.defaults(), specWithDurability(Durability.OS_CACHE)));

        // Override de abertura vence o YAML (ambas as direções).
        assertEquals(Durability.FSYNC,
                Ngrrd.resolveDurability(Ngrrd.OpenOptions.durability(Durability.FSYNC),
                        specWithDurability(Durability.OS_CACHE)));
        assertEquals(Durability.OS_CACHE,
                Ngrrd.resolveDurability(Ngrrd.OpenOptions.durability(Durability.OS_CACHE),
                        specWithDurability(null)));
    }

    @Test
    void s3ComOsCacheRejeitaNaAbertura() throws Exception {
        String objectStorageYaml = yaml("/iface-traffic-errors-v1.yaml");
        StorageFactory.StorageBindings bindings =
                StorageFactory.StorageBindings.forS3(S3Settings.forAws("dummy-bucket", "us-east-1"));
        Map<String, String> tags = Map.of("deviceId", "r1", "interfaceId", "eth0",
                "region", "br", "vendor", "acme", "role", "edge");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Ngrrd.fromYaml(objectStorageYaml, bindings, tags, null,
                        Ngrrd.OpenOptions.durability(Durability.OS_CACHE)));
        assertTrue(ex.getMessage().contains("OBJECT_STORAGE"),
                "mensagem deveria explicar a incompatibilidade S3/OS_CACHE: " + ex.getMessage());
    }

    @Test
    void osCacheEmDiscoLocalAbreEFunciona(@TempDir Path tempDir) throws Exception {
        String localYaml = yaml("/iface-traffic-local-disk.yaml");
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        Map<String, String> tags = Map.of("deviceId", "r1", "interfaceId", "eth0");

        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        try (NgrrdHandle handle = Ngrrd.fromYaml(localYaml, bindings, tags, null,
                Ngrrd.OpenOptions.durability(Durability.OS_CACHE))) {
            handle.write("in_octets", new Sample(START_MS, 1_000_000L));
            handle.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            handle.checkpoint();

            SeriesResult result = handle.read("in_bps", query, START_MS + 2L * STEP_MS);
            List<Double> values = result.points().stream().map(p -> p.value())
                    .filter(v -> !Double.isNaN(v)).toList();
            assertTrue(values.contains(EXPECTED_BPS),
                    "OS_CACHE em disco local deveria manter o CDP parcial legível: " + values);
        }
    }
}
