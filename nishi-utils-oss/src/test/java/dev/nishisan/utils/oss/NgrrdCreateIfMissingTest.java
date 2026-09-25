package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@code Ngrrd.OpenOptions.createIfMissing} e {@code Ngrrd.exists} no
 * modo local, nos dois backends com {@link dev.nishisan.utils.oss.storage.SeriesChannelProvider}:
 * sharded blob (via locator) e localDisk (via {@code fromYaml}+tags).
 *
 * <p>Ver {@code planning/2026-09-25-issue-171-series-exists-design.md}.</p>
 */
class NgrrdCreateIfMissingTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;

    private String loadBlobYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private String loadLocalDiskYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private BlobVolumeRegistry blobRegistry(Path base) {
        return NgrrdBlob.registry()
                .basePath(base).shardCount(4).segmentBytes(1L << 20)
                .volume("ifaceStats")
                .build();
    }

    private Map<String, String> sampleTags() {
        return Map.of(
                "deviceId", "r1",
                "interfaceId", "eth0",
                "region", "br-sp",
                "vendor", "x",
                "role", "core");
    }

    // ---------------------------------------------------------------- blob

    @Test
    void abrirSemCriarSerieInexistenteNoBlobLancaENadaCria(@TempDir Path base) throws Exception {
        String yaml = loadBlobYaml();
        try (BlobVolumeRegistry registry = blobRegistry(base)) {
            NgrrdUri locator = NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0");
            Ngrrd.OpenOptions options = Ngrrd.OpenOptions.defaults().withCreateIfMissing(false);

            SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                    () -> Ngrrd.open(registry, locator, yaml, options));
            assertEquals(locator.seriesPath(), ex.seriesKey());
            assertEquals(SeriesNotFoundException.Reason.ABSENT, ex.reason());

            BlobVolume volume = registry.require("ifaceStats");
            assertFalse(volume.storage().exists("series/device:r1/iface:eth0.ngrr"));
            assertFalse(Ngrrd.exists(volume, locator, yaml));
        }
    }

    @Test
    void abrirSemCriarSerieExistenteNoBlobAbre(@TempDir Path base) throws Exception {
        String yaml = loadBlobYaml();
        try (BlobVolumeRegistry registry = blobRegistry(base)) {
            NgrrdUri locator = NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0");
            try (NgrrdHandle handle = Ngrrd.open(registry, locator, yaml)) {
                handle.write("in_octets", new Sample(BLOCK_START_MS, 50_000L));
                handle.checkpoint();
            }

            try (NgrrdHandle handle = Ngrrd.open(registry, locator, yaml,
                    Ngrrd.OpenOptions.defaults().withCreateIfMissing(false))) {
                assertEquals(locator.seriesPath(), handle.seriesKey());
            }
            assertTrue(Ngrrd.exists(registry, locator, yaml));
        }
    }

    @Test
    void openPadraoContinuaCriandoNoBlob(@TempDir Path base) throws Exception {
        String yaml = loadBlobYaml();
        assertTrue(Ngrrd.OpenOptions.defaults().createIfMissing());
        try (BlobVolumeRegistry registry = blobRegistry(base)) {
            NgrrdUri locator = NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0");
            assertFalse(Ngrrd.exists(registry, locator, yaml));
            try (NgrrdHandle handle = Ngrrd.open(registry, locator, yaml)) {
                assertEquals(locator.seriesPath(), handle.seriesKey());
            }
            assertTrue(Ngrrd.exists(registry, locator, yaml));
        }
    }

    // ---------------------------------------------------------------- localDisk

    @Test
    void abrirSemCriarSerieInexistenteNoLocalDiskLancaENadaCria(@TempDir Path tempDir) throws Exception {
        String yaml = loadLocalDiskYaml();
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        Map<String, String> tags = sampleTags();
        Ngrrd.OpenOptions options = Ngrrd.OpenOptions.defaults().withCreateIfMissing(false);

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                () -> Ngrrd.fromYaml(yaml, bindings, tags, null, options));
        assertEquals("device:r1/iface:eth0", ex.seriesKey());
        assertEquals(SeriesNotFoundException.Reason.ABSENT, ex.reason());

        assertNoNgrFilesUnder(tempDir);
        assertFalse(Ngrrd.exists(yaml, bindings, tags));
    }

    @Test
    void abrirSemCriarSerieExistenteNoLocalDiskAbre(@TempDir Path tempDir) throws Exception {
        String yaml = loadLocalDiskYaml();
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        Map<String, String> tags = sampleTags();

        try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags, null)) {
            handle.write("in_octets", new Sample(BLOCK_START_MS, 50_000L));
            handle.checkpoint();
        }

        try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags, null,
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(false))) {
            assertEquals("device:r1/iface:eth0", handle.seriesKey());
        }
        assertTrue(Ngrrd.exists(yaml, bindings, tags));
    }

    @Test
    void openPadraoContinuaCriandoNoLocalDisk(@TempDir Path tempDir) throws Exception {
        String yaml = loadLocalDiskYaml();
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        Map<String, String> tags = sampleTags();

        assertFalse(Ngrrd.exists(yaml, bindings, tags));
        try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags, null)) {
            assertEquals("device:r1/iface:eth0", handle.seriesKey());
        }
        assertTrue(Ngrrd.exists(yaml, bindings, tags));
    }

    @Test
    void construtorDeDoisArgumentosMantemCriacao() {
        assertTrue(new Ngrrd.OpenOptions(null, null).createIfMissing());
    }

    private void assertNoNgrFilesUnder(Path root) throws Exception {
        try (Stream<Path> paths = Files.walk(root)) {
            assertFalse(paths.anyMatch(p -> p.toString().endsWith(".ngrr")),
                    "esperava nenhum arquivo .ngrr criado em " + root);
        }
    }
}
