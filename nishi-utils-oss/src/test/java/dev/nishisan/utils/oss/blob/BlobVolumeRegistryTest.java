package dev.nishisan.utils.oss.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Registro programático de blob volumes (ver doc/oss/ngrrd-blob-volume.md). */
class BlobVolumeRegistryTest {

    @Test
    void builderResolvesVolumeDirUnderBasePath(@TempDir Path base) {
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .volume("ifaceStats")
                .build()) {
            BlobVolume v = reg.require("ifaceStats");
            assertEquals(base.resolve("ifaceStats"), v.directory());
            assertEquals(2, v.shardCount());
        }
    }

    @Test
    void registersMultipleVolumesAndLooksThemUp(@TempDir Path base) {
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .volume("v1").volume("flows")
                .build()) {
            Set<String> names = reg.volumes().stream().map(BlobVolume::name).collect(Collectors.toSet());
            assertEquals(Set.of("v1", "flows"), names);
            assertSame(reg.require("v1"), reg.lookup("v1").orElseThrow());
            assertTrue(reg.lookup("absent").isEmpty());
            assertThrows(IllegalArgumentException.class, () -> reg.require("absent"));
        }
    }

    @Test
    void volumeStorageIsUsableViaBindings(@TempDir Path base) {
        byte[] data = "hello-ngrrd".getBytes();
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .volume("v1").build()) {
            BlobVolume v = reg.require("v1");
            v.storage().put("series/x.ngrr", data);
            assertArrayEquals(data, v.storage().get("series/x.ngrr").orElseThrow());
            // bindings() pode ser usado pelo StorageFactory para abrir handles
            assertSame(v.storage(), v.bindings().blobVolume());
        }
    }

    @Test
    void openIsIdempotentPerName(@TempDir Path base) {
        BlobVolumeConfig cfg = new BlobVolumeConfig("v1", base.resolve("v1"), 2, 1L << 20, 1L << 20);
        try (BlobVolumeRegistry reg = NgrrdBlob.registry().build()) {
            BlobVolume first = reg.open(cfg);
            BlobVolume second = reg.open(cfg);
            assertSame(first, second);
        }
    }

    @Test
    void dataSurvivesReopeningTheVolume(@TempDir Path base) {
        byte[] data = "persisted".getBytes();
        BlobVolumeConfig cfg = new BlobVolumeConfig("v1", base.resolve("v1"), 2, 1L << 20, 1L << 20);
        try (BlobVolumeRegistry reg = NgrrdBlob.registry().build()) {
            reg.open(cfg).storage().put("series/p.ngrr", data);
        }
        try (BlobVolumeRegistry reg = NgrrdBlob.registry().build()) {
            assertArrayEquals(data, reg.open(cfg).storage().get("series/p.ngrr").orElseThrow());
        }
    }
}
