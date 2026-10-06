package dev.nishisan.utils.oss.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Enum de modo de escrita e sua propagação pela configuração do volume. */
class VolumeWriteModeTest {

    @Test
    void parseAceitaQualquerCaixa() {
        assertEquals(VolumeWriteMode.MMAP, VolumeWriteMode.parse("mmap"));
        assertEquals(VolumeWriteMode.MMAP, VolumeWriteMode.parse("MMAP"));
        assertEquals(VolumeWriteMode.PWRITE, VolumeWriteMode.parse("pwrite"));
        assertEquals(VolumeWriteMode.PWRITE, VolumeWriteMode.parse("  PWrite "));
    }

    @Test
    void parseInvalidoListaValoresAceitos() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> VolumeWriteMode.parse("directio"));
        assertTrue(e.getMessage().contains("directio"), e.getMessage());
        assertTrue(e.getMessage().contains("mmap") && e.getMessage().contains("pwrite"), e.getMessage());
    }

    @Test
    void parseNuloOuVazioFalha() {
        assertThrows(IllegalArgumentException.class, () -> VolumeWriteMode.parse(null));
        assertThrows(IllegalArgumentException.class, () -> VolumeWriteMode.parse(" "));
    }

    @Test
    void configComConstrutorAntigoUsaMmap(@TempDir Path dir) {
        BlobVolumeConfig cfg = new BlobVolumeConfig("v1", dir, 2, 1L << 20, 1L << 20);
        assertEquals(VolumeWriteMode.MMAP, cfg.writeMode());
        assertEquals(VolumeWriteMode.MMAP, BlobVolumeConfig.of("v1", dir).writeMode());
    }

    @Test
    void configComModoExplicitoPreservaOModo(@TempDir Path dir) {
        BlobVolumeConfig cfg = new BlobVolumeConfig("v1", dir, 2, 1L << 20, 1L << 20, VolumeWriteMode.PWRITE);
        assertEquals(VolumeWriteMode.PWRITE, cfg.writeMode());
    }

    @Test
    void configRejeitaModoNulo(@TempDir Path dir) {
        assertThrows(NullPointerException.class,
                () -> new BlobVolumeConfig("v1", dir, 2, 1L << 20, 1L << 20, null));
    }

    @Test
    void builderPropagaOModoAoVolume(@TempDir Path base) {
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .writeMode(VolumeWriteMode.PWRITE)
                .volume("ifaceStats")
                .build()) {
            assertEquals(VolumeWriteMode.PWRITE, reg.require("ifaceStats").storage().writeMode());
            assertEquals(VolumeWriteMode.PWRITE, reg.require("ifaceStats").storage().stats().writeMode());
        }
    }

    @Test
    void builderSemModoUsaMmap(@TempDir Path base) {
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .volume("ifaceStats")
                .build()) {
            assertEquals(VolumeWriteMode.MMAP, reg.require("ifaceStats").storage().writeMode());
        }
    }
}
