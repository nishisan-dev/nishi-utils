package dev.nishisan.utils.oss.metrics;

import dev.nishisan.utils.oss.blob.VolumeWriteMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Campos derivados e compatibilidade de construtores do snapshot do volume. */
class BlobVolumeStatsTest {

    private static BlobVolumeStats stats(long declared, long live, long reserved) {
        return new BlobVolumeStats(1, new long[]{1L}, new long[]{1L}, new long[]{1L}, new double[]{1.0},
                1, 0L, 0L, VolumeWriteMode.MMAP, 0L, declared, live, reserved);
    }

    @Test
    void fillDeclaradaSemCapacidadeEZero() {
        assertEquals(0.0, stats(0L, 500L, 500L).fillOfDeclaredCapacity());
    }

    @Test
    void fillDeclaradaSomaLiveEReservadoSobreADeclarada() {
        assertEquals(0.75, stats(1000L, 500L, 250L).fillOfDeclaredCapacity(), 1e-12);
    }

    @Test
    void construtoresAntigosZeramOsCamposNovos() {
        BlobVolumeStats legacy = new BlobVolumeStats(1, new long[]{1L}, new long[]{1L}, new long[]{1L},
                new double[]{1.0}, 1, 0L, 0L, VolumeWriteMode.MMAP, 7L);
        assertEquals(0L, legacy.declaredCapacityBytes());
        assertEquals(0L, legacy.liveBytes());
        assertEquals(0L, legacy.reservedBytes());
        assertEquals(7L, legacy.bytesWritten());
        assertEquals(0.0, legacy.fillOfDeclaredCapacity());
    }
}
