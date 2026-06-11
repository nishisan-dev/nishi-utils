package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.NamingScheme;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o {@link SeriesChannel} sobre disco local: pré-alocação, escrita in-place
 * por região, persistência após {@code force}/reabertura e a chave do objeto único.
 */
class SeriesChannelLocalTest {

    @Test
    void chaveSeriesUsaPrefixoEExtensaoNgrr() {
        ObjectNaming naming = new ObjectNaming(NamingScheme.DETERMINISTIC, null, null);
        assertEquals("series/device:r1/iface:eth0.ngrr",
                StorageKey.series(naming, "device:r1/iface:eth0"));
    }

    @Test
    void allocateEscritaPorRegiaoEReabertura(@TempDir Path tempDir) {
        LocalDiskStorage storage = new LocalDiskStorage(tempDir);
        String key = "series/device:r1/iface:eth0.ngrr";

        assertTrue(!storage.seriesExists(key), "série não deveria existir ainda");

        byte[] head = "HEADER--".getBytes();   // 8 bytes
        byte[] mid = "MIDDLE!!".getBytes();     // 8 bytes
        try (SeriesChannel ch = storage.openSeries(key)) {
            ch.allocate(64);
            assertEquals(64, ch.size());
            ch.writeRegion(0, head);
            ch.writeRegion(40, mid);
            ch.force();
        }

        assertTrue(storage.seriesExists(key));
        try (SeriesChannel ch = storage.openSeries(key)) {
            assertEquals(64, ch.size());
            assertArrayEquals(head, ch.readRegion(0, 8));
            assertArrayEquals(mid, ch.readRegion(40, 8));
            // Região não escrita permanece zerada.
            assertArrayEquals(new byte[8], ch.readRegion(16, 8));
        }
    }

    @Test
    void escritaInPlaceAlteraApenasARegiao(@TempDir Path tempDir) {
        LocalDiskStorage storage = new LocalDiskStorage(tempDir);
        String key = "series/s.ngrr";
        try (SeriesChannel ch = storage.openSeries(key)) {
            ch.allocate(32);
            ch.writeRegion(0, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
            ch.force();
        }
        try (SeriesChannel ch = storage.openSeries(key)) {
            ch.writeRegion(8, new byte[]{9, 9, 9, 9});
            ch.force();
        }
        byte[] all = storage.get(key).orElseThrow();
        assertEquals(32, all.length);
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Arrays.copyOfRange(all, 0, 8));
        assertArrayEquals(new byte[]{9, 9, 9, 9}, Arrays.copyOfRange(all, 8, 12));
    }
}
