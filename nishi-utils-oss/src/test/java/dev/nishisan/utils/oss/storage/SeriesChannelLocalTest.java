package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.NamingScheme;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    @Test
    void prepareForcePadraoEInline(@TempDir Path tempDir) throws Exception {
        // Disco local e S3 herdam o padrão da interface: nenhum dos dois destaca o force.
        assertSame(SeriesChannel.class,
                Class.forName(LocalDiskStorage.class.getName() + "$LocalSeriesChannel")
                        .getMethod("prepareForce").getDeclaringClass());
        assertSame(SeriesChannel.class,
                Class.forName(S3Storage.class.getName() + "$S3SeriesChannel")
                        .getMethod("prepareForce").getDeclaringClass());

        // O padrão faz o force inline, na thread chamadora, e devolve DONE.
        List<Thread> forceThreads = new ArrayList<>();
        LocalDiskStorage storage = new LocalDiskStorage(tempDir);
        String key = "series/inline.ngrr";
        try (SeriesChannel inner = storage.openSeries(key)) {
            inner.allocate(16);
            SeriesChannel spy = new ForceRecordingChannel(inner, forceThreads);
            spy.writeRegion(0, new byte[]{7, 7, 7, 7});
            SeriesChannel.ForceTicket ticket = spy.prepareForce();
            assertSame(SeriesChannel.ForceTicket.DONE, ticket);
            assertFalse(ticket.detached());
            assertEquals(List.of(Thread.currentThread()), forceThreads, "o force roda inline, uma vez");
            ticket.run();
            assertEquals(1, forceThreads.size(), "DONE.run() não força de novo");
        }
        assertArrayEquals(new byte[]{7, 7, 7, 7}, Arrays.copyOfRange(storage.get(key).orElseThrow(), 0, 4));
    }

    /** Canal que delega e registra a thread de cada {@code force()}; herda o {@code prepareForce()} padrão. */
    private record ForceRecordingChannel(SeriesChannel inner, List<Thread> forceThreads) implements SeriesChannel {

        @Override
        public long size() {
            return inner.size();
        }

        @Override
        public void allocate(long totalBytes) {
            inner.allocate(totalBytes);
        }

        @Override
        public byte[] readRegion(long offset, int len) {
            return inner.readRegion(offset, len);
        }

        @Override
        public void writeRegion(long offset, byte[] data) {
            inner.writeRegion(offset, data);
        }

        @Override
        public void force() {
            forceThreads.add(Thread.currentThread());
            inner.force();
        }

        @Override
        public void close() {
            inner.close();
        }
    }
}
