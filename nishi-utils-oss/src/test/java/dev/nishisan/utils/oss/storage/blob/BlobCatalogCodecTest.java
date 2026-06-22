package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Codec do snapshot do catálogo (catalog.bin). Ver doc/oss/ngrrd-blob-volume.md §6. */
class BlobCatalogCodecTest {

    private static final UUID UUID_V = new UUID(1L, 2L);

    @Test
    void roundTripEmptyCatalog() {
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 5L, List.of());
        BlobCatalogCodec.Snapshot s = BlobCatalogCodec.decode(image);
        assertEquals(64, s.shardCount());
        assertEquals(UUID_V, s.volumeUuid());
        assertEquals(5L, s.generation());
        assertEquals(List.of(), s.entries());
    }

    @Test
    void roundTripMultipleEntriesIncludingTombstone() {
        List<CatalogEntry> entries = List.of(
                new CatalogEntry("series/device:r1/iface:eth0/group:traffic-v1.ngrr", 37, 4096L, 344064L, State.LIVE),
                new CatalogEntry("series/höst=π;if=eth0.ngrr", 15, 348160L, 8192L, State.LIVE),
                new CatalogEntry("series/old.ngrr", 3, 356352L, 8192L, State.DELETED));
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 99L, entries);
        BlobCatalogCodec.Snapshot s = BlobCatalogCodec.decode(image);
        assertEquals(entries, s.entries());
        assertEquals(99L, s.generation());
    }

    @Test
    void rejectsBadHeaderMagic() {
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 1L, List.of());
        image[1] ^= 0xFF;
        assertThrows(BlobVolumeException.class, () -> BlobCatalogCodec.decode(image));
    }

    @Test
    void rejectsCorruptedHeaderCrc() {
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 1L, List.of());
        image[12] ^= 0xFF; // muta shardCount sem recalcular CRC
        assertThrows(BlobVolumeException.class, () -> BlobCatalogCodec.decode(image));
    }

    @Test
    void rejectsCorruptedEntry() {
        List<CatalogEntry> entries = List.of(
                new CatalogEntry("series/a.ngrr", 1, 4096L, 8192L, State.LIVE));
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 1L, entries);
        image[image.length - 8] ^= 0xFF; // muta bytes de uma entrada
        assertThrows(BlobVolumeException.class, () -> BlobCatalogCodec.decode(image));
    }

    @Test
    void rejectsCorruptedTrailerCrc() {
        List<CatalogEntry> entries = List.of(
                new CatalogEntry("series/a.ngrr", 1, 4096L, 8192L, State.LIVE));
        byte[] image = BlobCatalogCodec.encode(64, UUID_V, 1L, entries);
        image[image.length - 1] ^= 0xFF; // muta o trailer CRC
        assertThrows(BlobVolumeException.class, () -> BlobCatalogCodec.decode(image));
    }

    @Test
    void rejectsTruncatedImage() {
        assertThrows(BlobVolumeException.class, () -> BlobCatalogCodec.decode(new byte[10]));
    }
}
