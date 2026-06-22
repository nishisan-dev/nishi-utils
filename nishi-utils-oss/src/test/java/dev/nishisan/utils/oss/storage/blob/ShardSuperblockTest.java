package dev.nishisan.utils.oss.storage.blob;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Codec do superblock do shard (ver doc/oss/ngrrd-blob-volume.md §5). */
class ShardSuperblockTest {

    private static ShardSuperblock sample() {
        return new ShardSuperblock(1, 7, 64,
                ShardSuperblock.HEADER_BYTES, 1L << 30, 4096L + 3 * 8192L,
                new UUID(0xAABBCCDDEEFF0011L, 0x2233445566778899L), 42L);
    }

    @Test
    void encodesToExactlyOnePage() {
        assertEquals(ShardSuperblock.BYTES, sample().encode().length);
        assertEquals(4096, ShardSuperblock.BYTES);
    }

    @Test
    void roundTripPreservesAllFields() {
        ShardSuperblock original = sample();
        ShardSuperblock decoded = ShardSuperblock.decode(original.encode());
        assertEquals(original.formatVersion(), decoded.formatVersion());
        assertEquals(original.shardId(), decoded.shardId());
        assertEquals(original.shardCount(), decoded.shardCount());
        assertEquals(original.headerBytes(), decoded.headerBytes());
        assertEquals(original.shardCapacityBytes(), decoded.shardCapacityBytes());
        assertEquals(original.bumpCursor(), decoded.bumpCursor());
        assertEquals(original.volumeUuid(), decoded.volumeUuid());
        assertEquals(original.generation(), decoded.generation());
    }

    @Test
    void rejectsBadMagic() {
        byte[] image = sample().encode();
        image[3] ^= 0xFF;
        assertThrows(BlobVolumeException.class, () -> ShardSuperblock.decode(image));
    }

    @Test
    void rejectsCorruptedCrc() {
        byte[] image = sample().encode();
        image[40] ^= 0xFF; // muta bumpCursor sem recalcular CRC
        assertThrows(BlobVolumeException.class, () -> ShardSuperblock.decode(image));
    }

    @Test
    void rejectsUndersizedImage() {
        assertThrows(BlobVolumeException.class, () -> ShardSuperblock.decode(new byte[100]));
    }

    @Test
    void rejectsUnknownFormatVersion() {
        ShardSuperblock future = new ShardSuperblock(2, 0, 64,
                ShardSuperblock.HEADER_BYTES, 1L << 30, 4096L, UUID.randomUUID(), 0L);
        byte[] image = future.encode();
        assertThrows(BlobVolumeException.class, () -> ShardSuperblock.decode(image));
    }
}
