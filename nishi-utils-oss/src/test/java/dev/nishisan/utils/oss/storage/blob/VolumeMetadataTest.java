package dev.nishisan.utils.oss.storage.blob;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Codec do {@code volume.meta} (ver doc/oss/ngrrd-blob-volume.md §4). */
class VolumeMetadataTest {

    private static VolumeMetadata sample() {
        return new VolumeMetadata(1, 64,
                new UUID(0x0123456789ABCDEFL, 0xFEDCBA9876543210L),
                1L << 30, BlobRouting.ALGORITHM_SHA256_PREFIX64, BlobRouting.ROUTING_VERSION, 7L);
    }

    @Test
    void encodesToFixedSize() {
        assertEquals(VolumeMetadata.BYTES, sample().encode().length);
    }

    @Test
    void roundTripPreservesAllFields() {
        VolumeMetadata original = sample();
        VolumeMetadata decoded = VolumeMetadata.decode(original.encode());
        assertEquals(original.formatVersion(), decoded.formatVersion());
        assertEquals(original.shardCount(), decoded.shardCount());
        assertEquals(original.volumeUuid(), decoded.volumeUuid());
        assertEquals(original.segmentBytes(), decoded.segmentBytes());
        assertEquals(original.routingAlgorithm(), decoded.routingAlgorithm());
        assertEquals(original.routingVersion(), decoded.routingVersion());
        assertEquals(original.generation(), decoded.generation());
    }

    @Test
    void rejectsBadMagic() {
        byte[] image = sample().encode();
        image[0] ^= 0xFF;
        assertThrows(BlobVolumeException.class, () -> VolumeMetadata.decode(image));
    }

    @Test
    void rejectsCorruptedCrc() {
        byte[] image = sample().encode();
        image[20] ^= 0xFF; // muta segmentBytes sem recalcular CRC
        assertThrows(BlobVolumeException.class, () -> VolumeMetadata.decode(image));
    }

    @Test
    void rejectsTruncatedImage() {
        byte[] image = sample().encode();
        byte[] truncated = Arrays.copyOf(image, VolumeMetadata.BYTES - 1);
        assertThrows(BlobVolumeException.class, () -> VolumeMetadata.decode(truncated));
    }

    @Test
    void rejectsUnknownFormatVersion() {
        VolumeMetadata future = new VolumeMetadata(2, 64, UUID.randomUUID(),
                1L << 30, BlobRouting.ALGORITHM_SHA256_PREFIX64, BlobRouting.ROUTING_VERSION, 0L);
        byte[] image = future.encode();
        assertThrows(BlobVolumeException.class, () -> VolumeMetadata.decode(image));
    }
}
