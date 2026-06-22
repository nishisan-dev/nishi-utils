package dev.nishisan.utils.oss.storage.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Shard mapeado em memória (mmap segmentado). Ver doc/oss/ngrrd-blob-volume.md §5/§10. */
class MappedShardTest {

    private static final long SEG = 64 * 1024; // 64 KiB por segmento (pequeno p/ testar multi-segmento)
    private static final UUID UUID_V = new UUID(0xDEADBEEFL, 0xCAFEBABEL);

    private static ShardSuperblock superblock(int shardId, long capacity) {
        return new ShardSuperblock(1, shardId, 64, ShardSuperblock.HEADER_BYTES,
                capacity, ShardSuperblock.HEADER_BYTES, UUID_V, 1L);
    }

    private static byte[] pattern(int len, int seed) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i);
        }
        return b;
    }

    @Test
    void createWriteReadRoundTripWithinRegion(@TempDir Path dir) {
        Path file = dir.resolve("shard-00.blob");
        byte[] data = pattern(8192, 7);
        try (MappedShard shard = MappedShard.create(file, superblock(0, SEG), SEG)) {
            shard.writeAt(4096L, data);
            shard.forceRange(4096L, data.length);
            assertArrayEquals(data, shard.readAt(4096L, data.length));
        }
    }

    @Test
    void reopenReadsSuperblockAndData(@TempDir Path dir) {
        Path file = dir.resolve("shard-03.blob");
        byte[] data = pattern(4096, 33);
        try (MappedShard shard = MappedShard.create(file, superblock(3, SEG), SEG)) {
            shard.writeAt(8192L, data);
            shard.forceRange(8192L, data.length);
        }
        try (MappedShard reopened = MappedShard.open(file, SEG)) {
            assertEquals(3, reopened.superblock().shardId());
            assertEquals(SEG, reopened.capacity());
            assertArrayEquals(data, reopened.readAt(8192L, data.length));
        }
    }

    @Test
    void readsAndWritesAcrossSegments(@TempDir Path dir) {
        Path file = dir.resolve("shard-01.blob");
        byte[] data = pattern(2048, 99);
        try (MappedShard shard = MappedShard.create(file, superblock(1, 2 * SEG), SEG)) {
            long offsetInSegment1 = SEG + 100;
            shard.writeAt(offsetInSegment1, data);
            shard.forceRange(offsetInSegment1, data.length);
            assertArrayEquals(data, shard.readAt(offsetInSegment1, data.length));
        }
    }

    @Test
    void writeSuperblockRoundTrips(@TempDir Path dir) {
        Path file = dir.resolve("shard-00.blob");
        try (MappedShard shard = MappedShard.create(file, superblock(0, SEG), SEG)) {
            ShardSuperblock updated = new ShardSuperblock(1, 0, 64, ShardSuperblock.HEADER_BYTES,
                    SEG, 20480L, UUID_V, 9L);
            shard.writeSuperblock(updated);
            assertEquals(20480L, shard.superblock().bumpCursor());
        }
        try (MappedShard reopened = MappedShard.open(file, SEG)) {
            assertEquals(20480L, reopened.superblock().bumpCursor());
            assertEquals(9L, reopened.superblock().generation());
        }
    }

    @Test
    void growExtendsCapacityAndAllowsWritesInNewRegion(@TempDir Path dir) {
        Path file = dir.resolve("shard-00.blob");
        byte[] data = pattern(4096, 5);
        try (MappedShard shard = MappedShard.create(file, superblock(0, SEG), SEG)) {
            shard.grow(2 * SEG);
            assertEquals(2 * SEG, shard.capacity());
            long offset = SEG + 4096;
            shard.writeAt(offset, data);
            shard.forceRange(offset, data.length);
            assertArrayEquals(data, shard.readAt(offset, data.length));
        }
    }

    @Test
    void rejectsReadBeyondCapacity(@TempDir Path dir) {
        Path file = dir.resolve("shard-00.blob");
        try (MappedShard shard = MappedShard.create(file, superblock(0, SEG), SEG)) {
            assertThrows(BlobVolumeException.class, () -> shard.readAt(SEG - 10, 100));
        }
    }
}
