package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.blob.VolumeWriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    // ---------------------------------------------------------------- modos de escrita

    @Test
    void modoPadraoDoShardEMmap(@TempDir Path dir) {
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG)) {
            assertEquals(VolumeWriteMode.MMAP, shard.writeMode());
        }
        try (MappedShard reopened = MappedShard.open(dir.resolve("shard-00.blob"), SEG)) {
            assertEquals(VolumeWriteMode.MMAP, reopened.writeMode());
        }
    }

    @ParameterizedTest
    @EnumSource(VolumeWriteMode.class)
    void escritaELeituraDentroDeUmSegmentoNosDoisModos(VolumeWriteMode mode, @TempDir Path dir) {
        byte[] data = pattern(8192, 7);
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG, mode)) {
            assertEquals(mode, shard.writeMode());
            shard.writeAt(4096L, data);
            assertArrayEquals(data, shard.readAt(4096L, data.length));
        }
    }

    @ParameterizedTest
    @EnumSource(VolumeWriteMode.class)
    void escritaCruzandoFronteiraDeSegmentoNosDoisModos(VolumeWriteMode mode, @TempDir Path dir) {
        byte[] data = pattern(4096, 99);
        long offset = SEG - 1000; // 1000 bytes no segmento 0 e o restante no segmento 1
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-01.blob"), superblock(1, 2 * SEG), SEG, mode)) {
            shard.writeAt(offset, data);
            assertArrayEquals(data, shard.readAt(offset, data.length));
        }
    }

    @ParameterizedTest
    @EnumSource(VolumeWriteMode.class)
    void escritaAposGrowNosDoisModos(VolumeWriteMode mode, @TempDir Path dir) {
        byte[] data = pattern(4096, 5);
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG, mode)) {
            shard.grow(3 * SEG);
            long offset = 2 * SEG + 4096;
            shard.writeAt(offset, data);
            assertArrayEquals(data, shard.readAt(offset, data.length));
            long crossing = 2 * SEG - 100;
            shard.writeAt(crossing, data);
            assertArrayEquals(data, shard.readAt(crossing, data.length));
        }
    }

    @ParameterizedTest
    @EnumSource(VolumeWriteMode.class)
    void bytesWrittenContaBytesLogicosNosDoisModos(VolumeWriteMode mode, @TempDir Path dir) {
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, 2 * SEG), SEG, mode)) {
            long afterSuperblock = shard.bytesWritten();
            assertEquals(ShardSuperblock.BYTES, afterSuperblock, "criação grava o superblock via writeAt");
            shard.writeAt(8192L, pattern(100, 1));
            shard.writeAt(SEG - 10, pattern(50, 2)); // cruza segmento: conta uma única escrita lógica de 50 bytes
            assertEquals(afterSuperblock + 150, shard.bytesWritten());
            shard.readAt(8192L, 100);
            assertEquals(afterSuperblock + 150, shard.bytesWritten(), "leitura não conta");
        }
    }

    @Test
    void pwriteEnxergaPeloMmapSemForce(@TempDir Path dir) {
        byte[] first = pattern(512, 11);
        byte[] second = pattern(512, 77);
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG,
                VolumeWriteMode.PWRITE)) {
            shard.writeAt(10_000L, first);
            assertArrayEquals(first, shard.readAt(10_000L, first.length));
            shard.writeAt(10_000L, second);
            assertArrayEquals(second, shard.readAt(10_000L, second.length));
            assertNotEquals(first[0], shard.readAt(10_000L, 1)[0]);
        }
    }

    @Test
    void pwriteGravaOsMesmosBytesQueMmapNoArquivoEAposReabrir(@TempDir Path dir) {
        byte[] a = pattern(6000, 3);
        byte[] b = pattern(300, 200);
        Path mmapFile = dir.resolve("mmap.blob");
        Path pwriteFile = dir.resolve("pwrite.blob");
        for (Object[] c : new Object[][]{{mmapFile, VolumeWriteMode.MMAP}, {pwriteFile, VolumeWriteMode.PWRITE}}) {
            try (MappedShard shard = MappedShard.create((Path) c[0], superblock(0, 2 * SEG), SEG,
                    (VolumeWriteMode) c[1])) {
                shard.writeAt(5000L, a);
                shard.writeAt(SEG - 100, b);
                shard.forceRange(5000L, a.length);
                shard.forceRange(SEG - 100, b.length);
            }
        }
        // superblocks iguais (mesmos parâmetros); o conteúdo bruto dos arquivos deve coincidir
        try (MappedShard m = MappedShard.open(mmapFile, SEG, VolumeWriteMode.PWRITE);
             MappedShard p = MappedShard.open(pwriteFile, SEG, VolumeWriteMode.MMAP)) {
            assertArrayEquals(m.readAt(0, (int) (2 * SEG)), p.readAt(0, (int) (2 * SEG)));
            assertArrayEquals(a, p.readAt(5000L, a.length));
            assertArrayEquals(b, p.readAt(SEG - 100, b.length));
        }
    }

    @Test
    void pwriteAceitaEscritasConcorrentesEmRegioesDisjuntas(@TempDir Path dir) throws Exception {
        int writers = 8;
        int regionBytes = 4096;
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG,
                VolumeWriteMode.PWRITE)) {
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int id = w;
                Thread t = new Thread(() -> {
                    byte[] data = pattern(regionBytes, id * 16);
                    for (int i = 0; i < 200; i++) {
                        shard.writeAt(8192L + (long) id * regionBytes, data);
                    }
                });
                threads.add(t);
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
            for (int w = 0; w < writers; w++) {
                assertArrayEquals(pattern(regionBytes, w * 16), shard.readAt(8192L + (long) w * regionBytes, regionBytes));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(VolumeWriteMode.class)
    void escritaForaDaCapacidadeERejeitadaNosDoisModos(VolumeWriteMode mode, @TempDir Path dir) {
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG, mode)) {
            assertThrows(BlobVolumeException.class, () -> shard.writeAt(SEG - 10, pattern(100, 1)));
        }
    }

    // ---------------------------------------------------------------- contadores por caminho

    @Test
    void pwriteIncrementaSoOContadorPosicionalEMmapSoOMapeado(@TempDir Path dir) {
        try (MappedShard pwrite = MappedShard.create(dir.resolve("pwrite.blob"), superblock(0, SEG), SEG,
                VolumeWriteMode.PWRITE);
             MappedShard mmap = MappedShard.create(dir.resolve("mmap.blob"), superblock(0, SEG), SEG,
                     VolumeWriteMode.MMAP)) {
            // a criação grava o superblock pelo mesmo caminho do modo
            assertEquals(0L, pwrite.mappedBytesWritten());
            assertEquals(ShardSuperblock.BYTES, pwrite.positionalBytesWritten());
            assertEquals(0L, mmap.positionalBytesWritten());
            assertEquals(ShardSuperblock.BYTES, mmap.mappedBytesWritten());

            pwrite.writeAt(8192L, pattern(100, 1));
            mmap.writeAt(8192L, pattern(100, 1));

            assertEquals(0L, pwrite.mappedBytesWritten());
            assertEquals(ShardSuperblock.BYTES + 100, pwrite.positionalBytesWritten());
            assertEquals(pwrite.positionalBytesWritten(), pwrite.bytesWritten());
            assertEquals(0L, mmap.positionalBytesWritten());
            assertEquals(ShardSuperblock.BYTES + 100, mmap.mappedBytesWritten());
            assertEquals(mmap.mappedBytesWritten(), mmap.bytesWritten());
        }
    }

    @Test
    void escritaPosicionalGrandeCruzandoSegmentosChegaIntegra(@TempDir Path dir) {
        long capacity = 4 * SEG;
        byte[] big = pattern((int) (SEG + 123), 17); // maior que um segmento: cruza a fronteira
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, capacity), SEG,
                VolumeWriteMode.PWRITE)) {
            shard.writeAt(2 * SEG - 70_000, big);
            assertArrayEquals(big, shard.readAt(2 * SEG - 70_000, big.length));
            assertEquals(0L, shard.mappedBytesWritten());
        }
    }

    @Test
    void closeDoShardEIdempotenteEAposFecharAEscritaPosicionalFalha(@TempDir Path dir) {
        MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(0, SEG), SEG,
                VolumeWriteMode.PWRITE);
        shard.close();
        shard.close();
        assertThrows(BlobVolumeException.class, () -> shard.writeAt(8192L, pattern(10, 1)));
        assertThrows(BlobVolumeException.class, () -> shard.grow(2 * SEG));
    }
}
