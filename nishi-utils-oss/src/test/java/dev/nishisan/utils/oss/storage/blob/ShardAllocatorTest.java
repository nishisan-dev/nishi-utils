package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Alocador por shard (ver doc/oss/ngrrd-blob-volume.md §9). */
class ShardAllocatorTest {

    private static final long HEADER = 4096L;
    private static final long BIG_SEGMENT = 1L << 30;
    private static final long BIG_CAPACITY = 1L << 40;

    private static ShardAllocator fresh(long segmentBytes, long capacity) {
        return ShardAllocator.fresh(HEADER, segmentBytes, capacity);
    }

    @Test
    void bumpAllocatesContiguouslyFromHeader() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, BIG_CAPACITY);
        assertEquals(4096L, alloc.allocate(8192L).getAsLong());
        assertEquals(12288L, alloc.allocate(8192L).getAsLong());
        assertEquals(20480L, alloc.allocate(4096L).getAsLong());
        assertEquals(24576L, alloc.bumpCursor());
    }

    @Test
    void alignsRegionSizeToPage() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, BIG_CAPACITY);
        assertEquals(4096L, alloc.allocate(5000L).getAsLong()); // 5000 -> 8192
        assertEquals(12288L, alloc.bumpCursor());
    }

    @Test
    void reusesFreedSlotOfSameSizeClass() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, BIG_CAPACITY);
        long a = alloc.allocate(8192L).getAsLong(); // 4096
        alloc.allocate(8192L); // 12288
        alloc.free(a, 8192L);
        assertEquals(a, alloc.allocate(8192L).getAsLong()); // reusa o slot liberado
    }

    @Test
    void freedSlotNotReusedForDifferentSizeClass() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, BIG_CAPACITY);
        long a = alloc.allocate(8192L).getAsLong(); // 4096; bump -> 12288
        alloc.free(a, 8192L);
        long b = alloc.allocate(16384L).getAsLong(); // size-class diferente -> bump, não reusa o slot 4096
        assertEquals(12288L, b);
        assertEquals(4096L, a); // slot liberado continua reservado para a classe 8192
    }

    @Test
    void padsToAvoidCrossingSegmentBoundary() {
        // Segmento de 8192 bytes: uma região de 8192 em offset 4096 cruzaria a
        // fronteira; o alocador avança para o início do próximo segmento (8192).
        ShardAllocator alloc = fresh(8192L, BIG_CAPACITY);
        assertEquals(8192L, alloc.allocate(8192L).getAsLong());
        assertEquals(16384L, alloc.bumpCursor());
    }

    @Test
    void returnsEmptyWhenExceedsCapacity() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, 12288L);
        assertEquals(4096L, alloc.allocate(8192L).getAsLong());
        assertFalse(alloc.allocate(8192L).isPresent());
    }

    @Test
    void growEnablesAllocationAfterCapacityIncrease() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, 12288L);
        alloc.allocate(8192L);
        assertFalse(alloc.allocate(8192L).isPresent());
        alloc.grow(1L << 20);
        assertEquals(12288L, alloc.allocate(8192L).getAsLong());
    }

    @Test
    void rebuildFromCatalogRestoresBumpAndFreeList() {
        List<CatalogEntry> entries = List.of(
                new CatalogEntry("series/a.ngrr", 0, 4096L, 8192L, State.LIVE),
                new CatalogEntry("series/b.ngrr", 0, 12288L, 8192L, State.DELETED),
                new CatalogEntry("series/c.ngrr", 0, 20480L, 4096L, State.LIVE));
        ShardAllocator alloc = ShardAllocator.rebuild(HEADER, BIG_SEGMENT, BIG_CAPACITY, entries);
        assertEquals(24576L, alloc.bumpCursor()); // high-water = 20480 + 4096
        // O slot DELETED (12288, 8192) deve ser reaproveitado antes de qualquer bump.
        assertEquals(12288L, alloc.allocate(8192L).getAsLong());
        assertEquals(24576L, alloc.allocate(8192L).getAsLong()); // agora bump
    }

    @Test
    void freshAllocatorHasNoFreeSlots() {
        ShardAllocator alloc = fresh(BIG_SEGMENT, BIG_CAPACITY);
        OptionalLong first = alloc.allocate(8192L);
        assertTrue(first.isPresent());
        assertEquals(HEADER, first.getAsLong());
    }
}
