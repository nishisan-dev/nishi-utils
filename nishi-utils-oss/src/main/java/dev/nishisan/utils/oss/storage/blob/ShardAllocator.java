package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Alocador de regiões dentro de um shard: bump cursor + free-list por size-class
 * (geometria fixa ⇒ poucos tamanhos distintos). Reaproveita slots de tombstones
 * do mesmo tamanho; garante que nenhuma região cruze fronteira de segmento mmap.
 *
 * <p>Não é thread-safe: o uso é serializado pelo {@code structuralLock} do volume.
 * O estado é integralmente rederivável do catálogo via {@link #rebuild}. Ver
 * {@code doc/oss/ngrrd-blob-volume.md} §9.</p>
 */
public final class ShardAllocator {

    private static final long PAGE = 4096L;

    private final long headerBytes;
    private final long segmentBytes;
    private long capacity;
    private long bumpCursor;
    private final Map<Long, Deque<Long>> freeLists;

    private ShardAllocator(long headerBytes, long segmentBytes, long capacity,
                           long bumpCursor, Map<Long, Deque<Long>> freeLists) {
        this.headerBytes = headerBytes;
        this.segmentBytes = segmentBytes;
        this.capacity = capacity;
        this.bumpCursor = bumpCursor;
        this.freeLists = freeLists;
    }

    /** Alocador vazio (shard recém-criado). */
    public static ShardAllocator fresh(long headerBytes, long segmentBytes, long capacity) {
        return new ShardAllocator(headerBytes, segmentBytes, capacity, headerBytes, new HashMap<>());
    }

    /** Reconstrói o alocador a partir das entradas do catálogo deste shard. */
    public static ShardAllocator rebuild(long headerBytes, long segmentBytes, long capacity,
                                         List<CatalogEntry> shardEntries) {
        long bump = headerBytes;
        Map<Long, Deque<Long>> free = new HashMap<>();
        for (CatalogEntry e : shardEntries) {
            long end = e.regionOffset() + e.regionBytes();
            if (end > bump) {
                bump = end;
            }
            if (e.state() == State.DELETED) {
                free.computeIfAbsent(align(e.regionBytes()), k -> new ArrayDeque<>()).push(e.regionOffset());
            }
        }
        return new ShardAllocator(headerBytes, segmentBytes, capacity, bump, free);
    }

    /**
     * Aloca uma região de {@code regionBytes} (arredondado para página). Tenta
     * primeiro o free-list da size-class; senão faz bump (com padding para não
     * cruzar fronteira de segmento). Retorna vazio se não couber na capacidade
     * atual (o chamador decide crescer o shard ou tentar outro).
     */
    public OptionalLong allocate(long regionBytes) {
        long aligned = align(regionBytes);
        Deque<Long> free = freeLists.get(aligned);
        if (free != null && !free.isEmpty()) {
            return OptionalLong.of(free.pop());
        }
        long candidate = bumpCursor;
        if (segmentBytes > 0
                && Math.floorDiv(candidate, segmentBytes) != Math.floorDiv(candidate + aligned - 1, segmentBytes)) {
            candidate = (Math.floorDiv(candidate, segmentBytes) + 1) * segmentBytes;
        }
        if (candidate + aligned > capacity) {
            return OptionalLong.empty();
        }
        bumpCursor = candidate + aligned;
        return OptionalLong.of(candidate);
    }

    /** Devolve uma região ao free-list da sua size-class. */
    public void free(long offset, long regionBytes) {
        freeLists.computeIfAbsent(align(regionBytes), k -> new ArrayDeque<>()).push(offset);
    }

    /** Aumenta a capacidade após o shard ter crescido em disco. */
    public void grow(long newCapacity) {
        if (newCapacity > capacity) {
            this.capacity = newCapacity;
        }
    }

    public long bumpCursor() {
        return bumpCursor;
    }

    public long capacity() {
        return capacity;
    }

    private static long align(long x) {
        return ((x + PAGE - 1) / PAGE) * PAGE;
    }
}
