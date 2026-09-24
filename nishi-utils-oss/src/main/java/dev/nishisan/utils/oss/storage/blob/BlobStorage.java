package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;
import dev.nishisan.utils.oss.storage.blob.CatalogJournal.WalOp;
import dev.nishisan.utils.oss.storage.blob.CatalogJournal.WalRecord;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Backend "sharded blob": um filesystem virtual onde cada série é uma região de
 * tamanho fixo dentro de um de N shards pré-alocados, acessada via mmap. Mantém
 * apenas N file handles + segmentos mmap, independente do número de séries.
 *
 * <p>Implementa {@link NgrrdStorage} (objeto-inteiro) e {@link SeriesChannelProvider}
 * (região in-place), exatamente como {@code LocalDiskStorage}, de modo que o
 * writer/reader não mudam. Ver {@code doc/oss/ngrrd-blob-volume.md}.</p>
 */
public final class BlobStorage implements NgrrdStorage, SeriesChannelProvider, AutoCloseable {

    private static final long PAGE = 4096L;
    private static final String VOLUME_META = "volume.meta";
    private static final String CATALOG_BIN = "catalog.bin";
    private static final String CATALOG_TMP = "catalog.bin.tmp";
    private static final String CATALOG_WAL = "catalog.wal";
    private static final String CATALOG_WAL_OLD = "catalog.wal.old";

    /** Listener no-op padrão: evita guardas {@code != null} nos call-sites de telemetria. */
    private static final BlobVolumeMetricsListener NO_OP = new BlobVolumeMetricsListener() {
    };

    private final Path volumeDir;
    private final int shardCount;
    private final UUID volumeUuid;
    private final long segmentBytes;
    private final MappedShard[] shards;
    private final ShardAllocator[] allocators;
    private final ConcurrentHashMap<String, CatalogEntry> catalog;
    private final ReentrantLock structuralLock = new ReentrantLock();
    private final BlobVolumeMetricsListener volumeMetrics;

    private CatalogJournal journal;
    private long generation;
    private boolean closed;
    private long declaredCapacity;
    private long liveBytes;
    private java.util.function.LongSupplier usableSpace = () -> Long.MAX_VALUE;
    private final java.util.Map<String, Reservation> reservations = new java.util.HashMap<>();

    private record Reservation(String key, long objectBytes, long growthBytes, long physicalBytes) { }

    private BlobStorage(Path volumeDir, int shardCount, UUID volumeUuid, long segmentBytes,
                        MappedShard[] shards, ShardAllocator[] allocators,
                        ConcurrentHashMap<String, CatalogEntry> catalog, CatalogJournal journal, long generation,
                        BlobVolumeMetricsListener volumeMetrics) {
        this.volumeDir = volumeDir;
        this.shardCount = shardCount;
        this.volumeUuid = volumeUuid;
        this.segmentBytes = segmentBytes;
        this.shards = shards;
        this.allocators = allocators;
        this.catalog = catalog;
        this.liveBytes = catalog.values().stream().mapToLong(CatalogEntry::regionBytes).sum();
        this.journal = journal;
        this.generation = generation;
        this.volumeMetrics = volumeMetrics == null ? NO_OP : volumeMetrics;
    }

    /** Record interno do resultado de um checkpoint, para emitir a telemetria fora do lock. */
    private record CheckpointInfo(long durationMs, int entryCount) {
    }

    /** Região física de uma série dentro de um shard. */
    record Region(MappedShard shard, long offset, long length) {
    }

    // ---------------------------------------------------------------- lifecycle

    public static BlobStorage openOrCreate(Path volumeDir, int shardCount, long segmentBytes, long initialCapacity) {
        return openOrCreate(volumeDir, shardCount, segmentBytes, initialCapacity, NO_OP);
    }

    public static BlobStorage openOrCreate(Path volumeDir, int shardCount, long segmentBytes, long initialCapacity,
                                           BlobVolumeMetricsListener volumeMetrics) {
        Objects.requireNonNull(volumeDir, "volumeDir é obrigatório");
        if (Files.exists(volumeDir.resolve(VOLUME_META))) {
            BlobStorage bs = open(volumeDir, volumeMetrics);
            if (bs.shardCount != shardCount) {
                throw new BlobVolumeException("volume " + volumeDir + " tem shardCount=" + bs.shardCount
                        + " (≠ " + shardCount + "); resharding não é suportado");
            }
            if (bs.segmentBytes != segmentBytes) {
                throw new BlobVolumeException("volume " + volumeDir + " tem segmentBytes=" + bs.segmentBytes
                        + " (≠ " + segmentBytes + ")");
            }
            return bs;
        }
        return create(volumeDir, shardCount, segmentBytes, initialCapacity, volumeMetrics);
    }

    public static BlobStorage create(Path volumeDir, int shardCount, long segmentBytes, long initialCapacity) {
        return create(volumeDir, shardCount, segmentBytes, initialCapacity, NO_OP);
    }

    public static BlobStorage create(Path volumeDir, int shardCount, long segmentBytes, long initialCapacity,
                                     BlobVolumeMetricsListener volumeMetrics) {
        if (shardCount <= 0) {
            throw new BlobVolumeException("shardCount deve ser > 0: " + shardCount);
        }
        if (segmentBytes < PAGE || segmentBytes % PAGE != 0) {
            throw new BlobVolumeException("segmentBytes deve ser múltiplo de " + PAGE + ": " + segmentBytes);
        }
        long capacity = roundUp(Math.max(initialCapacity, segmentBytes), segmentBytes);
        UUID uuid = UUID.randomUUID();
        long gen = 0L;
        try {
            Files.createDirectories(volumeDir);
            VolumeMetadata meta = new VolumeMetadata(VolumeMetadata.FORMAT_VERSION, shardCount, uuid,
                    segmentBytes, BlobRouting.ALGORITHM_SHA256_PREFIX64, BlobRouting.ROUTING_VERSION, gen);
            writeFileSync(volumeDir.resolve(VOLUME_META), meta.encode());

            MappedShard[] shards = new MappedShard[shardCount];
            ShardAllocator[] allocators = new ShardAllocator[shardCount];
            for (int i = 0; i < shardCount; i++) {
                ShardSuperblock sb = new ShardSuperblock(ShardSuperblock.FORMAT_VERSION, i, shardCount,
                        ShardSuperblock.HEADER_BYTES, capacity, ShardSuperblock.HEADER_BYTES, uuid, gen);
                shards[i] = MappedShard.create(shardFile(volumeDir, i, shardCount), sb, segmentBytes);
                allocators[i] = ShardAllocator.fresh(ShardSuperblock.HEADER_BYTES, segmentBytes, capacity);
            }
            writeFileSync(volumeDir.resolve(CATALOG_BIN),
                    BlobCatalogCodec.encode(shardCount, uuid, gen, List.of()));
            CatalogJournal journal = new CatalogJournal(volumeDir.resolve(CATALOG_WAL));
            return new BlobStorage(volumeDir, shardCount, uuid, segmentBytes, shards, allocators,
                    new ConcurrentHashMap<>(), journal, gen, volumeMetrics);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao criar volume " + volumeDir, e);
        }
    }

    public static BlobStorage open(Path volumeDir) {
        return open(volumeDir, NO_OP);
    }

    public static BlobStorage open(Path volumeDir, BlobVolumeMetricsListener volumeMetrics) {
        try {
            VolumeMetadata meta = VolumeMetadata.decode(Files.readAllBytes(volumeDir.resolve(VOLUME_META)));
            int shardCount = meta.shardCount();
            long segmentBytes = meta.segmentBytes();
            UUID uuid = meta.volumeUuid();

            MappedShard[] shards = new MappedShard[shardCount];
            for (int i = 0; i < shardCount; i++) {
                MappedShard shard = MappedShard.open(shardFile(volumeDir, i, shardCount), segmentBytes);
                ShardSuperblock sb = shard.superblock();
                if (sb.shardId() != i || sb.shardCount() != shardCount || !sb.volumeUuid().equals(uuid)) {
                    throw new BlobVolumeException("shard " + i + " inconsistente com volume.meta");
                }
                shards[i] = shard;
            }

            ConcurrentHashMap<String, CatalogEntry> catalog = new ConcurrentHashMap<>();
            long gen = meta.generation();
            Path catalogBin = volumeDir.resolve(CATALOG_BIN);
            if (Files.exists(catalogBin)) {
                BlobCatalogCodec.Snapshot snap = BlobCatalogCodec.decode(Files.readAllBytes(catalogBin));
                if (snap.shardCount() != shardCount || !snap.volumeUuid().equals(uuid)) {
                    throw new BlobVolumeException("catalog.bin inconsistente com volume.meta");
                }
                gen = Math.max(gen, snap.generation());
                for (CatalogEntry e : snap.entries()) {
                    catalog.put(e.key(), e);
                }
            }

            List<WalRecord> recs = new ArrayList<>();
            recs.addAll(CatalogJournal.replay(volumeDir.resolve(CATALOG_WAL_OLD)));
            recs.addAll(CatalogJournal.replay(volumeDir.resolve(CATALOG_WAL)));
            for (WalRecord r : recs) {
                gen = Math.max(gen, r.generation());
                if (r.op() == WalOp.ALLOC) {
                    catalog.put(r.entry().key(), r.entry());
                } else {
                    catalog.remove(r.entry().key());
                }
            }
            Files.deleteIfExists(volumeDir.resolve(CATALOG_WAL_OLD));

            ShardAllocator[] allocators = new ShardAllocator[shardCount];
            for (int i = 0; i < shardCount; i++) {
                final int shardId = i;
                List<CatalogEntry> live = catalog.values().stream()
                        .filter(e -> e.shardId() == shardId)
                        .collect(Collectors.toList());
                allocators[i] = ShardAllocator.rebuild(ShardSuperblock.HEADER_BYTES, segmentBytes,
                        shards[i].capacity(), live);
            }
            CatalogJournal journal = new CatalogJournal(volumeDir.resolve(CATALOG_WAL));
            return new BlobStorage(volumeDir, shardCount, uuid, segmentBytes, shards, allocators, catalog, journal,
                    gen, volumeMetrics);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao abrir volume " + volumeDir, e);
        }
    }

    // ---------------------------------------------------------------- alocação

    /** Enables declared-capacity and filesystem admission for this volume. */
    public void configureCapacity(long capacityBytes) {
        configureCapacity(capacityBytes, () -> {
            try {
                return Files.getFileStore(volumeDir).getUsableSpace();
            } catch (IOException e) {
                throw new BlobVolumeException("cannot inspect usable space for " + volumeDir, e);
            }
        });
    }

    /** Same admission policy with an injectable filesystem gauge. */
    public void configureCapacity(long capacityBytes, java.util.function.LongSupplier usableSpace) {
        structuralLock.lock();
        try {
            this.declaredCapacity = capacityBytes;
            this.usableSpace = Objects.requireNonNull(usableSpace, "usableSpace");
        } finally {
            structuralLock.unlock();
        }
    }

    /** Reserves a destination allocation without changing its data or catalog. Idempotent by id. */
    public void reserve(String id, String key, long objectBytes) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(key, "key");
        structuralLock.lock();
        try {
            Reservation old = reservations.get(id);
            if (old != null) {
                if (!old.key().equals(key) || old.objectBytes() != objectBytes) {
                    throw new BlobCapacityException("reservation identity mismatch: " + id);
                }
                return;
            }
            checkKeyReservation(key, null);
            long region = alignedRegionBytes(objectBytes);
            if (region > segmentBytes) {
                throw new BlobCapacityException("series exceeds segmentBytes");
            }
            CatalogEntry existing = catalog.get(key);
            long growth = Math.max(0, region - (existing == null ? 0 : existing.regionBytes()));
            long physical = existing != null && existing.regionBytes() == region ? 0 : region;
            checkAdmission(growth, physical, null, true);
            reservations.put(id, new Reservation(key, objectBytes, growth, physical));
        } finally {
            structuralLock.unlock();
        }
    }

    /** Releases an unused reservation; repeated release is harmless. */
    public void releaseReservation(String id) {
        structuralLock.lock();
        try {
            reservations.remove(id);
        } finally {
            structuralLock.unlock();
        }
    }

    /** Outstanding logical bytes, used by the node status reporter. */
    public long reservedBytes() {
        structuralLock.lock();
        try {
            return reservations.values().stream().mapToLong(Reservation::growthBytes).sum();
        } finally {
            structuralLock.unlock();
        }
    }

    private void checkKeyReservation(String key, String reservationId) {
        for (var entry : reservations.entrySet()) {
            if (entry.getValue().key().equals(key) && !entry.getKey().equals(reservationId)) {
                throw new BlobCapacityException("series has a migration reservation: " + key);
            }
        }
    }

    private void checkAdmission(long growth, long physical, String excluding, boolean migration) {
        long reserved = 0;
        long reservedPhysical = 0;
        for (var entry : reservations.entrySet()) {
            if (!entry.getKey().equals(excluding)) {
                reserved = Math.addExact(reserved, entry.getValue().growthBytes());
                reservedPhysical = Math.addExact(reservedPhysical, entry.getValue().physicalBytes());
            }
        }
        if ((growth > 0 || migration) && !CapacityBudget.fits(declaredCapacity, liveBytes, reserved, growth)) {
            throw new BlobCapacityException("CAPACITY_EXCEEDED used=" + liveBytes + " reserved=" + reserved
                    + " additional=" + growth + " limit=" + CapacityBudget.limit(declaredCapacity));
        }
        if (physical > 0) {
            long free = usableSpace.getAsLong();
            if (reservedPhysical > free || physical > free - reservedPhysical) {
                throw new BlobCapacityException("FILESYSTEM_CAPACITY_EXCEEDED available=" + free);
            }
        }
    }

    /** Size of a blob allocation, including page alignment. */
    public static long alignedRegionBytes(long objectBytes) {
        if (objectBytes <= 0 || objectBytes > Long.MAX_VALUE - (PAGE - 1)) {
            throw new IllegalArgumentException("invalid object size: " + objectBytes);
        }
        return alignToPage(objectBytes);
    }

    /** Reads only the immutable header/dictionaries, never the time-series rings. */
    public Optional<byte[]> seriesStaticSection(String key) {
        structuralLock.lock();
        try {
            CatalogEntry entry = catalog.get(key);
            if (entry == null) {
                return Optional.empty();
            }
            var codecHeader = dev.nishisan.utils.oss.format.SeriesFileCodec.decodeFixedHeader(
                    shards[entry.shardId()].readAt(entry.regionOffset(),
                            dev.nishisan.utils.oss.format.SeriesFileCodec.FIXED_HEADER_BYTES));
            if (codecHeader.staticSectionBytes() > entry.objectBytes()
                    || codecHeader.staticSectionBytes() > Integer.MAX_VALUE) {
                throw new BlobVolumeException("invalid static section size: " + key);
            }
            return Optional.of(shards[entry.shardId()].readAt(entry.regionOffset(),
                    (int) codecHeader.staticSectionBytes()));
        } finally {
            structuralLock.unlock();
        }
    }

    /** Aloca (ou reaproveita) a região da série {@code key}, ajustando o tamanho se mudou. */
    Region allocateRegion(String key, long totalBytes) {
        return allocateRegion(key, totalBytes, null);
    }

    private Region allocateRegion(String key, long totalBytes, String reservationId) {
        structuralLock.lock();
        try {
            checkKeyReservation(key, reservationId);
            Reservation reservation = reservationId == null ? null : reservations.get(reservationId);
            if (reservationId != null && (reservation == null || !reservation.key().equals(key)
                    || reservation.objectBytes() != totalBytes)) {
                throw new BlobCapacityException("missing or mismatching migration reservation");
            }
            long regionBytes = alignedRegionBytes(totalBytes);
            if (regionBytes > segmentBytes) {
                throw new BlobVolumeException("objeto (" + totalBytes + " bytes) maior que o segmento ("
                        + segmentBytes + "); aumente segmentBytes do volume");
            }
            CatalogEntry existing = catalog.get(key);
            long growth = Math.max(0, regionBytes - (existing == null ? 0 : existing.regionBytes()));
            long physical = existing != null && existing.regionBytes() == regionBytes ? 0 : regionBytes;
            checkAdmission(growth, physical, reservationId, reservationId != null);
            if (existing != null && existing.regionBytes() == regionBytes) {
                if (existing.objectBytes() != totalBytes) {
                    CatalogEntry updated = new CatalogEntry(key, existing.shardId(), existing.regionOffset(),
                            regionBytes, totalBytes, State.LIVE);
                    journal.appendAlloc(++generation, updated);
                    catalog.put(key, updated);
                }
                reservations.remove(reservationId);
                return new Region(shards[existing.shardId()], existing.regionOffset(), regionBytes);
            }
            if (existing != null) {
                allocators[existing.shardId()].free(existing.regionOffset(), existing.regionBytes());
                journal.appendFree(++generation, existing);
                catalog.remove(key);
                liveBytes -= existing.regionBytes();
                volumeMetrics.onRegionFree(existing.shardId(), existing.regionBytes());
            }
            int shardId = BlobRouting.shardFor(key, shardCount);
            long offset = allocateInShard(shardId, regionBytes);
            CatalogEntry entry = new CatalogEntry(key, shardId, offset, regionBytes, totalBytes, State.LIVE);
            journal.appendAlloc(++generation, entry);
            catalog.put(key, entry);
            liveBytes += regionBytes;
            reservations.remove(reservationId);
            volumeMetrics.onRegionAllocate(shardId, regionBytes);
            return new Region(shards[shardId], offset, regionBytes);
        } finally {
            structuralLock.unlock();
        }
    }

    private long allocateInShard(int shardId, long regionBytes) {
        ShardAllocator alloc = allocators[shardId];
        OptionalLong offset = alloc.allocate(regionBytes);
        while (offset.isEmpty()) {
            MappedShard shard = shards[shardId];
            long oldCapacity = shard.capacity();
            shard.grow(oldCapacity + segmentBytes); // setLength + force + map (durável)
            persistSuperblock(shardId); // capacidade durável ANTES de jornalizar a alocação (§9)
            long newCapacity = shard.capacity();
            alloc.grow(newCapacity);
            if (newCapacity > oldCapacity) {
                volumeMetrics.onShardGrow(shardId, oldCapacity, newCapacity);
            }
            offset = alloc.allocate(regionBytes);
        }
        return offset.getAsLong();
    }

    private void persistSuperblock(int shardId) {
        MappedShard shard = shards[shardId];
        ShardSuperblock sb = new ShardSuperblock(ShardSuperblock.FORMAT_VERSION, shardId, shardCount,
                ShardSuperblock.HEADER_BYTES, shard.capacity(), allocators[shardId].bumpCursor(),
                volumeUuid, ++generation);
        shard.writeSuperblock(sb);
    }

    // ---------------------------------------------------------------- SeriesChannelProvider

    @Override
    public SeriesChannel openSeries(String key) {
        CatalogEntry e = catalog.get(key);
        if (e == null) {
            return new BlobSeriesChannel(this, key);
        }
        return new BlobSeriesChannel(this, key, shards[e.shardId()], e.regionOffset(), e.regionBytes());
    }

    @Override
    public boolean seriesExists(String key) {
        return catalog.containsKey(key);
    }

    // ---------------------------------------------------------------- NgrrdStorage

    @Override
    public void put(String key, byte[] data) {
        writeObject(key, data);
    }

    @Override
    public void atomicReplace(String key, byte[] data) {
        writeObject(key, data);
    }

    /** Installs bytes using a previously accepted migration reservation. */
    public void atomicReplaceReserved(String key, byte[] data, String reservationId) {
        Region region = allocateRegion(key, data.length, reservationId);
        region.shard().writeAt(region.offset(), data);
        region.shard().forceRange(region.offset(), data.length);
    }

    private void writeObject(String key, byte[] data) {
        Region region = allocateRegion(key, data.length);
        region.shard().writeAt(region.offset(), data);
        region.shard().forceRange(region.offset(), data.length);
    }

    @Override
    public Optional<byte[]> get(String key) {
        CatalogEntry e = catalog.get(key);
        if (e == null) {
            return Optional.empty();
        }
        return Optional.of(shards[e.shardId()].readAt(e.regionOffset(), (int) e.objectBytes()));
    }

    @Override
    public boolean exists(String key) {
        return catalog.containsKey(key);
    }

    @Override
    public void delete(String key) {
        structuralLock.lock();
        try {
            CatalogEntry e = catalog.remove(key);
            if (e != null) {
                liveBytes -= e.regionBytes();
                allocators[e.shardId()].free(e.regionOffset(), e.regionBytes());
                journal.appendFree(++generation, e);
                volumeMetrics.onRegionFree(e.shardId(), e.regionBytes());
            }
        } finally {
            structuralLock.unlock();
        }
    }

    @Override
    public List<String> list(String prefix) {
        return catalog.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .sorted()
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------- checkpoint/close

    /** Escreve um snapshot do catálogo e rotaciona o WAL (limita o tamanho do WAL). */
    public void checkpoint() {
        CheckpointInfo info;
        structuralLock.lock();
        try {
            info = writeCheckpointLocked();
        } finally {
            structuralLock.unlock();
        }
        // Telemetria fora do lock: nunca segura o structuralLock durante código de usuário.
        volumeMetrics.onCheckpoint(info.durationMs(), info.entryCount());
    }

    /**
     * Materializa o snapshot do catálogo e rotaciona o WAL. Pré-condição: chamado
     * segurando o {@code structuralLock}. Não emite telemetria (o chamador emite
     * {@code onCheckpoint} após liberar o lock).
     */
    private CheckpointInfo writeCheckpointLocked() {
        long startNanos = System.nanoTime();
        try {
            long gen = ++generation;
            List<CatalogEntry> live = new ArrayList<>(catalog.values());
            byte[] image = BlobCatalogCodec.encode(shardCount, volumeUuid, gen, live);
            Path tmp = volumeDir.resolve(CATALOG_TMP);
            Path bin = volumeDir.resolve(CATALOG_BIN);
            writeFileSync(tmp, image);
            atomicMove(tmp, bin);
            journal.close();
            Path wal = volumeDir.resolve(CATALOG_WAL);
            Path walOld = volumeDir.resolve(CATALOG_WAL_OLD);
            if (Files.exists(wal)) {
                atomicMove(wal, walOld);
            }
            journal = new CatalogJournal(wal);
            Files.deleteIfExists(walOld);
            long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
            return new CheckpointInfo(durationMs, live.size());
        } catch (IOException e) {
            throw new BlobVolumeException("falha no checkpoint do volume " + volumeDir, e);
        }
    }

    @Override
    public void close() {
        CheckpointInfo info = null;
        structuralLock.lock();
        try {
            if (closed) {
                return;
            }
            info = writeCheckpointLocked();
            journal.close();
            for (MappedShard shard : shards) {
                shard.close();
            }
            closed = true;
        } finally {
            structuralLock.unlock();
        }
        // close() também dispara onCheckpoint (checkpoint implícito de shutdown), fora do lock.
        if (info != null) {
            volumeMetrics.onCheckpoint(info.durationMs(), info.entryCount());
        }
    }

    public int shardCount() {
        return shardCount;
    }

    /**
     * Snapshot dos gauges operacionais do volume (modelo <em>pull</em>): uso líquido,
     * fill ratio e séries por shard, além do tamanho de catálogo e WAL. Lido sob o
     * {@code structuralLock} para uma fotografia consistente; destina-se a poll de
     * baixa frequência. Ver {@link BlobVolumeStats}.
     */
    public BlobVolumeStats stats() {
        structuralLock.lock();
        try {
            long[] capacity = new long[shardCount];
            long[] used = new long[shardCount];
            long[] series = new long[shardCount];
            for (int i = 0; i < shardCount; i++) {
                capacity[i] = shards[i].capacity();
            }
            for (CatalogEntry e : catalog.values()) {
                used[e.shardId()] += e.regionBytes();
                series[e.shardId()]++;
            }
            double[] fill = new double[shardCount];
            for (int i = 0; i < shardCount; i++) {
                fill[i] = capacity[i] > 0 ? (double) used[i] / capacity[i] : 0.0;
            }
            long catalogImageBytes;
            try {
                Path bin = volumeDir.resolve(CATALOG_BIN);
                catalogImageBytes = Files.exists(bin) ? Files.size(bin) : 0L;
            } catch (IOException ex) {
                throw new BlobVolumeException("falha ao ler o tamanho de " + CATALOG_BIN, ex);
            }
            return new BlobVolumeStats(shardCount, capacity, used, series, fill,
                    catalog.size(), catalogImageBytes, journal.size());
        } finally {
            structuralLock.unlock();
        }
    }

    // ---------------------------------------------------------------- helpers

    static long alignToPage(long x) {
        return ((x + PAGE - 1) / PAGE) * PAGE;
    }

    private static long roundUp(long value, long multiple) {
        return ((value + multiple - 1) / multiple) * multiple;
    }

    private static Path shardFile(Path volumeDir, int shardId, int shardCount) {
        int width = Math.max(2, Integer.toString(shardCount - 1).length());
        return volumeDir.resolve(String.format("shard-%0" + width + "d.blob", shardId));
    }

    private static void writeFileSync(Path path, byte[] data) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(data));
            ch.force(true);
        }
    }

    private static void atomicMove(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
