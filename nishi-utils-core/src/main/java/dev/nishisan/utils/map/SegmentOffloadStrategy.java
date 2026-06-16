/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.map;

import dev.nishisan.utils.ngrid.cluster.transport.codec.Lz4FrameCompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Log-structured (Bitcask-style) disk offloading strategy for {@link NMap}.
 * <p>
 * Entries are appended to a small, fixed number of append-only segment logs
 * ({@code seg-NNN.log}) instead of one file per entry. A key is routed to a
 * segment by a stable SHA-1 hash of its serialized form, so the same key always
 * lands in the same segment across restarts. A global in-memory index maps each
 * key to the byte offset of its latest record; reads are a single positional
 * channel read. Updates append a new record (the previous one becomes garbage);
 * removals append a tombstone.
 * <p>
 * Compared to {@link DiskOffloadStrategy} (one file per entry), this collapses
 * the inode/file count from O(entries) to {@code numSegments}, eliminating the
 * inode-exhaustion and slow-directory-operation problems at scale.
 *
 * <h2>Thread safety</h2>
 * Each segment owns a {@link ReentrantReadWriteLock}, its append channel and a
 * bounded per-segment LRU hot cache. All mutations and reads for a key are
 * serialized on the owning segment's lock; unrelated segments proceed
 * concurrently. The key index is a {@link ConcurrentHashMap}.
 *
 * @param <K> the key type (must be {@link Serializable})
 * @param <V> the value type (must be {@link Serializable})
 */
public final class SegmentOffloadStrategy<K, V>
        implements NMapOffloadStrategy<K, V> {

    private static final Logger LOGGER = Logger.getLogger(SegmentOffloadStrategy.class.getName());

    private static final String OFFLOAD_DIR = "segment-offload";
    private static final String LOG_SUFFIX = ".log";
    private static final String HINT_SUFFIX = ".hint";
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String COMPACTING_SUFFIX = ".compacting";

    /** Hint file magic — ASCII "NMSH". */
    private static final int HINT_MAGIC = 0x4E4D5348;
    private static final byte HINT_VERSION = 1;
    private static final int HINT_HEADER_LEN = 4 + 1 + 8 + 4; // magic + version + coveredOffset + count

    /** Segments below this size are never compacted (avoids churn on tiny logs). */
    private static final long MIN_COMPACT_BYTES = 64 * 1024;

    /** Minimum number of segments. */
    static final int MIN_SEGMENTS = 1;
    /** Maximum number of segments. */
    static final int MAX_SEGMENTS = 128;

    private final Path offloadDir;
    private final int numSegments;
    private final boolean compressionEnabled;
    private final double compactionThreshold;
    private final boolean fsyncOnWrite;
    private final int maxPerSegment;

    /** Global key index: key → location of its latest record. */
    private final ConcurrentHashMap<K, EntryLocation> index = new ConcurrentHashMap<>();
    private final List<Segment> segments;
    private final ExecutorService compactionExecutor;

    /** Location of a record within a segment log. */
    private record EntryLocation(int segmentId, long offset, int recordLength) {
    }

    /**
     * Creates a segment offload strategy with the default knobs
     * ({@value #DEFAULT_NUM_SEGMENTS} segments, no compression, default hot
     * cache, default compaction threshold, no fsync).
     *
     * @param baseDir the base directory for data storage
     * @param name    the map name (used as subdirectory)
     */
    public SegmentOffloadStrategy(Path baseDir, String name) {
        this(baseDir, name, DEFAULT_NUM_SEGMENTS, false,
                DiskOffloadStrategy.DEFAULT_HOT_CACHE_MAX_ENTRIES, DEFAULT_COMPACTION_THRESHOLD, false);
    }

    /** Default number of segments. */
    static final int DEFAULT_NUM_SEGMENTS = 16;
    /** Default fraction of dead bytes that triggers compaction. */
    static final double DEFAULT_COMPACTION_THRESHOLD = 0.5;

    /**
     * Creates a segment offload strategy.
     *
     * @param baseDir             the base directory for data storage
     * @param name                the map name (used as subdirectory)
     * @param numSegments         number of segment logs (1..128); must be fixed
     *                            across restarts for a given directory
     * @param compressionEnabled  whether values are LZ4-compressed on disk
     * @param hotCacheMaxEntries  upper bound on cached values ({@code 0} disables)
     * @param compactionThreshold dead-bytes fraction that triggers compaction
     *                            ({@code 0 < t <= 1})
     * @param fsyncOnWrite        whether to fsync each append (durability vs throughput)
     */
    public SegmentOffloadStrategy(Path baseDir, String name, int numSegments,
            boolean compressionEnabled, int hotCacheMaxEntries, double compactionThreshold,
            boolean fsyncOnWrite) {
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(name, "name");
        if (numSegments < MIN_SEGMENTS || numSegments > MAX_SEGMENTS) {
            throw new IllegalArgumentException(
                    "numSegments must be in [" + MIN_SEGMENTS + ", " + MAX_SEGMENTS + "], got " + numSegments);
        }
        if (hotCacheMaxEntries < 0) {
            throw new IllegalArgumentException("hotCacheMaxEntries must be >= 0");
        }
        if (compactionThreshold <= 0.0 || compactionThreshold > 1.0) {
            throw new IllegalArgumentException("compactionThreshold must be in (0, 1], got " + compactionThreshold);
        }
        this.offloadDir = baseDir.resolve(name).resolve(OFFLOAD_DIR);
        this.numSegments = numSegments;
        this.compressionEnabled = compressionEnabled;
        this.compactionThreshold = compactionThreshold;
        this.fsyncOnWrite = fsyncOnWrite;
        this.maxPerSegment = hotCacheMaxEntries == 0 ? 0 : Math.max(1, hotCacheMaxEntries / numSegments);
        this.segments = new ArrayList<>(numSegments);
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nmap-segment-compaction");
            t.setDaemon(true);
            return t;
        });
        try {
            Files.createDirectories(offloadDir);
            for (int i = 0; i < numSegments; i++) {
                Segment segment = new Segment(i);
                segment.recover();
                segments.add(segment);
            }
        } catch (IOException | RuntimeException e) {
            // Avoid leaking channels/executor if a segment fails to recover
            for (Segment seg : segments) {
                seg.closeSegment();
            }
            compactionExecutor.shutdownNow();
            if (e instanceof IOException io) {
                throw new UncheckedIOException("Failed to initialize segment offload directory", io);
            }
            throw (RuntimeException) e;
        }
    }

    // ── Routing ─────────────────────────────────────────────────────────

    /**
     * Routes a key to a segment using the top 32 bits of its stable SHA-1 hash.
     * Deterministic across restarts so a key always maps to the same segment.
     */
    private int segmentFor(K key) {
        String hash = OffloadLayout.keyHash(key);
        long prefix = Long.parseLong(hash.substring(0, 8), 16);
        return (int) (prefix % numSegments);
    }

    // ── NMapOffloadStrategy ─────────────────────────────────────────────

    @Override
    public V get(K key) {
        EntryLocation loc = index.get(key);
        if (loc == null) {
            return null;
        }
        Segment seg = segments.get(loc.segmentId());
        seg.lock.writeLock().lock();
        try {
            V cached = seg.cacheGet(key);
            if (cached != null) {
                return cached;
            }
            loc = index.get(key);
            if (loc == null) {
                return null;
            }
            V value = seg.readValue(loc);
            if (value != null) {
                seg.cachePut(key, value);
            }
            return value;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        Segment seg = segments.get(segmentFor(key));
        seg.lock.writeLock().lock();
        try {
            V previous = seg.currentValue(key);
            seg.append(key, value);
            seg.cachePut(key, value);
            return previous;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) {
        EntryLocation loc = index.get(key);
        if (loc == null) {
            return null;
        }
        Segment seg = segments.get(loc.segmentId());
        seg.lock.writeLock().lock();
        try {
            loc = index.get(key);
            if (loc == null) {
                return null;
            }
            V previous = seg.cacheGet(key);
            if (previous == null) {
                previous = seg.readValue(loc);
            }
            seg.appendTombstone(key, loc);
            seg.cacheRemove(key);
            return previous;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        return index.containsKey(key);
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(index.keySet());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                Iterator<K> keys = index.keySet().iterator();
                // Look-ahead iterator that skips keys whose value resolved to null
                // (concurrently removed), so it never emits a (key, null) entry.
                return new Iterator<>() {
                    private Map.Entry<K, V> nextEntry = advance();

                    private Map.Entry<K, V> advance() {
                        while (keys.hasNext()) {
                            K key = keys.next();
                            V value = get(key);
                            if (value != null) {
                                return new AbstractMap.SimpleImmutableEntry<>(key, value);
                            }
                        }
                        return null;
                    }

                    @Override
                    public boolean hasNext() {
                        return nextEntry != null;
                    }

                    @Override
                    public Map.Entry<K, V> next() {
                        if (nextEntry == null) {
                            throw new java.util.NoSuchElementException();
                        }
                        Map.Entry<K, V> current = nextEntry;
                        nextEntry = advance();
                        return current;
                    }
                };
            }

            @Override
            public int size() {
                return index.size();
            }
        };
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        index.keySet().forEach(key -> {
            V value = get(key);
            if (value != null) {
                action.accept(key, value);
            }
        });
    }

    @Override
    public void clear() {
        lockAll();
        try {
            index.clear();
            for (Segment seg : segments) {
                seg.clearSegment();
            }
        } finally {
            unlockAll();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        entries.forEach(this::put);
    }

    @Override
    public Map<K, V> asMap() {
        return new AbstractMap<>() {
            @Override
            public V put(K key, V value) {
                return SegmentOffloadStrategy.this.put(key, value);
            }

            @Override
            public V remove(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return SegmentOffloadStrategy.this.remove(k);
            }

            @Override
            public V get(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return SegmentOffloadStrategy.this.get(k);
            }

            @Override
            public void clear() {
                SegmentOffloadStrategy.this.clear();
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                SegmentOffloadStrategy.this.putAll(m);
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return SegmentOffloadStrategy.this.entrySet();
            }

            @Override
            public int size() {
                return SegmentOffloadStrategy.this.size();
            }
        };
    }

    @Override
    public boolean isInherentlyPersistent() {
        return true;
    }

    @Override
    public void close() {
        shutdownCompaction();
        lockAll();
        try {
            Map<Integer, Map<K, EntryLocation>> bySegment = groupIndexBySegment();
            for (Segment seg : segments) {
                try {
                    seg.writeHint(bySegment.getOrDefault(seg.id, Map.of()));
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to write hint on close for " + seg.logPath, e);
                }
                seg.closeSegment();
            }
        } finally {
            unlockAll();
        }
    }

    private void shutdownCompaction() {
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** Groups the current index entries by their segment id. */
    private Map<Integer, Map<K, EntryLocation>> groupIndexBySegment() {
        Map<Integer, Map<K, EntryLocation>> bySegment = new HashMap<>();
        for (Map.Entry<K, EntryLocation> e : index.entrySet()) {
            bySegment.computeIfAbsent(e.getValue().segmentId(), k -> new HashMap<>())
                    .put(e.getKey(), e.getValue());
        }
        return bySegment;
    }

    /**
     * Schedules a background compaction of the segment when its dead-bytes ratio
     * crosses the configured threshold. Caller holds the segment's write lock.
     */
    private void maybeCompact(Segment seg) {
        if (seg.compacting || seg.appendOffset < MIN_COMPACT_BYTES) {
            return;
        }
        if (seg.deadBytes < compactionThreshold * seg.appendOffset) {
            return;
        }
        seg.compacting = true;
        try {
            compactionExecutor.execute(seg::compact);
        } catch (RejectedExecutionException e) {
            seg.compacting = false; // executor shutting down — skip
        }
    }

    /**
     * Compacts the given segment synchronously. Package-private for tests.
     *
     * @param segmentId the segment index
     */
    void compactNow(int segmentId) {
        segments.get(segmentId).compact();
    }

    private static void atomicMove(Path src, Path dst) throws IOException {
        try {
            Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Remove todas as entradas do disco e da memória, libera recursos e apaga
     * recursivamente o diretório de offload ({@code segment-offload}).
     *
     * @throws IOException se ocorrer erro de I/O durante a remoção
     */
    @Override
    public void destroy() throws IOException {
        clear();
        close();
        DiskOffloadStrategy.deleteDirectoryRecursively(offloadDir);
    }

    // ── Lock helpers ────────────────────────────────────────────────────

    private void lockAll() {
        for (Segment seg : segments) {
            seg.lock.writeLock().lock();
        }
    }

    private void unlockAll() {
        for (int i = segments.size() - 1; i >= 0; i--) {
            segments.get(i).lock.writeLock().unlock();
        }
    }

    // ── Serialization helpers ───────────────────────────────────────────

    private static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
        }
        return baos.toByteArray();
    }

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }

    /**
     * Returns the raw value bytes ready to deserialize, decompressing with LZ4
     * when the record was stored compressed.
     */
    private byte[] decodeValueBytes(SegmentRecord.Decoded decoded) throws IOException {
        if (decoded.value() == null) {
            return null;
        }
        return decoded.isValueCompressed() ? Lz4FrameCompressor.unwrap(decoded.value()) : decoded.value();
    }

    // ── Segment ─────────────────────────────────────────────────────────

    /**
     * A single append-only segment log with its own channel, lock and hot cache.
     * The enclosing {@link #index} is the source of truth for which record is
     * live; {@link #deadBytes} tracks superseded/tombstone bytes for compaction.
     */
    private final class Segment {
        private final int id;
        private final Path logPath;
        private final Path hintPath;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final LinkedHashMap<K, V> hotCache;
        private FileChannel channel;
        private long appendOffset;
        private long deadBytes;
        private volatile boolean compacting;

        Segment(int id) {
            this.id = id;
            this.logPath = offloadDir.resolve(String.format("seg-%03d%s", id, LOG_SUFFIX));
            this.hintPath = offloadDir.resolve(String.format("seg-%03d%s", id, HINT_SUFFIX));
            this.hotCache = newHotCache(maxPerSegment);
        }

        void recover() throws IOException {
            // Remove leftovers from a compaction/hint write interrupted by a crash
            Files.deleteIfExists(offloadDir.resolve(logPath.getFileName().toString() + COMPACTING_SUFFIX));
            Files.deleteIfExists(offloadDir.resolve(hintPath.getFileName().toString() + TEMP_SUFFIX));
            this.channel = FileChannel.open(logPath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                replay();
            } catch (IOException | RuntimeException e) {
                closeSegment(); // avoid leaking the channel if replay fails
                throw e;
            }
        }

        private void replay() throws IOException {
            long size = channel.size();
            long[] hint = loadHint(size);
            long offset;
            if (hint != null) {
                offset = hint[0];
                deadBytes = hint[0] - hint[1]; // coveredOffset - liveBytes
            } else {
                offset = 0;
                deadBytes = 0;
            }
            while (true) {
                SegmentRecord.Decoded decoded = SegmentRecord.readAt(channel, offset, size);
                if (decoded == null) {
                    if (offset < size) {
                        LOGGER.log(Level.WARNING, "Discarding {0} corrupt/truncated tail byte(s) in {1}",
                                new Object[]{size - offset, logPath});
                    }
                    channel.truncate(offset);
                    break;
                }
                K key;
                try {
                    @SuppressWarnings("unchecked")
                    K k = (K) deserialize(decoded.key());
                    key = k;
                } catch (ClassNotFoundException | IOException e) {
                    LOGGER.log(Level.WARNING, "Skipping unreadable record in " + logPath, e);
                    deadBytes += decoded.recordLength(); // reclaimable orphan bytes
                    offset += decoded.recordLength();
                    continue;
                }
                if (decoded.isTombstone()) {
                    EntryLocation old = index.remove(key);
                    if (old != null && old.segmentId() == id) {
                        deadBytes += old.recordLength();
                    }
                    deadBytes += decoded.recordLength();
                } else {
                    EntryLocation old = index.put(key, new EntryLocation(id, offset, decoded.recordLength()));
                    if (old != null && old.segmentId() == id) {
                        deadBytes += old.recordLength();
                    }
                }
                offset += decoded.recordLength();
            }
            this.appendOffset = offset;
        }

        /** Appends a PUT record and updates the index. Caller holds the write lock. */
        void append(K key, V value) {
            try {
                byte[] keyBytes = serialize(key);
                byte[] rawValue = serialize(value);
                byte[] valueBytes;
                int flags;
                if (compressionEnabled) {
                    valueBytes = Lz4FrameCompressor.wrap(rawValue);
                    flags = SegmentRecord.FLAG_VALUE_LZ4;
                } else {
                    valueBytes = rawValue;
                    flags = 0;
                }
                byte[] record = SegmentRecord.encode(SegmentRecord.TYPE_PUT, flags, keyBytes, valueBytes);
                long offset = appendOffset;
                writeFully(channel, ByteBuffer.wrap(record), offset);
                if (fsyncOnWrite) {
                    channel.force(false);
                }
                appendOffset += record.length;
                EntryLocation old = index.put(key, new EntryLocation(id, offset, record.length));
                if (old != null) {
                    deadBytes += old.recordLength();
                }
                maybeCompact(this);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append record to " + logPath, e);
                throw new UncheckedIOException("Failed to append record", e);
            }
        }

        /** Appends a TOMBSTONE record and removes the key. Caller holds the write lock. */
        void appendTombstone(K key, EntryLocation current) {
            try {
                byte[] keyBytes = serialize(key);
                byte[] record = SegmentRecord.encode(SegmentRecord.TYPE_TOMBSTONE, 0, keyBytes, null);
                writeFully(channel, ByteBuffer.wrap(record), appendOffset);
                if (fsyncOnWrite) {
                    channel.force(false);
                }
                appendOffset += record.length;
                index.remove(key);
                deadBytes += current.recordLength() + record.length;
                maybeCompact(this);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append tombstone to " + logPath, e);
                throw new UncheckedIOException("Failed to append tombstone", e);
            }
        }

        /** Reads the current value from cache or disk. Caller holds the write lock. */
        V currentValue(K key) {
            V cached = cacheGet(key);
            if (cached != null) {
                return cached;
            }
            EntryLocation loc = index.get(key);
            if (loc == null || loc.segmentId() != id) {
                return null;
            }
            return readValue(loc);
        }

        /** Reads and deserializes the value at the given location. Caller holds the lock. */
        V readValue(EntryLocation loc) {
            try {
                ByteBuffer buf = ByteBuffer.allocate(loc.recordLength());
                if (!readFully(channel, buf, loc.offset())) {
                    return null;
                }
                SegmentRecord.Decoded decoded = SegmentRecord.decode(buf.array());
                if (decoded == null || decoded.isTombstone()) {
                    return null;
                }
                byte[] valueBytes = decodeValueBytes(decoded);
                if (valueBytes == null) {
                    return null;
                }
                @SuppressWarnings("unchecked")
                V value = (V) deserialize(valueBytes);
                return value;
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.log(Level.WARNING, "Failed to read record from " + logPath, e);
                return null;
            }
        }

        V cacheGet(K key) {
            return maxPerSegment == 0 ? null : hotCache.get(key);
        }

        void cachePut(K key, V value) {
            if (maxPerSegment != 0) {
                hotCache.put(key, value);
            }
        }

        void cacheRemove(K key) {
            if (maxPerSegment != 0) {
                hotCache.remove(key);
            }
        }

        void clearSegment() {
            hotCache.clear();
            deadBytes = 0;
            appendOffset = 0;
            compacting = false;
            try {
                channel.truncate(0);
                if (fsyncOnWrite) {
                    channel.force(true); // make the emptied log durable before dropping the hint
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to truncate segment " + logPath, e);
            }
            try {
                Files.deleteIfExists(hintPath);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete hint " + hintPath, e);
            }
        }

        void closeSegment() {
            hotCache.clear();
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close segment " + logPath, e);
            }
        }

        /**
         * Rewrites the segment keeping only the live records, reclaiming the
         * space taken by superseded entries and tombstones. Holds the segment's
         * write lock for the whole operation: other segments are unaffected, and
         * the swap is atomic (temp file + {@code ATOMIC_MOVE}). On any I/O error
         * the original segment is left intact.
         */
        void compact() {
            lock.writeLock().lock();
            try {
                doCompact();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Compaction failed for " + logPath, e);
            } finally {
                compacting = false;
                lock.writeLock().unlock();
            }
        }

        private void doCompact() throws IOException {
            if (appendOffset == 0 || deadBytes == 0) {
                return;
            }
            Path tempPath = offloadDir.resolve(logPath.getFileName().toString() + COMPACTING_SUFFIX);
            Map<K, EntryLocation> live = new HashMap<>();
            try {
                long tempOffset = 0;
                FileChannel temp = FileChannel.open(tempPath, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    for (Map.Entry<K, EntryLocation> e : index.entrySet()) {
                        EntryLocation loc = e.getValue();
                        if (loc.segmentId() != id) {
                            continue;
                        }
                        ByteBuffer buf = ByteBuffer.allocate(loc.recordLength());
                        if (!readFully(channel, buf, loc.offset())) {
                            continue;
                        }
                        buf.flip();
                        writeFully(temp, buf, tempOffset);
                        live.put(e.getKey(), new EntryLocation(id, tempOffset, loc.recordLength()));
                        tempOffset += loc.recordLength();
                    }
                    // Compaction is infrequent: make the compacted data durable before the swap.
                    temp.force(true);
                } finally {
                    temp.close();
                }
                // Invalidate the stale hint BEFORE swapping the log, so a crash in the
                // swap window leaves no hint and recovery safely replays the new log.
                Files.deleteIfExists(hintPath);
                atomicMove(tempPath, logPath);
                // Reopen the new log BEFORE closing the old channel: if the open
                // fails, the old channel still serves the (logically identical)
                // pre-compaction data via its now-renamed inode, keeping the
                // in-memory index consistent. A restart replays the new log.
                FileChannel previous = channel;
                channel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                appendOffset = tempOffset;
                deadBytes = 0;
                for (Map.Entry<K, EntryLocation> e : live.entrySet()) {
                    index.put(e.getKey(), e.getValue());
                }
                previous.close();
                writeHint(live);
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(tempPath); // never leave an orphan .compacting file
                throw e;
            }
        }

        /**
         * Writes the hint (index snapshot) file for fast startup. {@code live}
         * maps every live key of this segment to its current location. Caller
         * holds the segment's write lock.
         */
        void writeHint(Map<K, EntryLocation> live) throws IOException {
            // The hint is renamed durably; force the log first so coveredOffset
            // never references bytes that have not reached disk.
            if (channel != null && channel.isOpen()) {
                channel.force(false);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(HINT_MAGIC);
            out.writeByte(HINT_VERSION);
            out.writeLong(appendOffset);
            out.writeInt(live.size());
            for (Map.Entry<K, EntryLocation> e : live.entrySet()) {
                byte[] keyBytes = serialize(e.getKey());
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(e.getValue().offset());
                out.writeInt(e.getValue().recordLength());
            }
            out.flush();
            byte[] payload = baos.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(payload);
            ByteBuffer full = ByteBuffer.allocate(payload.length + 4);
            full.put(payload);
            full.putInt((int) crc.getValue());
            full.flip();
            Path tempHint = offloadDir.resolve(hintPath.getFileName().toString() + TEMP_SUFFIX);
            try (FileChannel hc = FileChannel.open(tempHint, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (full.hasRemaining()) {
                    hc.write(full);
                }
                if (fsyncOnWrite) {
                    hc.force(true);
                }
            }
            atomicMove(tempHint, hintPath);
        }

        /**
         * Loads the hint file into the global index. Returns
         * {@code [coveredOffset, liveBytes]} on success, or {@code null} if the
         * hint is absent, corrupt, or inconsistent with the current log size (in
         * which case the global index is left untouched and the caller falls back
         * to a full log replay).
         */
        long[] loadHint(long fileSize) {
            if (!Files.exists(hintPath)) {
                return null;
            }
            try {
                byte[] all = Files.readAllBytes(hintPath);
                if (all.length < HINT_HEADER_LEN + 4) {
                    return null;
                }
                CRC32 crc = new CRC32();
                crc.update(all, 0, all.length - 4);
                int storedCrc = ByteBuffer.wrap(all, all.length - 4, 4).getInt();
                if ((int) crc.getValue() != storedCrc) {
                    return null;
                }
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(all, 0, all.length - 4));
                if (in.readInt() != HINT_MAGIC || in.readByte() != HINT_VERSION) {
                    return null;
                }
                long coveredOffset = in.readLong();
                if (coveredOffset < 0 || coveredOffset > fileSize) {
                    return null;
                }
                int count = in.readInt();
                if (count < 0) {
                    return null;
                }
                Map<K, EntryLocation> loaded = new HashMap<>(Math.max(16, count * 2));
                long liveBytes = 0;
                for (int i = 0; i < count; i++) {
                    int keyLen = in.readInt();
                    if (keyLen <= 0 || keyLen > SegmentRecord.MAX_FIELD_LEN) {
                        return null;
                    }
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    long off = in.readLong();
                    int recordLength = in.readInt();
                    if (off < 0 || recordLength <= 0 || off + recordLength > coveredOffset) {
                        return null;
                    }
                    @SuppressWarnings("unchecked")
                    K key = (K) deserialize(keyBytes);
                    loaded.put(key, new EntryLocation(id, off, recordLength));
                    liveBytes += recordLength;
                }
                index.putAll(loaded);
                return new long[]{coveredOffset, liveBytes};
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.log(Level.WARNING, "Ignoring unreadable hint " + hintPath + "; replaying log", e);
                return null;
            }
        }
    }

    private static <K, V> LinkedHashMap<K, V> newHotCache(int capacity) {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return capacity != 0 && size() > capacity;
            }
        };
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.write(buf, p);
            p += n;
        }
    }

    private static boolean readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                return false;
            }
            p += n;
        }
        return true;
    }
}
