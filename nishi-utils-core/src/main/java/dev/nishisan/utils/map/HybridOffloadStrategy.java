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

import dev.nishisan.utils.stats.StatsUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hybrid offloading strategy that keeps hot entries in memory and spills
 * cold entries to disk based on a configurable {@link EvictionPolicy}.
 * <p>
 * When the number of in-memory entries in a given stripe exceeds its
 * per-stripe threshold, victim entries are selected according to the
 * active policy and serialized to disk. Subsequent {@code get()} calls
 * for evicted entries automatically deserialize from disk and promote
 * them back into memory (warm-up), potentially triggering another eviction.
 *
 * <h2>Concurrency model</h2>
 * Instead of a single global lock, the key space is partitioned into
 * {@value #CONCURRENCY_LEVEL} independent segments (stripes). Each stripe
 * maintains its own {@link LinkedHashMap} for hot entries and its own
 * {@link ReentrantReadWriteLock}. This allows multiple threads to operate
 * on disjoint key ranges without contention — particularly important when
 * disk I/O (eviction or warm-up) would otherwise block the entire map.
 * <p>
 * The LRU policy becomes <em>per-stripe approximate</em> rather than
 * globally strict — the same trade-off used by production caches like
 * Caffeine and Guava's {@code ConcurrentLinkedHashMap}.
 *
 * <h2>Usage</h2>
 * 
 * <pre>{@code
 * NMapConfig cfg = NMapConfig.builder()
 *         .mode(NMapPersistenceMode.DISABLED)
 *         .offloadStrategyFactory((baseDir, name) -> HybridOffloadStrategy.<String, String>builder(baseDir, name)
 *                 .evictionPolicy(EvictionPolicy.LRU)
 *                 .maxInMemoryEntries(10_000)
 *                 .build())
 *         .build();
 *
 * try (NMap<String, String> map = NMap.open(Path.of("/data"), "sessions", cfg)) {
 *     map.put("session-123", "user-data");
 * }
 * }</pre>
 *
 * <h2>Thread safety</h2>
 * All operations are protected by striped {@link ReentrantReadWriteLock}s,
 * making this strategy safe for highly concurrent access with significantly
 * reduced contention compared to a single global lock.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class HybridOffloadStrategy<K, V>
        implements NMapOffloadStrategy<K, V> {

    private static final Logger LOGGER = Logger.getLogger(HybridOffloadStrategy.class.getName());
    private static final String OFFLOAD_DIR = "hybrid-offload";
    private static final String ENTRY_SUFFIX = ".dat";

    /**
     * Number of lock stripes (must be a power of 2 for fast modular hashing).
     */
    private static final int CONCURRENCY_LEVEL = 16;

    private final Path offloadDir;
    private final int maxInMemoryEntries;
    private final int maxPerStripe;
    private final EvictionPolicy evictionPolicy;
    private final StatsUtils stats;

    /**
     * Per-stripe hot caches. Each stripe has its own {@link LinkedHashMap}
     * whose ordering depends on the active {@link EvictionPolicy}:
     * <ul>
     * <li>{@code LRU}: access-ordered</li>
     * <li>{@code SIZE_THRESHOLD}: insertion-ordered</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private final LinkedHashMap<K, V>[] hotCaches = new LinkedHashMap[CONCURRENCY_LEVEL];

    /**
     * Index of entries that have been offloaded to disk.
     * Key → file path on disk. Shared across all stripes but inherently
     * thread-safe ({@link ConcurrentHashMap}).
     */
    private final ConcurrentHashMap<K, Path> coldIndex = new ConcurrentHashMap<>();

    /**
     * Per-stripe read/write locks.
     */
    private final ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[CONCURRENCY_LEVEL];

    private HybridOffloadStrategy(Builder<K, V> builder) {
        this.offloadDir = builder.baseDir.resolve(builder.name).resolve(OFFLOAD_DIR);
        this.maxInMemoryEntries = builder.maxInMemoryEntries;
        this.maxPerStripe = Math.max(1, builder.maxInMemoryEntries / CONCURRENCY_LEVEL);
        this.evictionPolicy = builder.evictionPolicy;
        this.stats = builder.stats;

        boolean accessOrder = evictionPolicy == EvictionPolicy.LRU;
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            hotCaches[i] = new LinkedHashMap<>(16, 0.75f, accessOrder);
            locks[i] = new ReentrantReadWriteLock();
        }

        try {
            Files.createDirectories(offloadDir);
            loadColdIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize hybrid offload directory", e);
        }
    }

    // ── Stripe helpers ──────────────────────────────────────────────────

    private int stripeFor(Object key) {
        int h = key.hashCode();
        h ^= (h >>> 16);
        return h & (CONCURRENCY_LEVEL - 1);
    }

    private void lockAllWrite() {
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].writeLock().lock();
        }
    }

    private void unlockAllWrite() {
        for (int i = CONCURRENCY_LEVEL - 1; i >= 0; i--) {
            locks[i].writeLock().unlock();
        }
    }

    // ── Factory ─────────────────────────────────────────────────────────

    /**
     * Creates a new builder.
     *
     * @param baseDir the base directory for data storage
     * @param name    the map name (used as subdirectory)
     * @param <K>     the key type
     * @param <V>     the value type
     * @return the builder
     */
    public static <K, V> Builder<K, V> builder(Path baseDir, String name) {
        return new Builder<>(baseDir, name);
    }

    // ── NMapOffloadStrategy ─────────────────────────────────────────────

    @Override
    public V get(K key) {
        notifyHit(NMapMetrics.GET_HIT);
        int stripe = stripeFor(key);
        locks[stripe].writeLock().lock();
        try {
            // 1. Check hot cache (also touches for LRU)
            V value = hotCaches[stripe].get(key);
            if (value != null) {
                notifyHit(NMapMetrics.CACHE_HIT);
                return value;
            }

            // 2. Check cold index → warm-up
            Path path = coldIndex.get(key);
            if (path == null || !Files.exists(path)) {
                if (path != null) {
                    coldIndex.remove(key);
                }
                notifyHit(NMapMetrics.CACHE_MISS);
                return null;
            }
            try {
                long t0 = System.currentTimeMillis();
                value = deserializeValue(path);
                notifyLatency(NMapMetrics.DISK_READ_LATENCY, System.currentTimeMillis() - t0);
                // Promote to hot cache
                coldIndex.remove(key);
                deleteFileQuietly(path);
                hotCaches[stripe].put(key, value);
                notifyHit(NMapMetrics.CACHE_MISS);
                notifyHit(NMapMetrics.WARM_UP);
                evictIfNeeded(stripe);
                publishSizeGauges();
                return value;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to read offloaded entry: " + path, e);
                notifyHit(NMapMetrics.DISK_IO_ERROR);
                return null;
            }
        } finally {
            locks[stripe].writeLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        notifyHit(NMapMetrics.PUT);
        int stripe = stripeFor(key);
        locks[stripe].writeLock().lock();
        try {
            // Remove from cold if present
            Path coldPath = coldIndex.remove(key);
            if (coldPath != null) {
                deleteFileQuietly(coldPath);
            }

            V prev = hotCaches[stripe].put(key, value);
            evictIfNeeded(stripe);
            publishSizeGauges();
            return prev;
        } finally {
            locks[stripe].writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) {
        notifyHit(NMapMetrics.REMOVE);
        int stripe = stripeFor(key);
        locks[stripe].writeLock().lock();
        try {
            // Try hot cache first
            V prev = hotCaches[stripe].remove(key);
            if (prev != null) {
                publishSizeGauges();
                return prev;
            }

            // Try cold index
            Path path = coldIndex.remove(key);
            if (path != null) {
                try {
                    prev = deserializeValue(path);
                    deleteFileQuietly(path);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to read cold entry for remove", e);
                    notifyHit(NMapMetrics.DISK_IO_ERROR);
                    deleteFileQuietly(path);
                }
            }
            publishSizeGauges();
            return prev;
        } finally {
            locks[stripe].writeLock().unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        int stripe = stripeFor(key);
        locks[stripe].readLock().lock();
        try {
            return hotCaches[stripe].containsKey(key) || coldIndex.containsKey(key);
        } finally {
            locks[stripe].readLock().unlock();
        }
    }

    @Override
    public int size() {
        // Aggregate hot sizes across all stripes (dirty read — acceptable for
        // monitoring/gauges) plus the thread-safe coldIndex size.
        int hot = 0;
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].readLock().lock();
            try {
                hot += hotCaches[i].size();
            } finally {
                locks[i].readLock().unlock();
            }
        }
        return hot + coldIndex.size();
    }

    @Override
    public boolean isEmpty() {
        if (!coldIndex.isEmpty()) {
            return false;
        }
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].readLock().lock();
            try {
                if (!hotCaches[i].isEmpty()) {
                    return false;
                }
            } finally {
                locks[i].readLock().unlock();
            }
        }
        return true;
    }

    @Override
    public Set<K> keySet() {
        Set<K> keys = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].readLock().lock();
            try {
                keys.addAll(hotCaches[i].keySet());
            } finally {
                locks[i].readLock().unlock();
            }
        }
        keys.addAll(coldIndex.keySet());
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        // Snapshot-based to avoid holding locks during iteration
        return new AbstractSet<>() {
            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                Set<K> allKeys = keySet();
                Iterator<K> keys = allKeys.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return keys.hasNext();
                    }

                    @Override
                    public Map.Entry<K, V> next() {
                        K key = keys.next();
                        V value = get(key);
                        return new AbstractMap.SimpleImmutableEntry<>(key, value);
                    }
                };
            }

            @Override
            public int size() {
                return HybridOffloadStrategy.this.size();
            }
        };
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        Set<K> allKeys = keySet();
        allKeys.forEach(key -> {
            V value = get(key);
            if (value != null) {
                action.accept(key, value);
            }
        });
    }

    @Override
    public void clear() {
        lockAllWrite();
        try {
            for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
                hotCaches[i].clear();
            }
            for (Path path : coldIndex.values()) {
                deleteFileQuietly(path);
            }
            coldIndex.clear();
        } finally {
            unlockAllWrite();
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
                return HybridOffloadStrategy.this.put(key, value);
            }

            @Override
            public V remove(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return HybridOffloadStrategy.this.remove(k);
            }

            @Override
            public V get(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return HybridOffloadStrategy.this.get(k);
            }

            @Override
            public void clear() {
                HybridOffloadStrategy.this.clear();
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                HybridOffloadStrategy.this.putAll(m);
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return HybridOffloadStrategy.this.entrySet();
            }

            @Override
            public int size() {
                return HybridOffloadStrategy.this.size();
            }
        };
    }

    @Override
    public boolean isInherentlyPersistent() {
        return true;
    }

    @Override
    public void close() {
        lockAllWrite();
        try {
            // Flush all hot entries across all stripes to disk so state survives restart
            for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
                for (Map.Entry<K, V> entry : hotCaches[i].entrySet()) {
                    try {
                        Path path = pathForKey(entry.getKey());
                        serializeEntry(path, entry.getKey(), entry.getValue());
                        coldIndex.put(entry.getKey(), path);
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "Failed to flush hot entry on close", e);
                    }
                }
                hotCaches[i].clear();
            }
        } finally {
            unlockAllWrite();
        }
    }

    /**
     * Remove todas as entradas (hot e cold), libera recursos e apaga
     * recursivamente o diretório de offload híbrido ({@code offloadDir}).
     *
     * @throws IOException se ocorrer erro de I/O durante a remoção
     */
    @Override
    public void destroy() throws IOException {
        clear();
        // close() without flushing — data is already cleared
        lockAllWrite();
        try {
            for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
                hotCaches[i].clear();
            }
            coldIndex.clear();
        } finally {
            unlockAllWrite();
        }
        DiskOffloadStrategy.deleteDirectoryRecursively(offloadDir);
    }

    // ── Metrics ─────────────────────────────────────────────────────────

    /**
     * Returns the number of entries currently held in memory (across all stripes).
     *
     * @return the hot cache size
     */
    public int hotSize() {
        int total = 0;
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].readLock().lock();
            try {
                total += hotCaches[i].size();
            } finally {
                locks[i].readLock().unlock();
            }
        }
        return total;
    }

    /**
     * Returns the number of entries currently offloaded to disk.
     *
     * @return the cold index size
     */
    public int coldSize() {
        return coldIndex.size();
    }

    /**
     * Returns the configured maximum number of in-memory entries.
     *
     * @return the threshold
     */
    public int maxInMemoryEntries() {
        return maxInMemoryEntries;
    }

    /**
     * Returns the active eviction policy.
     *
     * @return the policy
     */
    public EvictionPolicy evictionPolicy() {
        return evictionPolicy;
    }

    // ── Eviction Logic ──────────────────────────────────────────────────

    /**
     * Evicts eldest entries from the specified stripe's hot cache to disk
     * if the per-stripe threshold is exceeded. Must be called under the
     * write lock for the given stripe.
     *
     * @param stripe the stripe index
     */
    private void evictIfNeeded(int stripe) {
        LinkedHashMap<K, V> hotCache = hotCaches[stripe];
        while (hotCache.size() > maxPerStripe) {
            // LinkedHashMap iteration order gives us the eldest entry
            Iterator<Map.Entry<K, V>> it = hotCache.entrySet().iterator();
            if (!it.hasNext()) {
                break;
            }
            Map.Entry<K, V> eldest = it.next();
            it.remove();

            // Serialize to disk
            Path path = pathForKey(eldest.getKey());
            try {
                long t0 = System.currentTimeMillis();
                serializeEntry(path, eldest.getKey(), eldest.getValue());
                notifyLatency(NMapMetrics.DISK_WRITE_LATENCY, System.currentTimeMillis() - t0);
                coldIndex.put(eldest.getKey(), path);
                notifyHit(NMapMetrics.EVICTION);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to offload entry to disk: " + eldest.getKey(), e);
                notifyHit(NMapMetrics.DISK_IO_ERROR);
            }
        }
    }

    // ── Disk I/O ────────────────────────────────────────────────────────

    /**
     * Generates a collision-free file path for a given key using a SHA-1
     * digest of the serialized key bytes. Unlike {@code hashCode()}, SHA-1
     * produces a 160-bit fingerprint that never collides in practice, ensuring
     * two distinct keys always map to different files.
     */
    private Path pathForKey(K key) {
        return offloadDir.resolve(keyHash(key) + ENTRY_SUFFIX);
    }

    /**
     * Computes the SHA-1 hex digest of the serialized key.
     */
    private String keyHash(K key) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(key);
            }
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(baos.toByteArray());
            return HexFormat.of().formatHex(digest);
        } catch (IOException | NoSuchAlgorithmException e) {
            // SHA-1 is always available per JDK spec; IO on byte array never fails
            throw new UncheckedIOException(new IOException("Failed to compute key hash", e));
        }
    }

    private void serializeEntry(Path path, K key, V value) throws IOException {
        Files.createDirectories(path.getParent());
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(path)))) {
            oos.writeObject(key);
            oos.writeObject(value);
            oos.flush();
        }
    }

    @SuppressWarnings("unchecked")
    private V deserializeValue(Path path) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            ois.readObject(); // skip key
            return (V) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize offloaded entry", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadColdIndex() throws IOException {
        if (!Files.isDirectory(offloadDir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(offloadDir, "*" + ENTRY_SUFFIX)) {
            for (Path path : stream) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new BufferedInputStream(Files.newInputStream(path)))) {
                    K key = (K) ois.readObject();
                    coldIndex.put(key, path);
                } catch (ClassNotFoundException | IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to load cold entry key: " + path, e);
                }
            }
        }
    }

    private void deleteFileQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete offloaded file: " + path, e);
        }
    }

    // ── Stats helpers ───────────────────────────────────────────────────

    private void notifyHit(String metric) {
        if (stats != null) {
            stats.notifyHitCounter(metric);
        }
    }

    private void notifyLatency(String metric, long ms) {
        if (stats != null) {
            stats.notifyAverageCounter(metric, ms);
        }
    }

    private void publishSizeGauges() {
        if (stats != null) {
            int hot = 0;
            for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
                hot += hotCaches[i].size();
            }
            stats.notifyCurrentValue(NMapMetrics.HOT_SIZE, (long) hot);
            stats.notifyCurrentValue(NMapMetrics.COLD_SIZE, (long) coldIndex.size());
            stats.notifyCurrentValue(NMapMetrics.TOTAL_SIZE, (long) (hot + coldIndex.size()));
        }
    }

    // ── Builder ─────────────────────────────────────────────────────────

    /**
     * Builder for {@link HybridOffloadStrategy}.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static final class Builder<K, V> {
        private final Path baseDir;
        private final String name;
        private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;
        private int maxInMemoryEntries = 10_000;
        private StatsUtils stats;

        private Builder(Path baseDir, String name) {
            this.baseDir = Objects.requireNonNull(baseDir, "baseDir");
            this.name = Objects.requireNonNull(name, "name");
        }

        /**
         * Enables observability by wiring a {@link StatsUtils} instance.
         * When set, the strategy emits hit counters, gauge values, and
         * latency averages using the keys defined in {@link NMapMetrics}.
         *
         * @param stats the stats instance (may be {@code null} to disable)
         * @return this builder
         */
        public Builder<K, V> stats(StatsUtils stats) {
            this.stats = stats;
            return this;
        }

        /**
         * Sets the eviction policy. Default: {@link EvictionPolicy#LRU}.
         *
         * @param policy the policy
         * @return this builder
         */
        public Builder<K, V> evictionPolicy(EvictionPolicy policy) {
            this.evictionPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Sets the maximum number of entries to keep in memory before
         * offloading to disk. Default: 10,000.
         * <p>
         * The effective per-stripe limit is
         * {@code Math.max(1, maxInMemoryEntries / CONCURRENCY_LEVEL)}.
         *
         * @param max the threshold
         * @return this builder
         */
        public Builder<K, V> maxInMemoryEntries(int max) {
            if (max <= 0) {
                throw new IllegalArgumentException("maxInMemoryEntries must be > 0");
            }
            this.maxInMemoryEntries = max;
            return this;
        }

        /**
         * Builds the strategy.
         *
         * @return the configured strategy
         */
        public HybridOffloadStrategy<K, V> build() {
            return new HybridOffloadStrategy<>(this);
        }
    }
}
