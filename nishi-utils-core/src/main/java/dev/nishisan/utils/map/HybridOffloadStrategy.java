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
 * When the number of in-memory entries exceeds {@code maxInMemoryEntries},
 * victim entries are selected according to the active policy and serialized
 * to disk. Subsequent {@code get()} calls for evicted entries automatically
 * deserialize from disk and promote them back into memory (warm-up),
 * potentially triggering another eviction.
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
 * All operations are protected by a {@link ReentrantReadWriteLock}, making
 * this strategy safe for concurrent access.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class HybridOffloadStrategy<K extends Serializable, V extends Serializable>
        implements NMapOffloadStrategy<K, V> {

    private static final Logger LOGGER = Logger.getLogger(HybridOffloadStrategy.class.getName());
    private static final String OFFLOAD_DIR = "hybrid-offload";
    private static final String ENTRY_SUFFIX = ".dat";

    private final Path offloadDir;
    private final int maxInMemoryEntries;
    private final EvictionPolicy evictionPolicy;
    private final StatsUtils stats;

    /**
     * In-memory cache. Depending on the eviction policy:
     * <ul>
     * <li>{@code LRU}: access-ordered {@code LinkedHashMap}</li>
     * <li>{@code SIZE_THRESHOLD}: insertion-ordered {@code LinkedHashMap}</li>
     * </ul>
     * The eldest entry is the eviction candidate in both cases.
     */
    private final LinkedHashMap<K, V> hotCache;

    /**
     * Index of entries that have been offloaded to disk.
     * Key → file path on disk.
     */
    private final ConcurrentHashMap<K, Path> coldIndex = new ConcurrentHashMap<>();

    /**
     * Guards {@code hotCache} mutations (not concurrent-safe by itself).
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private HybridOffloadStrategy(Builder<K, V> builder) {
        this.offloadDir = builder.baseDir.resolve(builder.name).resolve(OFFLOAD_DIR);
        this.maxInMemoryEntries = builder.maxInMemoryEntries;
        this.evictionPolicy = builder.evictionPolicy;
        this.stats = builder.stats;
        boolean accessOrder = evictionPolicy == EvictionPolicy.LRU;
        this.hotCache = new LinkedHashMap<>(16, 0.75f, accessOrder);

        try {
            Files.createDirectories(offloadDir);
            loadColdIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize hybrid offload directory", e);
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
    public static <K extends Serializable, V extends Serializable> Builder<K, V> builder(Path baseDir, String name) {
        return new Builder<>(baseDir, name);
    }

    // ── NMapOffloadStrategy ─────────────────────────────────────────────

    @Override
    public V get(K key) {
        notifyHit(NMapMetrics.GET_HIT);
        lock.writeLock().lock();
        try {
            // 1. Check hot cache (also touches for LRU)
            V value = hotCache.get(key);
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
                hotCache.put(key, value);
                notifyHit(NMapMetrics.CACHE_MISS);
                notifyHit(NMapMetrics.WARM_UP);
                evictIfNeeded();
                publishSizeGauges();
                return value;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to read offloaded entry: " + path, e);
                notifyHit(NMapMetrics.DISK_IO_ERROR);
                return null;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        notifyHit(NMapMetrics.PUT);
        lock.writeLock().lock();
        try {
            // Remove from cold if present
            Path coldPath = coldIndex.remove(key);
            if (coldPath != null) {
                deleteFileQuietly(coldPath);
            }

            V prev = hotCache.put(key, value);
            evictIfNeeded();
            publishSizeGauges();
            return prev;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) {
        notifyHit(NMapMetrics.REMOVE);
        lock.writeLock().lock();
        try {
            // Try hot cache first
            V prev = hotCache.remove(key);
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
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return hotCache.containsKey(key) || coldIndex.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return hotCache.size() + coldIndex.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return hotCache.isEmpty() && coldIndex.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        lock.readLock().lock();
        try {
            Set<K> keys = ConcurrentHashMap.newKeySet();
            keys.addAll(hotCache.keySet());
            keys.addAll(coldIndex.keySet());
            return Collections.unmodifiableSet(keys);
        } finally {
            lock.readLock().unlock();
        }
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
        lock.writeLock().lock();
        try {
            hotCache.clear();
            for (Path path : coldIndex.values()) {
                deleteFileQuietly(path);
            }
            coldIndex.clear();
        } finally {
            lock.writeLock().unlock();
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
        lock.writeLock().lock();
        try {
            // Flush all hot entries to disk so state survives restart
            for (Map.Entry<K, V> entry : hotCache.entrySet()) {
                try {
                    Path path = pathForKey(entry.getKey());
                    serializeEntry(path, entry.getKey(), entry.getValue());
                    coldIndex.put(entry.getKey(), path);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to flush hot entry on close", e);
                }
            }
            hotCache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Metrics ─────────────────────────────────────────────────────────

    /**
     * Returns the number of entries currently held in memory.
     *
     * @return the hot cache size
     */
    public int hotSize() {
        lock.readLock().lock();
        try {
            return hotCache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the number of entries currently offloaded to disk.
     *
     * @return the cold index size
     */
    public int coldSize() {
        lock.readLock().lock();
        try {
            return coldIndex.size();
        } finally {
            lock.readLock().unlock();
        }
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
     * Evicts eldest entries from the hot cache to disk if the threshold is
     * exceeded. Must be called under write lock.
     */
    private void evictIfNeeded() {
        while (hotCache.size() > maxInMemoryEntries) {
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
            stats.notifyCurrentValue(NMapMetrics.HOT_SIZE, (long) hotCache.size());
            stats.notifyCurrentValue(NMapMetrics.COLD_SIZE, (long) coldIndex.size());
            stats.notifyCurrentValue(NMapMetrics.TOTAL_SIZE, (long) (hotCache.size() + coldIndex.size()));
        }
    }

    // ── Builder ─────────────────────────────────────────────────────────

    /**
     * Builder for {@link HybridOffloadStrategy}.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static final class Builder<K extends Serializable, V extends Serializable> {
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
