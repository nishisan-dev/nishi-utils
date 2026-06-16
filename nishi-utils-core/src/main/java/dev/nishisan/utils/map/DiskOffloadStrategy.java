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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Comparator;
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
 * Disk-backed offloading strategy for {@link NMap}.
 * <p>
 * Entries are serialized to individual files on disk under a dedicated
 * directory. A lightweight index ({@code ConcurrentHashMap<K, Path>}) maps
 * keys to their file locations, keeping heap usage minimal. A bounded,
 * per-stripe LRU hot cache avoids re-reads for recently accessed entries and
 * gives a predictable memory ceiling ({@code hotCacheMaxEntries}); set it to
 * {@code 0} to disable caching entirely.
 * <p>
 * <b>Thread safety:</b> All public operations are protected by a
 * striped {@link ReentrantReadWriteLock} scheme. Instead of a single
 * global lock, the key space is partitioned into {@value #CONCURRENCY_LEVEL}
 * independent segments (stripes). Each mutating operation acquires only the
 * lock for its stripe, allowing unrelated keys to be accessed concurrently
 * without contention.
 * <p>
 * <b>Trade-offs:</b>
 * <ul>
 * <li>Reads from cold entries incur disk I/O (deserialization).</li>
 * <li>Each entry is an individual file — suitable for maps with moderate
 * entry counts (&lt; 1M). For very large maps consider memory-mapped
 * strategies (future work).</li>
 * <li>Data is inherently persistent: closing and reopening with the same
 * path restores all entries. WAL-based persistence is redundant.</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class DiskOffloadStrategy<K, V>
        implements NMapOffloadStrategy<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DiskOffloadStrategy.class.getName());
    private static final String OFFLOAD_DIR = "offload";
    private static final String ENTRY_SUFFIX = ".dat";

    /**
     * Number of lock stripes (must be a power of 2 for fast modular hashing).
     */
    private static final int CONCURRENCY_LEVEL = 16;

    /** Default hot-cache ceiling when constructed without an explicit value. */
    static final int DEFAULT_HOT_CACHE_MAX_ENTRIES = 10_000;

    private final Path offloadDir;
    private final OffloadLayout layout;
    private final ConcurrentHashMap<K, Path> index = new ConcurrentHashMap<>();

    /**
     * Per-stripe access-ordered LRU hot caches, each capped at
     * {@link #maxPerStripe}. Accessed only under the owning stripe's lock.
     */
    private final LinkedHashMap<K, V>[] hotCaches;
    private final int maxPerStripe;
    private final ReentrantReadWriteLock[] locks;

    /**
     * Creates a new disk offload strategy with the default shard fan-out
     * ({@value OffloadLayout#DEFAULT_SHARD_DEPTH} levels of
     * {@value OffloadLayout#DEFAULT_SHARD_WIDTH} hex characters) and the default
     * hot-cache ceiling ({@value #DEFAULT_HOT_CACHE_MAX_ENTRIES}).
     *
     * @param baseDir the base directory for data storage
     * @param name    the map name (used as subdirectory)
     */
    public DiskOffloadStrategy(Path baseDir, String name) {
        this(baseDir, name, OffloadLayout.DEFAULT_SHARD_DEPTH, OffloadLayout.DEFAULT_SHARD_WIDTH,
                DEFAULT_HOT_CACHE_MAX_ENTRIES);
    }

    /**
     * Creates a new disk offload strategy with a configurable shard fan-out and
     * the default hot-cache ceiling ({@value #DEFAULT_HOT_CACHE_MAX_ENTRIES}).
     *
     * @param baseDir    the base directory for data storage
     * @param name       the map name (used as subdirectory)
     * @param shardDepth number of directory levels (0 = flat layout)
     * @param shardWidth hex characters consumed per directory level
     */
    public DiskOffloadStrategy(Path baseDir, String name, int shardDepth, int shardWidth) {
        this(baseDir, name, shardDepth, shardWidth, DEFAULT_HOT_CACHE_MAX_ENTRIES);
    }

    /**
     * Creates a new disk offload strategy with a configurable shard fan-out and
     * a bounded hot cache.
     * <p>
     * The fan-out determines how key files are spread across subdirectories:
     * {@code shardDepth} directory levels, each consuming {@code shardWidth}
     * hexadecimal characters of the key hash. It must be fixed at map creation
     * time — changing it for an already-populated directory orphans the
     * previously written paths.
     * <p>
     * The hot cache is an access-ordered LRU split across the lock stripes; it
     * holds at most {@code hotCacheMaxEntries} values in memory (per-stripe
     * ceiling {@code max(1, hotCacheMaxEntries / CONCURRENCY_LEVEL)}). A value
     * of {@code 0} disables caching, forcing every read to hit the disk.
     *
     * @param baseDir            the base directory for data storage
     * @param name               the map name (used as subdirectory)
     * @param shardDepth         number of directory levels (0 = flat layout)
     * @param shardWidth         hex characters consumed per directory level
     * @param hotCacheMaxEntries upper bound on cached values ({@code 0} disables)
     */
    @SuppressWarnings("unchecked")
    public DiskOffloadStrategy(Path baseDir, String name, int shardDepth, int shardWidth,
            int hotCacheMaxEntries) {
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(name, "name");
        if (hotCacheMaxEntries < 0) {
            throw new IllegalArgumentException("hotCacheMaxEntries must be >= 0");
        }
        this.offloadDir = baseDir.resolve(name).resolve(OFFLOAD_DIR);
        this.layout = OffloadLayout.of(shardDepth, shardWidth);
        this.maxPerStripe = hotCacheMaxEntries == 0
                ? 0
                : Math.max(1, hotCacheMaxEntries / CONCURRENCY_LEVEL);
        this.hotCaches = new LinkedHashMap[CONCURRENCY_LEVEL];
        this.locks = new ReentrantReadWriteLock[CONCURRENCY_LEVEL];
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            hotCaches[i] = newHotCache(maxPerStripe);
            locks[i] = new ReentrantReadWriteLock();
        }
        try {
            Files.createDirectories(offloadDir);
            loadIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize disk offload directory", e);
        }
    }

    private static <K, V> LinkedHashMap<K, V> newHotCache(int capacity) {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > capacity;
            }
        };
    }

    // ── Stripe helpers ──────────────────────────────────────────────────

    /**
     * Returns the stripe index for a given key using Fibonacci hashing
     * for better distribution across power-of-2 buckets.
     */
    private int stripeFor(Object key) {
        int h = key.hashCode();
        // Spread bits to reduce clustering on low-entropy hashCodes
        h ^= (h >>> 16);
        return h & (CONCURRENCY_LEVEL - 1);
    }

    private ReentrantReadWriteLock stripeLock(Object key) {
        return locks[stripeFor(key)];
    }

    /**
     * Acquires all write locks in index order to prevent deadlocks during
     * bulk operations such as {@link #clear()}.
     */
    private void lockAllWrite() {
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            locks[i].writeLock().lock();
        }
    }

    /**
     * Releases all write locks in reverse order.
     */
    private void unlockAllWrite() {
        for (int i = CONCURRENCY_LEVEL - 1; i >= 0; i--) {
            locks[i].writeLock().unlock();
        }
    }

    // ── Hot cache helpers (callers must hold the key's stripe lock) ──────

    private V cacheGet(K key) {
        if (maxPerStripe == 0) {
            return null;
        }
        return hotCaches[stripeFor(key)].get(key);
    }

    private void cachePut(K key, V value) {
        if (maxPerStripe == 0) {
            return;
        }
        hotCaches[stripeFor(key)].put(key, value);
    }

    private void cacheRemove(K key) {
        if (maxPerStripe == 0) {
            return;
        }
        hotCaches[stripeFor(key)].remove(key);
    }

    // ── NMapOffloadStrategy ─────────────────────────────────────────────

    @Override
    public V get(K key) {
        ReentrantReadWriteLock lock = stripeLock(key);
        lock.writeLock().lock();
        try {
            if (!index.containsKey(key)) {
                return null;
            }
            // Check hot cache first
            V cached = cacheGet(key);
            if (cached != null) {
                return cached;
            }
            // Cache miss — read from disk
            Path path = resolvePathForRead(key, index.get(key));
            if (path == null) {
                return null;
            }
            try {
                V value = deserialize(path);
                cachePut(key, value);
                return value;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to read offloaded entry: " + path, e);
                return null;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        ReentrantReadWriteLock lock = stripeLock(key);
        lock.writeLock().lock();
        try {
            V previous = getUnderLock(key);
            String keyHash = keyHash(key);
            Path path = pathForKey(keyHash);
            Path legacyPath = legacyPathForKey(keyHash);
            try {
                serialize(path, key, value);
                index.put(key, path);
                cachePut(key, value);
                if (!path.equals(legacyPath)) {
                    OffloadLayout.deleteFileQuietly(offloadDir, legacyPath, LOGGER);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to write offloaded entry", e);
            }
            return previous;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) {
        ReentrantReadWriteLock lock = stripeLock(key);
        lock.writeLock().lock();
        try {
            V previous = getUnderLock(key);
            index.remove(key);
            cacheRemove(key);
            String keyHash = keyHash(key);
            OffloadLayout.deleteFileQuietly(offloadDir, pathForKey(keyHash), LOGGER);
            OffloadLayout.deleteFileQuietly(offloadDir, legacyPathForKey(keyHash), LOGGER);
            return previous;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        ReentrantReadWriteLock lock = stripeLock(key);
        lock.readLock().lock();
        try {
            return index.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        // ConcurrentHashMap.size() is already thread-safe; no lock needed
        return index.size();
    }

    @Override
    public boolean isEmpty() {
        // ConcurrentHashMap.isEmpty() is already thread-safe; no lock needed
        return index.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        // Snapshot of the key set — ConcurrentHashMap iteration is thread-safe
        return Collections.unmodifiableSet(index.keySet());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                Iterator<K> keys = index.keySet().iterator();
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
        lockAllWrite();
        try {
            index.clear();
            for (LinkedHashMap<K, V> hotCache : hotCaches) {
                hotCache.clear();
            }
            OffloadLayout.clearDirectoryContentsRecursively(offloadDir, LOGGER);
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
        // Returns a write-through map view for persistence compatibility
        return new AbstractMap<>() {
            @Override
            public V put(K key, V value) {
                return DiskOffloadStrategy.this.put(key, value);
            }

            @Override
            public V remove(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return DiskOffloadStrategy.this.remove(k);
            }

            @Override
            public V get(Object key) {
                @SuppressWarnings("unchecked")
                K k = (K) key;
                return DiskOffloadStrategy.this.get(k);
            }

            @Override
            public void clear() {
                DiskOffloadStrategy.this.clear();
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                DiskOffloadStrategy.this.putAll(m);
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return DiskOffloadStrategy.this.entrySet();
            }

            @Override
            public int size() {
                return DiskOffloadStrategy.this.size();
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
            for (LinkedHashMap<K, V> hotCache : hotCaches) {
                hotCache.clear();
            }
            // Index and files remain on disk for future reopens
        } finally {
            unlockAllWrite();
        }
    }

    /**
     * Remove todas as entradas do disco e da memória, libera recursos e apaga
     * recursivamente o diretório de offload ({@code offloadDir}).
     *
     * @throws IOException se ocorrer erro de I/O durante a remoção
     */
    @Override
    public void destroy() throws IOException {
        clear();
        close();
        deleteDirectoryRecursively(offloadDir);
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /**
     * Reads a value from the hot cache or disk <b>without acquiring any
     * lock</b>. Must only be called when the caller already holds the
     * write lock for the key's stripe.
     */
    private V getUnderLock(K key) {
        if (!index.containsKey(key)) {
            return null;
        }
        V cached = cacheGet(key);
        if (cached != null) {
            return cached;
        }
        Path path = resolvePathForRead(key, index.get(key));
        if (path == null) {
            return null;
        }
        try {
            V value = deserialize(path);
            cachePut(key, value);
            return value;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read offloaded entry: " + path, e);
            return null;
        }
    }

    /**
     * Generates a collision-free file path for a given key using a SHA-1
     * digest of the serialized key bytes. Unlike {@code hashCode()}, SHA-1
     * produces a 160-bit fingerprint that never collides in practice, ensuring
     * two distinct keys always map to different files.
     */
    private Path pathForKey(K key) {
        return pathForKey(keyHash(key));
    }

    private Path pathForKey(String keyHash) {
        return layout.shardedPath(offloadDir, keyHash, ENTRY_SUFFIX);
    }

    private Path legacyPathForKey(String keyHash) {
        return OffloadLayout.legacyPath(offloadDir, keyHash, ENTRY_SUFFIX);
    }

    /**
     * Computes the SHA-1 hex digest of the serialized key.
     */
    private String keyHash(K key) {
        return OffloadLayout.keyHash(key);
    }

    /**
     * Scans the offload directory and rebuilds the key index from existing
     * files. Called once at construction time.
     */
    @SuppressWarnings("unchecked")
    private void loadIndex() throws IOException {
        if (!Files.isDirectory(offloadDir)) {
            return;
        }
        try (var stream = Files.walk(offloadDir)) {
            for (Path path : stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(ENTRY_SUFFIX))
                    .toList()) {
                try {
                    OffloadEntry<K, V> entry = deserializeEntry(path);
                    index.compute(entry.key(), (k, current) ->
                            OffloadLayout.shouldPrefer(offloadDir, current, path) ? path : current);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to load offloaded entry: " + path, e);
                }
            }
        }
    }

    private Path resolvePathForRead(K key, Path indexedPath) {
        if (indexedPath != null && Files.exists(indexedPath)) {
            return indexedPath;
        }
        Path resolved = layout.preferredExistingPath(offloadDir, keyHash(key), ENTRY_SUFFIX);
        if (resolved != null) {
            index.put(key, resolved);
            return resolved;
        }
        if (indexedPath != null) {
            index.remove(key, indexedPath);
        }
        cacheRemove(key);
        return null;
    }

    /**
     * Serializes a key-value pair to the given file path.
     */
    private void serialize(Path path, K key, V value) throws IOException {
        Files.createDirectories(path.getParent());
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(path)))) {
            oos.writeObject(key);
            oos.writeObject(value);
            oos.flush();
        }
    }

    /**
     * Deserializes only the value from a file (reads key first, then value).
     */
    @SuppressWarnings("unchecked")
    private V deserialize(Path path) throws IOException {
        OffloadEntry<K, V> entry = deserializeEntry(path);
        return entry.value();
    }

    /**
     * Deserializes both key and value from a file.
     */
    @SuppressWarnings("unchecked")
    private OffloadEntry<K, V> deserializeEntry(Path path) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            K key = (K) ois.readObject();
            V value = (V) ois.readObject();
            return new OffloadEntry<>(key, value);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize offloaded entry", e);
        }
    }

    /**
     * Internal record for holding a deserialized key-value pair.
     */
    private record OffloadEntry<K, V>(K key, V value) {
    }

    static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            LOGGER.log(Level.WARNING, "Failed to delete: " + p, e);
                        }
                    });
        }
    }
}
