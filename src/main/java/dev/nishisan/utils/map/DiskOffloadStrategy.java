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
import java.lang.ref.SoftReference;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Disk-backed offloading strategy for {@link NMap}.
 * <p>
 * Entries are serialized to individual files on disk under a dedicated
 * directory. A lightweight index ({@code ConcurrentHashMap<K, Path>}) maps
 * keys to their file locations, keeping heap usage minimal. An optional
 * {@link SoftReference} cache avoids re-reads for hot entries; the JVM
 * will reclaim cached values under memory pressure.
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
public final class DiskOffloadStrategy<K extends Serializable, V extends Serializable>
        implements NMapOffloadStrategy<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DiskOffloadStrategy.class.getName());
    private static final String OFFLOAD_DIR = "offload";
    private static final String ENTRY_SUFFIX = ".dat";

    private final Path offloadDir;
    private final ConcurrentHashMap<K, Path> index = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K, SoftReference<V>> cache = new ConcurrentHashMap<>();

    /**
     * Creates a new disk offload strategy.
     *
     * @param baseDir the base directory for data storage
     * @param name    the map name (used as subdirectory)
     */
    public DiskOffloadStrategy(Path baseDir, String name) {
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(name, "name");
        this.offloadDir = baseDir.resolve(name).resolve(OFFLOAD_DIR);
        try {
            Files.createDirectories(offloadDir);
            loadIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize disk offload directory", e);
        }
    }

    @Override
    public V get(K key) {
        if (!index.containsKey(key)) {
            return null;
        }
        // Check soft cache first
        SoftReference<V> ref = cache.get(key);
        if (ref != null) {
            V cached = ref.get();
            if (cached != null) {
                return cached;
            }
        }
        // Cache miss — read from disk
        Path path = index.get(key);
        if (path == null || !Files.exists(path)) {
            index.remove(key);
            cache.remove(key);
            return null;
        }
        try {
            V value = deserialize(path);
            cache.put(key, new SoftReference<>(value));
            return value;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read offloaded entry: " + path, e);
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        V previous = get(key);
        Path path = pathForKey(key);
        try {
            serialize(path, key, value);
            index.put(key, path);
            cache.put(key, new SoftReference<>(value));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write offloaded entry", e);
        }
        return previous;
    }

    @Override
    public V remove(K key) {
        V previous = get(key);
        Path path = index.remove(key);
        cache.remove(key);
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete offloaded entry: " + path, e);
            }
        }
        return previous;
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
        for (Path path : index.values()) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete offloaded entry: " + path, e);
            }
        }
        index.clear();
        cache.clear();
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
        cache.clear();
        // Index and files remain on disk for future reopens
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /**
     * Generates a deterministic file path for a given key.
     */
    private Path pathForKey(K key) {
        int hash = key.hashCode();
        // Use hex encoding of the hash for a flat namespace
        String fileName = String.format("%08x", hash) + ENTRY_SUFFIX;
        return offloadDir.resolve(fileName);
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
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(offloadDir, "*" + ENTRY_SUFFIX)) {
            for (Path path : stream) {
                try {
                    OffloadEntry<K, V> entry = deserializeEntry(path);
                    index.put(entry.key(), path);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to load offloaded entry: " + path, e);
                }
            }
        }
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
}
