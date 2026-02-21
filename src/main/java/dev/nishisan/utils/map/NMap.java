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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * A standalone persistent map backed by a pluggable storage strategy plus an
 * optional WAL + snapshot engine. Analogous to
 * {@link dev.nishisan.utils.queue.NQueue}, {@code NMap} can be used
 * <b>independently</b> of NGrid.
 *
 * <h2>Quick start (in-memory, default)</h2>
 * 
 * <pre>{@code
 * try (NMap<String, String> map = NMap.open(Path.of("/data"), "mymap")) {
 *     map.put("key", "value");
 *     System.out.println(map.get("key")); // prints "value"
 * }
 * }</pre>
 *
 * <h2>Disk offloading</h2>
 * 
 * <pre>{@code
 * NMapConfig cfg = NMapConfig.builder()
 *         .mode(NMapPersistenceMode.DISABLED) // WAL not needed
 *         .offloadStrategyFactory(DiskOffloadStrategy::new)
 *         .build();
 * try (NMap<String, String> map = NMap.open(Path.of("/data"), "offloaded", cfg)) {
 *     map.put("key", "value");
 *     // Entry is stored on disk, not in heap
 * }
 * }</pre>
 *
 * <h2>In-memory mode</h2>
 * 
 * <pre>{@code
 * try (NMap<String, String> map = NMap.open(Path.of("/tmp"), "ephemeral", NMapConfig.inMemory())) {
 *     map.put("k", "v");
 * }
 * }</pre>
 *
 * @param <K> the key type (must be {@link Serializable})
 * @param <V> the value type (must be {@link Serializable})
 */
public final class NMap<K extends Serializable, V extends Serializable> implements Closeable {

    private final NMapOffloadStrategy<K, V> storage;
    private final NMapPersistence<K, V> persistence;
    private final NMapConfig config;
    private final String name;

    private NMap(Path baseDir, String name, NMapConfig config) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");

        // Instantiate storage strategy
        if (config.offloadStrategyFactory() != null) {
            this.storage = config.offloadStrategyFactory().create(baseDir, name);
        } else {
            this.storage = new InMemoryStrategy<>();
        }

        // Persistence engine: only for non-inherently-persistent strategies
        if (config.mode() != NMapPersistenceMode.DISABLED && !storage.isInherentlyPersistent()) {
            this.persistence = new NMapPersistence<>(config, storage.asMap(), baseDir, name);
        } else {
            this.persistence = null;
        }
    }

    // ── Factory Methods ─────────────────────────────────────────────────

    /**
     * Opens (or creates) a persistent map with default configuration
     * ({@link NMapPersistenceMode#ASYNC_WITH_FSYNC}).
     *
     * @param baseDir the directory where the map data will be stored
     * @param name    the map name (used as subdirectory)
     * @param <K>     the key type
     * @param <V>     the value type
     * @return the opened map
     */
    public static <K extends Serializable, V extends Serializable> NMap<K, V> open(Path baseDir, String name) {
        return open(baseDir, name, NMapConfig.defaults(NMapPersistenceMode.ASYNC_WITH_FSYNC));
    }

    /**
     * Opens (or creates) a map with custom configuration.
     *
     * @param baseDir the directory where the map data will be stored
     * @param name    the map name (used as subdirectory)
     * @param config  the configuration
     * @param <K>     the key type
     * @param <V>     the value type
     * @return the opened map
     */
    public static <K extends Serializable, V extends Serializable> NMap<K, V> open(
            Path baseDir, String name, NMapConfig config) {
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(config, "config");

        NMap<K, V> map = new NMap<>(baseDir, name, config);
        if (map.persistence != null) {
            map.persistence.load();
            map.persistence.start();
        }
        return map;
    }

    // ── Mutating Operations ─────────────────────────────────────────────

    /**
     * Inserts or updates an entry, returning the previous value if any.
     *
     * @param key   the key
     * @param value the value
     * @return the previous value, or {@code empty}
     */
    public Optional<V> put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        V prev = storage.put(key, value);
        if (persistence != null) {
            persistence.appendAsync(NMapOperationType.PUT, key, value);
        }
        return Optional.ofNullable(prev);
    }

    /**
     * Removes an entry by key, returning the previous value if any.
     *
     * @param key the key
     * @return the previous value, or {@code empty}
     */
    public Optional<V> remove(K key) {
        Objects.requireNonNull(key, "key");
        V prev = storage.remove(key);
        if (prev != null && persistence != null) {
            persistence.appendAsync(NMapOperationType.REMOVE, key, null);
        }
        return Optional.ofNullable(prev);
    }

    /**
     * Puts all entries from the given map.
     *
     * @param entries the entries to put
     */
    public void putAll(Map<? extends K, ? extends V> entries) {
        Objects.requireNonNull(entries, "entries");
        entries.forEach(this::put);
    }

    /**
     * Removes all entries from this map.
     */
    public void clear() {
        if (persistence != null) {
            storage.keySet().forEach(k -> persistence.appendAsync(NMapOperationType.REMOVE, k, null));
        }
        storage.clear();
    }

    // ── Read Operations ─────────────────────────────────────────────────

    /**
     * Returns the value for the given key, or {@code empty}.
     *
     * @param key the key
     * @return the value
     */
    public Optional<V> get(K key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(storage.get(key));
    }

    /**
     * Returns whether the map contains the given key.
     *
     * @param key the key
     * @return true if the key exists
     */
    public boolean containsKey(K key) {
        Objects.requireNonNull(key, "key");
        return storage.containsKey(key);
    }

    /**
     * Returns the size of the map.
     *
     * @return the number of entries
     */
    public int size() {
        return storage.size();
    }

    /**
     * Returns whether the map is empty.
     *
     * @return true if the map is empty
     */
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    /**
     * Returns an unmodifiable view of the key set.
     *
     * @return the keys
     */
    public Set<K> keySet() {
        return Collections.unmodifiableSet(storage.keySet());
    }

    /**
     * Returns an unmodifiable view of the entry set.
     *
     * @return the entries
     */
    public Set<Map.Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(storage.entrySet());
    }

    /**
     * Iterates all entries.
     *
     * @param action the action to run on each entry
     */
    public void forEach(BiConsumer<? super K, ? super V> action) {
        storage.forEach(action);
    }

    /**
     * Returns an unmodifiable snapshot of the current map state.
     *
     * @return an immutable copy of the map
     * @implNote When using a disk-backed strategy ({@link DiskOffloadStrategy},
     *           {@link HybridOffloadStrategy}), this method iterates all entries
     *           from disk and may perform N synchronous I/O operations —
     *           potentially
     *           slow for large maps. Use with caution in latency-sensitive paths.
     */
    public Map<K, V> snapshot() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(storage.asMap()));
    }

    /**
     * Returns the map name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the number of persistence failures since this map was opened.
     *
     * @return the failure count, or 0 if persistence is disabled
     */
    public long persistenceFailureCount() {
        return persistence != null ? persistence.failureCount() : 0;
    }

    // ── Internal Access (for NGrid integration) ─────────────────────────

    /**
     * Provides direct access to the underlying map for NGrid's
     * {@code MapClusterService}. Package-private.
     *
     * @return the underlying concurrent map (only valid for in-memory strategy)
     */
    ConcurrentHashMap<K, V> internalMap() {
        if (storage instanceof InMemoryStrategy<K, V> inMem) {
            return inMem.internalMap();
        }
        throw new UnsupportedOperationException(
                "internalMap() is only supported with InMemoryStrategy");
    }

    /**
     * Provides direct access to the persistence engine for NGrid's
     * {@code MapClusterService}. Package-private.
     *
     * @return the persistence engine, or {@code null} if disabled
     */
    NMapPersistence<K, V> internalPersistence() {
        return persistence;
    }

    /**
     * Returns the active offload strategy. Package-private.
     *
     * @return the strategy
     */
    NMapOffloadStrategy<K, V> strategy() {
        return storage;
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    @Override
    public void close() throws IOException {
        if (persistence != null) {
            persistence.close();
        }
        storage.close();
    }
}
