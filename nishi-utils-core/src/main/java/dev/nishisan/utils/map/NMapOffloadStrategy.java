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
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Strategy interface that abstracts the storage backend for {@link NMap}.
 * <p>
 * Implementations control where key-value entries live: entirely in heap
 * ({@link InMemoryStrategy}) or spilled to disk ({@link DiskOffloadStrategy}).
 * The {@link NMap} class delegates all data operations to the active strategy,
 * keeping its public API unchanged.
 *
 * @param <K> the key type (must be {@link Serializable})
 * @param <V> the value type (must be {@link Serializable})
 */
public interface NMapOffloadStrategy<K extends Serializable, V extends Serializable> extends Closeable {

    /**
     * Returns the value associated with the given key, or {@code null} if absent.
     *
     * @param key the key
     * @return the value, or {@code null}
     */
    V get(K key);

    /**
     * Inserts or updates an entry, returning the previous value or {@code null}.
     *
     * @param key   the key
     * @param value the value
     * @return the previous value, or {@code null}
     */
    V put(K key, V value);

    /**
     * Removes an entry by key, returning the previous value or {@code null}.
     *
     * @param key the key
     * @return the previous value, or {@code null}
     */
    V remove(K key);

    /**
     * Returns whether the storage contains the given key.
     *
     * @param key the key
     * @return {@code true} if the key exists
     */
    boolean containsKey(K key);

    /**
     * Returns the number of entries.
     *
     * @return the size
     */
    int size();

    /**
     * Returns whether the storage is empty.
     *
     * @return {@code true} if empty
     */
    boolean isEmpty();

    /**
     * Returns a set view of the keys.
     *
     * @return the keys
     */
    Set<K> keySet();

    /**
     * Returns a set view of the entries.
     *
     * @return the entries
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Iterates all entries.
     *
     * @param action the action to run on each entry
     */
    void forEach(BiConsumer<? super K, ? super V> action);

    /**
     * Removes all entries.
     */
    void clear();

    /**
     * Puts all entries from the given map.
     *
     * @param entries the entries to put
     */
    void putAll(Map<? extends K, ? extends V> entries);

    /**
     * Returns a mutable {@link Map} view suitable for persistence replay.
     * <p>
     * For in-memory strategies this is the underlying map itself.
     * For disk-backed strategies this may return a view that writes through
     * to disk.
     *
     * @return a mutable map view
     */
    Map<K, V> asMap();

    /**
     * Returns whether this strategy already persists data to disk,
     * making WAL-based persistence redundant.
     *
     * @return {@code true} if data is inherently persisted by the strategy
     */
    boolean isInherentlyPersistent();
}
