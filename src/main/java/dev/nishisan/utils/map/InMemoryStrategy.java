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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Default in-memory strategy backed by a {@link ConcurrentHashMap}.
 * <p>
 * This preserves the original {@link NMap} behaviour: all entries reside
 * entirely in the JVM heap. No disk offloading is performed.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class InMemoryStrategy<K extends Serializable, V extends Serializable>
        implements NMapOffloadStrategy<K, V> {

    private final ConcurrentHashMap<K, V> data = new ConcurrentHashMap<>();

    @Override
    public V get(K key) {
        return data.get(key);
    }

    @Override
    public V put(K key, V value) {
        return data.put(key, value);
    }

    @Override
    public V remove(K key) {
        return data.remove(key);
    }

    @Override
    public boolean containsKey(K key) {
        return data.containsKey(key);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return data.keySet();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return data.entrySet();
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        data.forEach(action);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        data.putAll(entries);
    }

    @Override
    public Map<K, V> asMap() {
        return data;
    }

    @Override
    public boolean isInherentlyPersistent() {
        return false;
    }

    /**
     * Provides direct access to the underlying {@link ConcurrentHashMap}
     * for NGrid integration (package-private).
     *
     * @return the underlying concurrent map
     */
    ConcurrentHashMap<K, V> internalMap() {
        return data;
    }

    @Override
    public void close() {
        // No resources to release
    }
}
