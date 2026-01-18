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

package dev.nishisan.utils.ngrid.structures;

import java.io.Serializable;
import java.util.Objects;

/**
 * Type-safe queue descriptor for compile-time safety.
 * <p>
 * Provides a way to reference distributed queues with compile-time type
 * enforcement,
 * reducing errors from mismatched queue names and types.
 * 
 * <p>
 * Example usage:
 * 
 * <pre>
 * // Define typed queues as constants
 * public static final TypedQueue&lt;Order&gt; ORDERS = 
 *     TypedQueue.of("orders", Order.class);
 * 
 * // Use with compile-time safety
 * DistributedQueue&lt;Order&gt; queue = node.getQueue(ORDERS);
 * queue.offer(new Order(...));  // ✓ Type-safe
 * </pre>
 *
 * @param <T> the type of items in the queue
 * @since 2.1.0
 */
public interface TypedQueue<T extends Serializable> {
    /**
     * Returns the queue name.
     */
    String name();

    /**
     * Returns the queue item type.
     */
    Class<T> type();

    /**
     * Creates a typed queue descriptor.
     *
     * @param name the queue name
     * @param type the item type class
     * @param <T>  the item type
     * @return a new typed queue descriptor
     */
    static <T extends Serializable> TypedQueue<T> of(String name, Class<T> type) {
        return new TypedQueueImpl<>(name, type);
    }
}

/**
 * Implementation of TypedQueue.
 */
final class TypedQueueImpl<T extends Serializable> implements TypedQueue<T> {
    private final String name;
    private final Class<T> type;

    TypedQueueImpl(String name, Class<T> type) {
        this.name = Objects.requireNonNull(name, "queue name cannot be null");
        this.type = Objects.requireNonNull(type, "queue type cannot be null");

        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("queue name cannot be empty");
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TypedQueue))
            return false;
        TypedQueue<?> that = (TypedQueue<?>) o;
        return name.equals(that.name()) && type.equals(that.type());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "TypedQueue{" + name + ":" + type.getSimpleName() + "}";
    }
}
