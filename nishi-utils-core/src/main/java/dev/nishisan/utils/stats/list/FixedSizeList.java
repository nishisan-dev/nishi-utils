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

package dev.nishisan.utils.stats.list;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Fixed-capacity list that evicts the oldest element when full.
 *
 * @param <E> the numeric element type
 */
public class FixedSizeList<E extends Number> {
    private final List<E> internalList;
    private final int capacity;
    private final String name;
    private E lastAddedElement;

    /**
     * Creates a fixed-size list with the given name and capacity.
     *
     * @param name     the list name
     * @param capacity the maximum capacity
     */
    public FixedSizeList(String name, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive.");
        }
        this.capacity = capacity;
        this.name = name;
        this.internalList = new ArrayList<>(capacity);
    }

    /**
     * Adds an element, evicting the oldest if at capacity.
     *
     * @param element the element to add
     * @return {@code true} if the element was added
     */
    public boolean add(E element) {
        if (internalList.size() == capacity) {
            // Remove the first element to make space for the new element at the end
            internalList.remove(0);
        }
        this.lastAddedElement = element;
        return internalList.add(element);
    }

    /**
     * Removes and returns the last element.
     *
     * @return the last element, or {@code null} if empty
     */
    public E removeLast() {
        if (internalList.isEmpty()) {
            return null; // Or throw an exception
        }
        return internalList.remove(internalList.size() - 1);
    }

    /**
     * Returns the element at the given index.
     *
     * @param index the index
     * @return the element
     */
    public E get(int index) {
        return internalList.get(index);
    }

    /**
     * Returns the number of elements in this list.
     *
     * @return the size
     */
    public int size() {

        return internalList.size();
    }

    /**
     * Returns a stream of the elements.
     *
     * @return the element stream
     */
    public Stream<E> stream() {
        return internalList.stream();
    }

    /**
     * Computes the average of all non-null elements.
     *
     * @return the average, or {@code 0.0} if empty
     */
    public Double getAverage() {
        return stream()
                .filter(java.util.Objects::nonNull)
                .mapToDouble(Number::doubleValue)
                .average()
                .orElse(0.0);
    }

    /**
     * Returns the name of this list.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the internal list backing this fixed-size list.
     *
     * @return the internal list
     */
    public List<E> getInternalList() {
        return internalList;
    }

    /**
     * Returns the maximum capacity.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Returns the last added element.
     *
     * @return the last added element, or {@code null}
     */
    public E getLastAddedElement() {
        return lastAddedElement;
    }
}
