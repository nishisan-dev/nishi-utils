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

public class FixedSizeList<E extends Number> {
    private final List<E> internalList;
    private final int capacity;
    private final String name;
    private E lastAddedElement;
    public FixedSizeList(String name,int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive.");
        }
        this.capacity = capacity;
        this.name = name;
        this.internalList = new ArrayList<>(capacity);
    }

    public boolean add(E element) {
        if (internalList.size() == capacity) {
            // Remove the first element to make space for the new element at the end
            internalList.remove(0);
        }
        this.lastAddedElement = element;
        return internalList.add(element);
    }

    public E removeLast() {
        if (internalList.isEmpty()) {
            return null; // Or throw an exception
        }
        return internalList.remove(internalList.size() - 1);
    }

    public E get(int index) {
        return internalList.get(index);
    }

    public int size() {

        return internalList.size();
    }

    public Stream<E> stream() {
        return internalList.stream();
    }

    public Double getAverage() {
        return stream()
                .filter(java.util.Objects::nonNull)
                .mapToDouble(Number::doubleValue)
                .average()
                .orElse(0.0);
    }


    public String getName() {
        return name;
    }


    public List<E> getInternalList() {
        return internalList;
    }

    public int getCapacity() {
        return capacity;
    }

    public E getLastAddedElement() {
        return lastAddedElement;
    }
}
