package dev.nishisan.utils.stats.list;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FixedSizeList<E extends Number> {
    private final List<E> internalList;
    private final int capacity;

    public FixedSizeList(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive.");
        }
        this.capacity = capacity;
        this.internalList = new ArrayList<>(capacity);
    }

    public boolean add(E element) {
        if (internalList.size() == capacity) {
            // Remove the first element to make space for the new element at the end
            internalList.remove(0);
        }
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
}
