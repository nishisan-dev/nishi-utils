package dev.nishisan.utils.oss.engine;

import java.util.Arrays;

/**
 * Buffer round-robin de tamanho fixo para CDPs em formação dentro de um bloco
 * temporal aberto.
 *
 * <p>Não é thread-safe — o {@code NgrrdWriter} mantém um buffer por RRA por
 * série e serializa escrita por meio do seu thread dedicado.</p>
 */
public final class RingBuffer {

    private final int capacity;
    private final double[] data;
    private int writeIndex;
    private int size;

    public RingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity deve ser > 0");
        }
        this.capacity = capacity;
        this.data = new double[capacity];
        Arrays.fill(this.data, Double.NaN);
        this.writeIndex = 0;
        this.size = 0;
    }

    public int capacity() {
        return capacity;
    }

    public int size() {
        return size;
    }

    public void put(double value) {
        data[writeIndex] = value;
        writeIndex = (writeIndex + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    public double get(int absoluteIndex) {
        if (absoluteIndex < 0 || absoluteIndex >= capacity) {
            throw new IndexOutOfBoundsException("absoluteIndex fora dos limites: " + absoluteIndex);
        }
        return data[absoluteIndex];
    }

    /**
     * Devolve uma cópia ordenada (do mais antigo ao mais recente, considerando
     * a posição corrente do write head).
     */
    public double[] snapshotInOrder() {
        double[] out = new double[size];
        if (size < capacity) {
            System.arraycopy(data, 0, out, 0, size);
        } else {
            int tail = capacity - writeIndex;
            System.arraycopy(data, writeIndex, out, 0, tail);
            System.arraycopy(data, 0, out, tail, writeIndex);
        }
        return out;
    }

    public void clear() {
        Arrays.fill(data, Double.NaN);
        writeIndex = 0;
        size = 0;
    }
}
