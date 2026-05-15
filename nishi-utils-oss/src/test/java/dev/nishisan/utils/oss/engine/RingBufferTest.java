package dev.nishisan.utils.oss.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingBufferTest {

    @Test
    void putRespeitaCapacidadeSemSobrescreverAteEncher() {
        RingBuffer rb = new RingBuffer(3);
        rb.put(1.0);
        rb.put(2.0);
        assertEquals(2, rb.size());
    }

    @Test
    void snapshotInOrderAposWrapDevolveSequenciaCorreta() {
        RingBuffer rb = new RingBuffer(3);
        rb.put(1.0);
        rb.put(2.0);
        rb.put(3.0);
        rb.put(4.0); // wraps
        rb.put(5.0);
        assertArrayEquals(new double[]{3.0, 4.0, 5.0}, rb.snapshotInOrder(), 1e-9);
    }

    @Test
    void clearRestauraEstadoInicial() {
        RingBuffer rb = new RingBuffer(2);
        rb.put(1.0);
        rb.clear();
        assertEquals(0, rb.size());
    }

    @Test
    void rejeitaCapacidadeInvalida() {
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer(0));
    }
}
