package dev.nishisan.utils.oss.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeBucketTest {

    @Test
    void alinhaParaInicioDoBucket() {
        assertEquals(1747339500000L, TimeBucket.alignDown(1747339599000L, 300_000L));
        assertEquals(1747339500000L, TimeBucket.alignDown(1747339500000L, 300_000L));
    }

    @Test
    void alignUpAvancaSomenteSeDesalinhado() {
        assertEquals(1747339800000L, TimeBucket.alignUp(1747339500000L, 300_000L));
        assertEquals(1747339800000L, TimeBucket.alignUp(1747339599000L, 300_000L));
    }

    @Test
    void bucketCountContaJanelas() {
        long start = 1747339200000L;
        long end = start + 4 * 300_000L;
        assertEquals(4, TimeBucket.bucketCount(start, end, 300_000L));
    }

    @Test
    void rejeitaStepInvalido() {
        assertThrows(IllegalArgumentException.class, () -> TimeBucket.alignDown(1L, 0L));
    }
}
