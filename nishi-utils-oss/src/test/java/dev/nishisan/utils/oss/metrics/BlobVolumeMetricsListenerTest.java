package dev.nishisan.utils.oss.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Contrato do listener operacional do volume: os 4 callbacks são default no-op. */
class BlobVolumeMetricsListenerTest {

    @Test
    void defaultsSaoNoOpENaoLancam() {
        BlobVolumeMetricsListener listener = new BlobVolumeMetricsListener() {
        };
        assertDoesNotThrow(() -> {
            listener.onShardGrow(0, 64L, 128L);
            listener.onRegionAllocate(1, 8192L);
            listener.onRegionFree(1, 8192L);
            listener.onCheckpoint(5L, 42);
        });
    }
}
