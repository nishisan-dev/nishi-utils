package dev.nishisan.utils.oss.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Coletor operacional do volume: acumula contadores e encadeia para um forward. */
class BlobVolumeMetricsTest {

    @Test
    void coletorAcumulaContadores() {
        BlobVolumeMetrics metrics = new BlobVolumeMetrics();
        metrics.onShardGrow(0, 64L, 128L);
        metrics.onShardGrow(1, 128L, 256L);
        metrics.onRegionAllocate(0, 8192L);
        metrics.onRegionFree(0, 8192L);
        metrics.onCheckpoint(7L, 42);

        assertEquals(2, metrics.shardGrowCount());
        assertEquals(1, metrics.regionAllocateCount());
        assertEquals(1, metrics.regionFreeCount());
        assertEquals(1, metrics.checkpointCount());
        assertEquals(7L, metrics.lastCheckpointDurationMs());
        assertEquals(42, metrics.lastCheckpointEntryCount());
    }

    @Test
    void encadeiaEventosParaForward() {
        long[] grows = {0};
        long[] allocs = {0};
        long[] frees = {0};
        long[] checkpoints = {0};
        BlobVolumeMetricsListener forward = new BlobVolumeMetricsListener() {
            @Override
            public void onShardGrow(int shardId, long oldCapacityBytes, long newCapacityBytes) {
                grows[0]++;
            }

            @Override
            public void onRegionAllocate(int shardId, long regionBytes) {
                allocs[0]++;
            }

            @Override
            public void onRegionFree(int shardId, long regionBytes) {
                frees[0]++;
            }

            @Override
            public void onCheckpoint(long durationMs, int entryCount) {
                checkpoints[0]++;
            }
        };
        BlobVolumeMetrics metrics = new BlobVolumeMetrics(forward);
        metrics.onShardGrow(0, 64L, 128L);
        metrics.onRegionAllocate(0, 8192L);
        metrics.onRegionFree(0, 8192L);
        metrics.onCheckpoint(3L, 10);

        assertEquals(1, metrics.shardGrowCount());
        assertEquals(1, metrics.regionAllocateCount());
        assertEquals(1, metrics.regionFreeCount());
        assertEquals(1, metrics.checkpointCount());
        assertEquals(1, grows[0]);
        assertEquals(1, allocs[0]);
        assertEquals(1, frees[0]);
        assertEquals(1, checkpoints[0]);
    }
}
