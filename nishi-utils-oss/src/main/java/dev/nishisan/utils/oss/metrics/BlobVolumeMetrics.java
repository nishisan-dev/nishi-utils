package dev.nishisan.utils.oss.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Coletor thread-safe dos eventos operacionais de um blob volume. Mantém
 * contadores acumulados e os últimos valores de checkpoint, e faz forward opcional
 * para um {@link BlobVolumeMetricsListener} encadeado (mesmo padrão decorator de
 * {@link NgrrdMetrics}).
 *
 * <p>Lock-free (todos os campos são atômicos): seguro para ser invocado dentro ou
 * fora do lock estrutural do volume sem introduzir contenção.</p>
 */
public final class BlobVolumeMetrics implements BlobVolumeMetricsListener {

    private final AtomicLong shardGrowCount = new AtomicLong();
    private final AtomicLong regionAllocateCount = new AtomicLong();
    private final AtomicLong regionFreeCount = new AtomicLong();
    private final AtomicLong checkpointCount = new AtomicLong();
    private final AtomicLong lastCheckpointDurationMs = new AtomicLong();
    private final AtomicLong lastCheckpointEntryCount = new AtomicLong();
    private final BlobVolumeMetricsListener forward;

    public BlobVolumeMetrics() {
        this(null);
    }

    public BlobVolumeMetrics(BlobVolumeMetricsListener forward) {
        this.forward = forward;
    }

    public long shardGrowCount() {
        return shardGrowCount.get();
    }

    public long regionAllocateCount() {
        return regionAllocateCount.get();
    }

    public long regionFreeCount() {
        return regionFreeCount.get();
    }

    public long checkpointCount() {
        return checkpointCount.get();
    }

    public long lastCheckpointDurationMs() {
        return lastCheckpointDurationMs.get();
    }

    public long lastCheckpointEntryCount() {
        return lastCheckpointEntryCount.get();
    }

    @Override
    public void onShardGrow(int shardId, long oldCapacityBytes, long newCapacityBytes) {
        shardGrowCount.incrementAndGet();
        if (forward != null) {
            forward.onShardGrow(shardId, oldCapacityBytes, newCapacityBytes);
        }
    }

    @Override
    public void onRegionAllocate(int shardId, long regionBytes) {
        regionAllocateCount.incrementAndGet();
        if (forward != null) {
            forward.onRegionAllocate(shardId, regionBytes);
        }
    }

    @Override
    public void onRegionFree(int shardId, long regionBytes) {
        regionFreeCount.incrementAndGet();
        if (forward != null) {
            forward.onRegionFree(shardId, regionBytes);
        }
    }

    @Override
    public void onCheckpoint(long durationMs, int entryCount) {
        checkpointCount.incrementAndGet();
        lastCheckpointDurationMs.set(durationMs);
        lastCheckpointEntryCount.set(entryCount);
        if (forward != null) {
            forward.onCheckpoint(durationMs, entryCount);
        }
    }
}
