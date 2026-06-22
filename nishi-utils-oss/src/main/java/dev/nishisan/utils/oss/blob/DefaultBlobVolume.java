package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;

import java.nio.file.Path;

/** Implementação padrão de {@link BlobVolume} sobre um {@link BlobStorage}. */
final class DefaultBlobVolume implements BlobVolume {

    private final String name;
    private final Path directory;
    private final int shardCount;
    private final BlobStorage storage;
    private final NgrrdMetricsListener qualityListener;
    private final BlobVolumeMetricsListener volumeMetricsListener;

    private DefaultBlobVolume(String name, Path directory, int shardCount, BlobStorage storage,
                              NgrrdMetricsListener qualityListener,
                              BlobVolumeMetricsListener volumeMetricsListener) {
        this.name = name;
        this.directory = directory;
        this.shardCount = shardCount;
        this.storage = storage;
        this.qualityListener = qualityListener;
        this.volumeMetricsListener = volumeMetricsListener;
    }

    static DefaultBlobVolume open(BlobVolumeConfig config) {
        return open(config, null, null);
    }

    static DefaultBlobVolume open(BlobVolumeConfig config, NgrrdMetricsListener qualityListener,
                                  BlobVolumeMetricsListener volumeMetricsListener) {
        BlobStorage storage = BlobStorage.openOrCreate(config.directory(), config.shardCount(),
                config.segmentBytes(), config.initialShardCapacityBytes(), volumeMetricsListener);
        return new DefaultBlobVolume(config.name(), config.directory(), config.shardCount(), storage,
                qualityListener, volumeMetricsListener);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Path directory() {
        return directory;
    }

    @Override
    public int shardCount() {
        return shardCount;
    }

    @Override
    public BlobStorage storage() {
        return storage;
    }

    @Override
    public StorageFactory.StorageBindings bindings() {
        return StorageFactory.StorageBindings.forBlob(storage);
    }

    @Override
    public NgrrdMetricsListener qualityListener() {
        return qualityListener;
    }

    @Override
    public BlobVolumeMetricsListener volumeMetricsListener() {
        return volumeMetricsListener;
    }

    @Override
    public void checkpoint() {
        storage.checkpoint();
    }

    @Override
    public void close() {
        storage.close();
    }
}
