package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;

import java.nio.file.Path;

/** Implementação padrão de {@link BlobVolume} sobre um {@link BlobStorage}. */
final class DefaultBlobVolume implements BlobVolume {

    private final String name;
    private final Path directory;
    private final int shardCount;
    private final BlobStorage storage;

    private DefaultBlobVolume(String name, Path directory, int shardCount, BlobStorage storage) {
        this.name = name;
        this.directory = directory;
        this.shardCount = shardCount;
        this.storage = storage;
    }

    static DefaultBlobVolume open(BlobVolumeConfig config) {
        BlobStorage storage = BlobStorage.openOrCreate(config.directory(), config.shardCount(),
                config.segmentBytes(), config.initialShardCapacityBytes());
        return new DefaultBlobVolume(config.name(), config.directory(), config.shardCount(), storage);
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
    public void checkpoint() {
        storage.checkpoint();
    }

    @Override
    public void close() {
        storage.close();
    }
}
