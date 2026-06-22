package dev.nishisan.utils.oss.blob;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Ponto de entrada para configurar blob volumes — a "config geral" com o
 * {@code ngrrd-blob-base-path} e os namespaces. Constrói um {@link BlobVolumeRegistry}
 * programático. Ver {@code doc/oss/ngrrd-blob-volume.md}.
 *
 * <pre>{@code
 * BlobVolumeRegistry volumes = NgrrdBlob.registry()
 *         .basePath(Path.of("/var/ngrrd/blobs"))   // ngrrd-blob-base-path
 *         .volume("ifaceStats")                     // -> /var/ngrrd/blobs/ifaceStats
 *         .build();
 * }</pre>
 */
public final class NgrrdBlob {

    private NgrrdBlob() {
    }

    public static Builder registry() {
        return new Builder();
    }

    /** Builder fluente do registro de volumes. */
    public static final class Builder {

        private Path basePath;
        private int shardCount = BlobVolumeConfig.DEFAULT_SHARD_COUNT;
        private long segmentBytes = BlobVolumeConfig.DEFAULT_SEGMENT_BYTES;
        private long initialCapacity = -1;
        private final List<BlobVolumeConfig> configs = new ArrayList<>();

        /** Diretório base sob o qual volumes referenciados por nome são criados. */
        public Builder basePath(Path basePath) {
            this.basePath = basePath;
            return this;
        }

        public Builder shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder segmentBytes(long segmentBytes) {
            this.segmentBytes = segmentBytes;
            return this;
        }

        public Builder initialShardCapacityBytes(long initialCapacity) {
            this.initialCapacity = initialCapacity;
            return this;
        }

        /** Registra um volume por nome: diretório = {@code basePath/name}, usando os defaults atuais. */
        public Builder volume(String name) {
            Objects.requireNonNull(basePath, "basePath é obrigatório para registrar volume por nome");
            long capacity = initialCapacity > 0 ? initialCapacity : segmentBytes;
            configs.add(new BlobVolumeConfig(name, basePath.resolve(name), shardCount, segmentBytes, capacity));
            return this;
        }

        /** Registra um volume com configuração explícita. */
        public Builder volume(BlobVolumeConfig config) {
            configs.add(config);
            return this;
        }

        /** Abre todos os volumes registrados e devolve o registro. */
        public BlobVolumeRegistry build() {
            BlobVolumeRegistry registry = new BlobVolumeRegistry();
            for (BlobVolumeConfig config : configs) {
                registry.open(config);
            }
            return registry;
        }
    }
}
