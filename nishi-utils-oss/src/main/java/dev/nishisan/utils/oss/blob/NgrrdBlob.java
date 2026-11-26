package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

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
        private VolumeWriteMode writeMode = VolumeWriteMode.MMAP;
        private NgrrdMetricsListener qualityListener;
        private BlobVolumeMetricsListener volumeMetricsListener;
        private Executor checkpointForceExecutor;
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

        /**
         * Modo de escrita dos shards dos volumes registrados por nome (default
         * {@link VolumeWriteMode#MMAP}). Ver {@link VolumeWriteMode}.
         */
        public Builder writeMode(VolumeWriteMode writeMode) {
            this.writeMode = Objects.requireNonNull(writeMode, "writeMode é obrigatório");
            return this;
        }

        /**
         * Listener de qualidade default aplicado a todos os volumes do registro,
         * propagado a cada handle aberto via {@code Ngrrd.open(...)}.
         */
        public Builder qualityListener(NgrrdMetricsListener qualityListener) {
            this.qualityListener = qualityListener;
            return this;
        }

        /** Listener de métricas operacionais aplicado a todos os volumes do registro. */
        public Builder volumeMetricsListener(BlobVolumeMetricsListener volumeMetricsListener) {
            this.volumeMetricsListener = volumeMetricsListener;
            return this;
        }

        /**
         * Executor da fase B (o force) dos checkpoints, aplicado a todos os volumes do
         * registro e propagado a cada handle aberto via {@code Ngrrd.open(...)}. Com ele,
         * o msync de cada checkpoint sai da thread do writer; a fase A (CDPs parciais e
         * live-state) continua nela. {@code null} (padrão) = force inline, o comportamento
         * de sempre. O executor tem de continuar vivo até todos os handles fecharem: o
         * {@code close()} de cada handle espera os forces em voo. Fica fora de
         * {@link BlobVolumeConfig}, para não mudar a identidade do volume por nome.
         */
        public Builder checkpointForceExecutor(Executor checkpointForceExecutor) {
            this.checkpointForceExecutor = checkpointForceExecutor;
            return this;
        }

        /** Registra um volume por nome: diretório = {@code basePath/name}, usando os defaults atuais. */
        public Builder volume(String name) {
            Objects.requireNonNull(basePath, "basePath é obrigatório para registrar volume por nome");
            long capacity = initialCapacity > 0 ? initialCapacity : segmentBytes;
            configs.add(new BlobVolumeConfig(name, basePath.resolve(name), shardCount, segmentBytes, capacity,
                    writeMode));
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
                registry.open(config, qualityListener, volumeMetricsListener, checkpointForceExecutor);
            }
            return registry;
        }
    }
}
