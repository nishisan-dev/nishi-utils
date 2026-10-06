package dev.nishisan.utils.oss.blob;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Configuração imutável de um blob volume. Ver {@code doc/oss/ngrrd-blob-volume.md}.
 *
 * @param name                     nome lógico do volume (ex.: {@code ifaceStats})
 * @param directory                diretório físico do volume (ex.: {@code /var/ngrrd/blobs/ifaceStats})
 * @param shardCount               número de shards (imutável após criação; default {@value #DEFAULT_SHARD_COUNT})
 * @param segmentBytes             tamanho do segmento mmap (default 1 GiB)
 * @param initialShardCapacityBytes capacidade inicial (sparse) de cada shard
 * @param writeMode                modo de escrita dos shards (default {@link VolumeWriteMode#MMAP})
 */
public record BlobVolumeConfig(String name, Path directory, int shardCount,
                               long segmentBytes, long initialShardCapacityBytes,
                               VolumeWriteMode writeMode) {

    public static final int DEFAULT_SHARD_COUNT = 64;
    public static final long DEFAULT_SEGMENT_BYTES = 1L << 30; // 1 GiB

    public BlobVolumeConfig {
        Objects.requireNonNull(name, "name é obrigatório");
        Objects.requireNonNull(directory, "directory é obrigatório");
        Objects.requireNonNull(writeMode, "writeMode é obrigatório");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name do volume não pode ser vazio");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount deve ser > 0: " + shardCount);
        }
        if (segmentBytes <= 0) {
            throw new IllegalArgumentException("segmentBytes deve ser > 0: " + segmentBytes);
        }
        if (initialShardCapacityBytes <= 0) {
            throw new IllegalArgumentException("initialShardCapacityBytes deve ser > 0: " + initialShardCapacityBytes);
        }
    }

    /** Compatibilidade: configuração sem modo de escrita explícito (usa {@link VolumeWriteMode#MMAP}). */
    public BlobVolumeConfig(String name, Path directory, int shardCount,
                            long segmentBytes, long initialShardCapacityBytes) {
        this(name, directory, shardCount, segmentBytes, initialShardCapacityBytes, VolumeWriteMode.MMAP);
    }

    /** Configuração com defaults (64 shards, segmentos de 1 GiB, escrita {@link VolumeWriteMode#MMAP}). */
    public static BlobVolumeConfig of(String name, Path directory) {
        return new BlobVolumeConfig(name, directory, DEFAULT_SHARD_COUNT,
                DEFAULT_SEGMENT_BYTES, DEFAULT_SEGMENT_BYTES, VolumeWriteMode.MMAP);
    }
}
