package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Tipos de backend de armazenamento suportados pelo ngrrd.
 */
public enum StorageBackendType {
    /** Disco local (java.nio.Files). */
    LOCAL_DISK,
    /** Object storage compatível com S3 (AWS S3, MinIO, Ceph RGW). */
    OBJECT_STORAGE,
    /** Volume "sharded blob" local: N shards mmap, uma região por série. */
    SHARDED_BLOB;

    @JsonCreator
    public static StorageBackendType from(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.trim()) {
            case "localDisk", "local_disk", "LOCAL_DISK" -> LOCAL_DISK;
            case "objectStorage", "object_storage", "OBJECT_STORAGE", "s3", "S3" -> OBJECT_STORAGE;
            case "shardedBlob", "sharded_blob", "SHARDED_BLOB", "blob" -> SHARDED_BLOB;
            default -> StorageBackendType.valueOf(value.trim().toUpperCase());
        };
    }
}
