package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.definition.StorageSpec;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Fábrica de {@link NgrrdStorage} para uso pelo {@code Ngrrd.fromYaml(...)}.
 *
 * <p>A definição YAML especifica apenas o tipo do backend (campo
 * {@code spec.storage.backend}); parâmetros de conexão (root dir do disco,
 * bucket/endpoint S3, credenciais) são fornecidos explicitamente via
 * {@link StorageBindings} para evitar vazar credenciais para o arquivo de
 * definição.</p>
 */
public final class StorageFactory {

    private StorageFactory() {
    }

    public static NgrrdStorage from(StorageSpec spec, StorageBindings bindings) {
        Objects.requireNonNull(spec, "spec é obrigatório");
        Objects.requireNonNull(bindings, "bindings é obrigatório");
        StorageBackendType backend = Objects.requireNonNull(spec.backend(), "spec.backend é obrigatório");
        return switch (backend) {
            case LOCAL_DISK -> localDisk(Objects.requireNonNull(bindings.localRoot(),
                    "bindings.localRoot é obrigatório para backend LOCAL_DISK"));
            case OBJECT_STORAGE -> new S3Storage(Objects.requireNonNull(bindings.s3Settings(),
                    "bindings.s3Settings é obrigatório para backend OBJECT_STORAGE"));
            case SHARDED_BLOB -> Objects.requireNonNull(bindings.blobVolume(),
                    "bindings.blobVolume é obrigatório para backend SHARDED_BLOB");
        };
    }

    public static NgrrdStorage localDisk(Path rootDir) {
        return new LocalDiskStorage(rootDir);
    }

    public static NgrrdStorage s3(S3Settings settings) {
        return new S3Storage(settings);
    }

    /**
     * Parâmetros de conexão por backend. Apenas o campo correspondente ao backend
     * efetivamente usado precisa estar populado. O backend {@code SHARDED_BLOB}
     * recebe uma instância de {@link BlobStorage} já aberta e <strong>compartilhada</strong>
     * (um volume vive muito além de uma série; não é recriado por {@code fromYaml}).
     */
    public record StorageBindings(Path localRoot, S3Settings s3Settings, BlobStorage blobVolume) {

        public static StorageBindings forLocalDisk(Path rootDir) {
            return new StorageBindings(rootDir, null, null);
        }

        public static StorageBindings forS3(S3Settings settings) {
            return new StorageBindings(null, settings, null);
        }

        public static StorageBindings forBlob(BlobStorage volume) {
            return new StorageBindings(null, null, volume);
        }
    }
}
