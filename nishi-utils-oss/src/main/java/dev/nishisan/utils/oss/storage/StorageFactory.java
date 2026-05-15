package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.definition.StorageSpec;

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
     * efetivamente usado precisa estar populado.
     */
    public record StorageBindings(Path localRoot, S3Settings s3Settings) {

        public static StorageBindings forLocalDisk(Path rootDir) {
            return new StorageBindings(rootDir, null);
        }

        public static StorageBindings forS3(S3Settings settings) {
            return new StorageBindings(null, settings);
        }
    }
}
