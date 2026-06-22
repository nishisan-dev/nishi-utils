package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.ConflictResolution;
import dev.nishisan.utils.oss.api.NamingScheme;
import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.api.WriteMode;
import dev.nishisan.utils.oss.definition.IdempotencyDef;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import dev.nishisan.utils.oss.definition.StorageSpec;
import dev.nishisan.utils.oss.definition.WritePolicy;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageFactoryTest {

    private static StorageSpec spec(StorageBackendType backend) {
        return new StorageSpec(
                backend,
                new ObjectNaming(NamingScheme.DETERMINISTIC, "schema", "series"),
                new WritePolicy(WriteMode.APPEND_ONLY,
                        new IdempotencyDef("{seriesKey}", ConflictResolution.VERIFY_OR_REPLACE_IF_IDENTICAL)),
                null,
                null);
    }

    @Test
    void instanciaLocalDiskQuandoBackendEhLocal(@TempDir Path tempDir) {
        NgrrdStorage storage = StorageFactory.from(spec(StorageBackendType.LOCAL_DISK),
                StorageFactory.StorageBindings.forLocalDisk(tempDir));
        assertInstanceOf(LocalDiskStorage.class, storage);
    }

    @Test
    void usaInstanciaBlobCompartilhadaQuandoBackendEhShardedBlob(@TempDir Path tempDir) {
        try (BlobStorage volume = BlobStorage.create(tempDir.resolve("vol"), 4, 1L << 20, 1L << 20)) {
            NgrrdStorage storage = StorageFactory.from(spec(StorageBackendType.SHARDED_BLOB),
                    StorageFactory.StorageBindings.forBlob(volume));
            assertSame(volume, storage); // o backend blob é uma instância compartilhada, não recriada
        }
    }

    @Test
    void rejeitaQuandoBindingExigidoNaoEstaPresente() {
        assertThrows(NullPointerException.class,
                () -> StorageFactory.from(spec(StorageBackendType.LOCAL_DISK),
                        new StorageFactory.StorageBindings(null, null, null)));
        assertThrows(NullPointerException.class,
                () -> StorageFactory.from(spec(StorageBackendType.OBJECT_STORAGE),
                        new StorageFactory.StorageBindings(null, null, null)));
        assertThrows(NullPointerException.class,
                () -> StorageFactory.from(spec(StorageBackendType.SHARDED_BLOB),
                        new StorageFactory.StorageBindings(null, null, null)));
    }
}
