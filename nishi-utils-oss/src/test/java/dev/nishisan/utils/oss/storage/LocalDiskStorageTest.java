package dev.nishisan.utils.oss.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalDiskStorageTest {

    @Test
    void putEGetRoundTrip(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        byte[] payload = {1, 2, 3, 4};

        storage.put("raw/k1/in/300/100.ngrrd", payload);
        Optional<byte[]> read = storage.get("raw/k1/in/300/100.ngrrd");

        assertTrue(read.isPresent());
        assertArrayEquals(payload, read.get());
    }

    @Test
    void getDeKeyInexistenteRetornaEmpty(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        assertTrue(storage.get("nao/existe").isEmpty());
    }

    @Test
    void existsRespondeCorretamente(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        assertFalse(storage.exists("x"));
        storage.put("x", new byte[]{0});
        assertTrue(storage.exists("x"));
    }

    @Test
    void deleteRemoveArquivo(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        storage.put("y", new byte[]{1});
        storage.delete("y");
        assertFalse(storage.exists("y"));
        storage.delete("y"); // no-op
    }

    @Test
    void listEnumeraChavesAbaixoDoPrefixo(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        storage.put("raw/s/in/300/100.ngrrd", new byte[]{0});
        storage.put("raw/s/in/300/400.ngrrd", new byte[]{0});
        storage.put("manifest/s/v1.yaml", new byte[]{0});

        List<String> raw = storage.list("raw");
        assertEquals(2, raw.size());
        assertTrue(raw.contains("raw/s/in/300/100.ngrrd"));
        assertTrue(raw.contains("raw/s/in/300/400.ngrrd"));
    }

    @Test
    void listEmPrefixoInexistenteRetornaListaVazia(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        assertTrue(storage.list("vazio").isEmpty());
    }

    @Test
    void atomicReplaceNuncaDeixaResiduosTmpNoDiretorio(@TempDir Path tempDir) throws Exception {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        Path dir = tempDir.resolve("agg/k/r/300");

        storage.atomicReplace("agg/k/r/300/1.ngrrd", new byte[]{9, 9, 9});
        storage.atomicReplace("agg/k/r/300/1.ngrrd", new byte[]{1, 2, 3});

        assertArrayEquals(new byte[]{1, 2, 3}, storage.get("agg/k/r/300/1.ngrrd").orElseThrow());
        long tmpResiduals = Files.list(dir)
                .filter(p -> p.getFileName().toString().startsWith(".ngrrd-tmp-"))
                .count();
        assertEquals(0, tmpResiduals);
    }

    @Test
    void verifyOrReplaceMarcaWrittenIdenticalEReplaced(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        byte[] v1 = {1, 1, 1};
        byte[] v2 = {2, 2, 2};

        assertEquals(VerifyResult.WRITTEN, storage.verifyOrReplaceIfIdentical("k", v1));
        assertEquals(VerifyResult.IDENTICAL_SKIPPED, storage.verifyOrReplaceIfIdentical("k", v1));
        assertEquals(VerifyResult.REPLACED, storage.verifyOrReplaceIfIdentical("k", v2));
        assertArrayEquals(v2, storage.get("k").orElseThrow());
    }

    @Test
    void rejeitaKeyComBarraInicialOuTravessia(@TempDir Path tempDir) {
        NgrrdStorage storage = new LocalDiskStorage(tempDir);
        assertThrows(IllegalArgumentException.class, () -> storage.put("/abs", new byte[]{0}));
        assertThrows(IllegalArgumentException.class, () -> storage.put("../escape", new byte[]{0}));
    }
}
