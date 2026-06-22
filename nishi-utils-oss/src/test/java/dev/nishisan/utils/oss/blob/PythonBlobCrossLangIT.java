package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Paridade cross-language Java↔Python do formato de blob volume. Pula
 * automaticamente quando {@code python3} ou o pacote {@code ngrrd_python} não
 * estão disponíveis (rode com {@code mvn verify -Pblob-crosslang} num ambiente
 * com Python). Ver {@code doc/oss/ngrrd-blob-volume.md}.
 */
class PythonBlobCrossLangIT {

    private static Path pythonSrc;
    private static final long SEG = 1L << 20;
    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;

    @BeforeAll
    static void detectPython() {
        pythonSrc = locatePythonSrc();
        assumeTrue(pythonSrc != null, "python/ngrrd-python/src não encontrado");
        int exit = run(pythonSrc, "python3", "-c", "import ngrrd_python.blob").exit;
        assumeTrue(exit == 0, "python3 + pacote ngrrd_python indisponíveis");
    }

    private String localDiskYaml() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private String blobYaml() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Map<String, String> tags(String device, String iface) {
        return Map.of("deviceId", device, "interfaceId", iface,
                "region", "br-sp", "vendor", "x", "role", "core");
    }

    private void writeRamp(NgrrdHandle handle) {
        long octets = 0L;
        for (int i = 0; i < 8; i++) {
            octets += 50_000L;
            handle.write("in_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
            handle.write("out_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
        }
        handle.flush();
    }

    @Test
    void pythonMigratesAndJavaReadsByteForByte(@TempDir Path tmp) throws Exception {
        Path sourceRoot = tmp.resolve("src");
        Path volumeDir = tmp.resolve("vol");

        // 1) Java gera arquivos .ngrr reais (backend localDisk).
        String[][] series = {{"r1", "eth0"}, {"r2", "eth1"}, {"r3", "eth2"}};
        StorageFactory.StorageBindings disk = StorageFactory.StorageBindings.forLocalDisk(sourceRoot);
        for (String[] s : series) {
            try (NgrrdHandle h = Ngrrd.fromYaml(localDiskYaml(), disk, tags(s[0], s[1]), null)) {
                writeRamp(h);
            }
        }

        // 2) Python migra o diretório para um blob volume.
        Proc migrate = run(pythonSrc, "python3", "-m", "ngrrd_python.blob.cli", "migrate",
                sourceRoot.toString(), volumeDir.toString(),
                "--shards", "4", "--segment-bytes", String.valueOf(SEG),
                "--initial-capacity", String.valueOf(SEG), "--verify");
        assertEquals(0, migrate.exit, () -> "migração Python falhou:\n" + migrate.output);

        // 3) Java abre o volume escrito pelo Python e confere paridade byte-a-byte.
        try (BlobStorage blob = BlobStorage.open(volumeDir)) {
            for (String[] s : series) {
                String key = "series/device:" + s[0] + "/iface:" + s[1] + ".ngrr";
                byte[] original = Files.readAllBytes(sourceRoot.resolve(key));
                byte[] fromBlob = blob.get(key).orElseThrow(() -> new AssertionError("ausente: " + key));
                assertArrayEquals(original, fromBlob, "conteúdo divergente para " + key);
            }
        }
    }

    @Test
    void javaWritesAndPythonVerifies(@TempDir Path base) throws Exception {
        // 1) Java cria o volume e escreve uma série pelo pipeline real.
        try (BlobVolumeRegistry registry = NgrrdBlob.registry()
                .basePath(base).shardCount(4).segmentBytes(SEG).volume("ifaceStats").build()) {
            try (NgrrdHandle h = Ngrrd.open(registry,
                    NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0"), blobYaml())) {
                writeRamp(h);
            }
        } // close do registry -> checkpoint do catálogo (catalog.bin durável)

        // 2) Python verifica o volume escrito pelo Java.
        Path volumeDir = base.resolve("ifaceStats");
        Proc verify = run(pythonSrc, "python3", "-m", "ngrrd_python.blob.cli", "verify",
                volumeDir.toString());
        assertEquals(0, verify.exit, () -> "verify Python falhou:\n" + verify.output);
        assertTrue(verify.output.contains("healthy=True"), () -> "esperava healthy=True:\n" + verify.output);
    }

    // ------------------------------------------------------------------ helpers

    private record Proc(int exit, String output) {
    }

    private static Proc run(Path pythonPath, String... command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        if (pythonPath != null) {
            pb.environment().put("PYTHONPATH", pythonPath.toString());
        }
        try {
            Process p = pb.start();
            String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!p.waitFor(120, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return new Proc(-1, output + "\n[timeout]");
            }
            return new Proc(p.exitValue(), output);
        } catch (IOException | InterruptedException e) {
            return new Proc(-1, "falha ao executar " + String.join(" ", command) + ": " + e);
        }
    }

    private static Path locatePythonSrc() {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++) {
            Path candidate = dir.resolve("python/ngrrd-python/src");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        return null;
    }
}
