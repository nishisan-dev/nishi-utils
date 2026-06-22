package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Escala de threads: abrir muitos handles deve usar um pool de writers
 * compartilhado e limitado (≈ nCPU), não uma thread por série. Após fechar todos,
 * nenhuma thread {@code ngrrd-*} deve sobrar.
 */
class NgrrdWriterPoolTest {

    private static final int HANDLES = 64;

    private String loadYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static long writerThreadCount() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(n -> n.startsWith("ngrrd-writer"))
                .count();
    }

    @Test
    void manyHandlesShareABoundedWriterPool(@TempDir Path tmp) throws Exception {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tmp);
        String yaml = loadYaml();
        List<NgrrdHandle> handles = new ArrayList<>();
        try {
            for (int i = 0; i < HANDLES; i++) {
                NgrrdHandle h = Ngrrd.fromYaml(yaml, bindings,
                        Map.of("deviceId", "s" + i, "interfaceId", "e0",
                                "region", "r", "vendor", "v", "role", "core"), null);
                handles.add(h);
                h.write("in_octets", new Sample(1_747_339_200_000L, 1000L * i));
                h.flush();
            }
            long threads = writerThreadCount();
            int bound = Math.max(4, Runtime.getRuntime().availableProcessors()) + 2;
            assertTrue(threads <= bound,
                    "esperava pool limitado (<= " + bound + "), mas vi " + threads
                            + " threads de writer para " + HANDLES + " handles");
        } finally {
            for (NgrrdHandle h : handles) {
                h.close();
            }
        }

        // Após fechar todos os handles, o pool compartilhado deve encerrar.
        long deadline = System.currentTimeMillis() + 3_000L;
        while (writerThreadCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(writerThreadCount() == 0,
                "esperava nenhuma thread ngrrd-writer viva após fechar todos os handles");
    }
}
