package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NgrrdFacadeTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;

    private String loadYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private Map<String, String> sampleTags() {
        return Map.of(
                "deviceId", "r1",
                "interfaceId", "eth0",
                "region", "br-sp",
                "vendor", "x",
                "role", "core");
    }

    @Test
    void fromYamlWriteEReadDailyComShutdownOrdenado(@TempDir Path tempDir) throws Exception {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);

        try (NgrrdHandle handle = Ngrrd.fromYaml(loadYaml(), bindings, sampleTags(), null)) {
            assertEquals("device:r1/iface:eth0", handle.seriesKey());

            long octets = 0L;
            for (int i = 0; i < 8; i++) {
                octets += 50_000L;
                handle.write("in_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
                handle.write("out_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
            }
            handle.flush();

            SeriesResult inBps = handle.read("daily").get("in_bps");
            assertNotNull(inBps);
            assertEquals("rra_5m_30d", inBps.rraName());
        }

        // após close(), as threads do writer e do scheduler devem ter terminado.
        // Concede uma janela curta para os Executors finalizarem cleanup.
        long deadline = System.currentTimeMillis() + 2_000L;
        Set<String> activeNgrrdThreads;
        do {
            activeNgrrdThreads = Thread.getAllStackTraces().keySet().stream()
                    .filter(Thread::isAlive)
                    .map(Thread::getName)
                    .filter(n -> n.startsWith("ngrrd-"))
                    .collect(java.util.stream.Collectors.toSet());
            if (activeNgrrdThreads.isEmpty()) {
                break;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);
        assertFalse(!activeNgrrdThreads.isEmpty(),
                "esperava nenhuma thread ngrrd-* viva após close(): " + activeNgrrdThreads);
    }

    @Test
    void rejeitaTagsIncompletas(@TempDir Path tempDir) throws Exception {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        assertThrows(IllegalArgumentException.class,
                () -> Ngrrd.fromYaml(loadYaml(), bindings, Map.of("deviceId", "r1"), null));
    }

    @Test
    void apiVersionEstavel() {
        assertEquals("ngrrd/v1", Ngrrd.apiVersion());
    }

    @Test
    void resolveSeriesKeyExpandeTemplateCorretamente() {
        String key = Ngrrd.resolveSeriesKey(
                "device:{deviceId}/iface:{interfaceId}",
                Map.of("deviceId", "r1", "interfaceId", "eth0"));
        assertEquals("device:r1/iface:eth0", key);
    }
}
