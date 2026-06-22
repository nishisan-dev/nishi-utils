package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integração end-to-end do backend sharded blob via a façade pública
 * ({@code Ngrrd.open} com locator e modo compat), exercitando o pipeline real
 * de writer/reader sobre a geometria {@code .ngrr}.
 */
class NgrrdBlobFacadeTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;

    private String loadYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private BlobVolumeRegistry registry(Path base) {
        return NgrrdBlob.registry()
                .basePath(base).shardCount(4).segmentBytes(1L << 20)
                .volume("ifaceStats")
                .build();
    }

    private static void writeRamp(NgrrdHandle handle) {
        long octets = 0L;
        for (int i = 0; i < 8; i++) {
            octets += 50_000L;
            handle.write("in_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
            handle.write("out_octets", new Sample(BLOCK_START_MS + i * STEP_MS, octets));
        }
        handle.flush();
    }

    @Test
    void writeAndReadViaLocator(@TempDir Path base) throws Exception {
        try (BlobVolumeRegistry registry = registry(base)) {
            NgrrdUri locator = NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0");
            try (NgrrdHandle handle = Ngrrd.open(registry, locator, loadYaml())) {
                assertEquals("device:r1/iface:eth0", handle.seriesKey());
                writeRamp(handle);
                SeriesResult inBps = handle.read("daily").get("in_bps");
                assertNotNull(inBps);
                assertEquals("rra_5m_30d", inBps.rraName());
            }
        }
    }

    @Test
    void closingHandleDoesNotCloseSharedVolume(@TempDir Path base) throws Exception {
        try (BlobVolumeRegistry registry = registry(base)) {
            try (NgrrdHandle a = Ngrrd.open(registry, NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:a"), loadYaml())) {
                writeRamp(a);
            } // fecha o handle A — NÃO deve fechar o volume compartilhado
            // Uma segunda série no mesmo volume deve funcionar normalmente.
            try (NgrrdHandle b = Ngrrd.open(registry, NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:b"), loadYaml())) {
                writeRamp(b);
                assertNotNull(b.read("daily").get("in_bps"));
            }
        }
    }

    @Test
    void compatModeViaTagsAndForBlobBinding(@TempDir Path base) throws Exception {
        Map<String, String> tags = Map.of(
                "deviceId", "r9", "interfaceId", "eth3",
                "region", "br-sp", "vendor", "x", "role", "core");
        try (BlobVolumeRegistry registry = registry(base)) {
            var bindings = registry.require("ifaceStats").bindings();
            try (NgrrdHandle handle = Ngrrd.fromYaml(loadYaml(), bindings, tags, null)) {
                assertEquals("device:r9/iface:eth3", handle.seriesKey());
                writeRamp(handle);
                assertNotNull(handle.read("daily").get("in_bps"));
            }
            // volume continua utilizável após fechar o handle (binding compartilhado)
            assertNotNull(registry.require("ifaceStats").storage().get("series/device:r9/iface:eth3.ngrr").orElse(null));
        }
    }
}
