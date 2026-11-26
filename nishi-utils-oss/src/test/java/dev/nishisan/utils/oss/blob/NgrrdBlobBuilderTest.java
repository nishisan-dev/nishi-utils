package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Builder do NgrrdBlob: propaga listeners globais (escopo de registro) a cada volume. */
class NgrrdBlobBuilderTest {

    @Test
    void propagaListenersGlobaisParaVolume(@TempDir Path base) {
        NgrrdMetricsListener quality = new NgrrdMetricsListener() {
        };
        BlobVolumeMetricsListener volume = new BlobVolumeMetricsListener() {
        };
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .qualityListener(quality)
                .volumeMetricsListener(volume)
                .volume("ifaceStats")
                .build()) {
            BlobVolume v = reg.require("ifaceStats");
            assertSame(quality, v.qualityListener(), "mesma referência, sem reembrulho");
            assertSame(volume, v.volumeMetricsListener(), "mesma referência, sem reembrulho");
        }
    }

    @Test
    void semListenersOsAcessoresRetornamNull(@TempDir Path base) {
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .volume("ifaceStats")
                .build()) {
            BlobVolume v = reg.require("ifaceStats");
            assertNull(v.qualityListener());
            assertNull(v.volumeMetricsListener());
            assertNull(v.checkpointForceExecutor(), "sem executor, o force do checkpoint é inline");
        }
    }

    @Test
    void propagaExecutorDeForceParaVolume(@TempDir Path base) {
        Executor forceExecutor = Runnable::run;
        try (BlobVolumeRegistry reg = NgrrdBlob.registry()
                .basePath(base).shardCount(2).segmentBytes(1L << 20)
                .checkpointForceExecutor(forceExecutor)
                .volume("ifaceStats")
                .build()) {
            assertSame(forceExecutor, reg.require("ifaceStats").checkpointForceExecutor());
        }
    }
}
