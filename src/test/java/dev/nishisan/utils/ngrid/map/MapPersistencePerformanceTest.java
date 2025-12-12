package dev.nishisan.utils.ngrid.map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Lightweight (non-assertive) benchmark. Kept disabled to avoid flakiness on CI.
 */
class MapPersistencePerformanceTest {

    @TempDir
    Path tempDir;

    @Disabled("Benchmark only - run manually")
    @Test
    void benchmarkAppendThroughput() throws Exception {
        Path baseDir = tempDir.resolve("maps");
        Map<String, String> data = new ConcurrentHashMap<>();
        MapPersistenceConfig cfg = MapPersistenceConfig.builder(baseDir, "bench")
                .mode(MapPersistenceMode.ASYNC_NO_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(500)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        int ops = 200_000;
        long start = System.nanoTime();
        try (MapPersistence<String, String> p = new MapPersistence<>(cfg, data)) {
            p.start();
            for (int i = 0; i < ops; i++) {
                p.appendAsync(MapReplicationCommandType.PUT, "k" + i, "v" + i);
            }
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("MapPersistence ASYNC_NO_FSYNC ops=" + ops + " elapsedMs=" + elapsedMs);
    }
}


