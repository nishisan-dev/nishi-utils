package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.map.NMap;
import dev.nishisan.utils.map.NMapConfig;
import dev.nishisan.utils.map.NMapOperationType;
import dev.nishisan.utils.map.NMapPersistence;
import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/** Opt-in measurements, with real catalog values. No hardware-dependent timing assertions. */
@EnabledIfSystemProperty(named = "ngrrd.catalog.benchmark", matches = "true")
public class NMapCatalogPersistenceBenchmarkTest {
    public static void main(String[] args) throws Exception {
        var benchmark = new NMapCatalogPersistenceBenchmarkTest();
        benchmark.asyncAdmission();
        benchmark.periodicSnapshot();
    }

    @Test void asyncAdmission() throws Exception {
        for (var mode : new NMapPersistenceMode[]{NMapPersistenceMode.ASYNC_WITH_FSYNC,
                NMapPersistenceMode.ASYNC_NO_FSYNC}) {
            for (int round = 1; round <= 3; round++) {
                Path directory = Files.createTempDirectory("catalog-admission-");
                try {
                    var config = NMapConfig.builder().mode(mode).snapshotIntervalOperations(0)
                            .snapshotIntervalTime(Duration.ZERO).batchSize(100)
                            .batchTimeout(Duration.ofMillis(10)).build();
                    try (NMap<String, SeriesPlacement> map = NMap.open(directory, "catalog", config)) {
                        int count = 20_000;
                        var values = new SeriesPlacement[count];
                        var keys = new String[count];
                        for (int i = 0; i < count; i++) { keys[i] = key(i); values[i] = placement(i); }
                        for (int i = 0; i < 2_000; i++) map.put(keys[i], values[i]);
                        long[] latencies = new long[count];
                        long begin = System.nanoTime();
                        for (int i = 0; i < count; i++) {
                            long start = System.nanoTime();
                            map.put(keys[i], values[i]);
                            latencies[i] = System.nanoTime() - start;
                        }
                        long elapsed = System.nanoTime() - begin;
                        Arrays.sort(latencies);
                        System.out.printf("CATALOG_ADMISSION mode=%s round=%d entries=%d meanUs=%.3f p99Us=%.3f maxUs=%.3f%n",
                                mode, round, count, elapsed / (count * 1000.0), latencies[count * 99 / 100] / 1000.0,
                                latencies[count - 1] / 1000.0);
                    }
                } finally { deleteTree(directory); }
            }
        }
    }

    @Test void periodicSnapshot() throws Exception {
        int count = Integer.getInteger("ngrrd.catalog.benchmark.entries", 900_000);
        for (int round = 1; round <= 2; round++) {
            Path directory = Files.createTempDirectory("catalog-snapshot-");
            Map<String, SeriesPlacement> state = new ConcurrentHashMap<>();
            for (int i = 0; i < count; i++) state.put(key(i), placement(i));
            var config = NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                    .snapshotIntervalOperations(Integer.MAX_VALUE).snapshotIntervalTime(Duration.ZERO)
                    .batchSize(100).batchTimeout(Duration.ofMillis(10)).build();
            var mutationLock = new ReentrantLock();
            var persistence = new NMapPersistence<>(config, state, directory, "catalog", mutationLock);
            persistence.start();
            // Trigger exactly one periodic checkpoint without pre-writing 900k WAL entries.
            var operations = NMapPersistence.class.getDeclaredField("opsSinceSnapshot");
            operations.setAccessible(true);
            operations.setLong(persistence, Integer.MAX_VALUE);
            var done = new AtomicBoolean();
            var latencies = new ArrayList<Long>();
            Thread mutator = Thread.ofPlatform().start(() -> {
                while (!done.get()) {
                    long start = System.nanoTime();
                    mutationLock.lock();
                    try {
                        var value = placement(0);
                        state.put(key(0), value);
                        persistence.appendAsync(NMapOperationType.PUT, key(0), value);
                    } finally { mutationLock.unlock(); }
                    latencies.add(System.nanoTime() - start);
                    try { Thread.sleep(1); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                }
            });
            try {
                long begin = System.nanoTime();
                persistence.maybeSnapshot();
                // If the writer captured the checkpoint first, await the same checkpoint's completion.
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (!Files.exists(directory.resolve("catalog/snapshot.dat"))) {
                    if (System.nanoTime() >= deadline) throw new IllegalStateException("Checkpoint did not complete");
                    Thread.sleep(1);
                }
                // A rename precedes the final durability steps. Await the checkpoint owner as well.
                ReentrantLock completionLock = mutationLock;
                try {
                    var guard = NMapPersistence.class.getDeclaredField("checkpointLock");
                    guard.setAccessible(true);
                    completionLock = (ReentrantLock) guard.get(persistence);
                } catch (NoSuchFieldException published8110) { /* 8.11.0 holds mutationLock throughout. */ }
                completionLock.lock();
                completionLock.unlock();
                done.set(true);
                mutator.join();
                long elapsed = System.nanoTime() - begin;
                latencies.sort(Comparator.naturalOrder());
                System.out.printf("CATALOG_PERIODIC round=%d entries=%d snapshotBytes=%d elapsedMs=%.3f writes=%d mutationP99Ms=%.3f mutationMaxMs=%.3f%n",
                        round, count, Files.size(directory.resolve("catalog/snapshot.dat")), elapsed / 1_000_000.0,
                        latencies.size(), latencies.get(latencies.size() * 99 / 100) / 1_000_000.0,
                        latencies.get(latencies.size() - 1) / 1_000_000.0);
            } finally {
                done.set(true); mutator.join(); persistence.close(); deleteTree(directory);
            }
        }
    }

    private static String key(int i) { return "router-" + (i / 1000) + "|interface-" + i + "|ifHCInOctets"; }
    private static SeriesPlacement placement(int i) {
        return new SeriesPlacement("storage-" + (i % 3), null, PlacementState.ACTIVE, null,
                1_790_000_000_000L, 1_790_000_000_000L, "geometry-traffic", true, "interface-traffic",
                "generation-" + i, null);
    }
    private static void deleteTree(Path directory) throws Exception {
        try (var files = Files.walk(directory)) {
            for (var path : files.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
