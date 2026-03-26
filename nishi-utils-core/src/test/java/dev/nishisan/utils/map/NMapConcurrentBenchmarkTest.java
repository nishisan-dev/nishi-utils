/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Benchmark comparativo de performance de leitura/escrita concorrente
 * entre os três modos de armazenamento do NMap:
 * <ul>
 * <li><b>InMemory</b> (baseline) — {@link InMemoryStrategy}</li>
 * <li><b>Disk</b> — {@link DiskOffloadStrategy}</li>
 * <li><b>Hybrid</b> — {@link HybridOffloadStrategy} com LRU</li>
 * </ul>
 * <p>
 * Cada cenário executa 10.000 elementos com 8 threads de escritores
 * concorrentes seguido de 8 threads de leitores concorrentes, medindo
 * throughput (ops/s), tempo médio por operação e latências percentis.
 */
class NMapConcurrentBenchmarkTest {

    private static final int TOTAL_ENTRIES = 10_000;
    private static final int THREADS = 8;

    @TempDir
    Path tempDir;

    // ── Configs ─────────────────────────────────────────────────────────

    private NMapConfig inMemoryConfig() {
        return NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .build();
    }

    private NMapConfig diskConfig() {
        return NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(DiskOffloadStrategy::new)
                .build();
    }

    private NMapConfig hybridConfig(int maxInMemory) {
        return NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(new NMapOffloadStrategyFactory() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <K, V> NMapOffloadStrategy<K, V> create(Path bd, String n) {
                        return (NMapOffloadStrategy<K, V>) HybridOffloadStrategy
                                .<String, String>builder(bd, n)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(maxInMemory)
                                .build();
                    }
                })
                .build();
    }

    // ── Main Benchmark ──────────────────────────────────────────────────

    @Test
    void benchmarkAllModes() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();

        results.add(runBenchmark("InMemory", inMemoryConfig(), "bench-inmem"));
        results.add(runBenchmark("Disk", diskConfig(), "bench-disk"));
        results.add(runBenchmark("Hybrid-50%", hybridConfig(5_000), "bench-hybrid50"));
        results.add(runBenchmark("Hybrid-10%", hybridConfig(1_000), "bench-hybrid10"));

        BenchmarkResult baseline = results.get(0);

        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════════");
        System.out.println("  NMap Concurrent R/W Benchmark — " + TOTAL_ENTRIES + " entries, " + THREADS + " threads");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════════");

        // ── Write Performance ───────────────────────────────────────────
        System.out.println();
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                           WRITE PERFORMANCE                                │");
        System.out.println("  ├──────────────┬──────────┬──────────┬───────────┬───────────┬────────────────┤");
        System.out.println("  │ Mode         │ Total ms │  Avg/op  │  P50/op   │  P99/op   │ Throughput     │");
        System.out.println("  ├──────────────┼──────────┼──────────┼───────────┼───────────┼────────────────┤");
        for (BenchmarkResult r : results) {
            long opsPerSec = r.writeTotalMs > 0 ? (TOTAL_ENTRIES * 1000L) / r.writeTotalMs : 0;
            System.out.printf("  │ %-12s │ %6d ms│ %6.1f µs│ %7.1f µs│ %7.1f µs│ %,10d op/s │%n",
                    r.label, r.writeTotalMs,
                    r.writeAvgUs, r.writeP50Us, r.writeP99Us, opsPerSec);
        }
        System.out.println("  └──────────────┴──────────┴──────────┴───────────┴───────────┴────────────────┘");

        // ── Read Performance ────────────────────────────────────────────
        System.out.println();
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                           READ PERFORMANCE                                 │");
        System.out.println("  ├──────────────┬──────────┬──────────┬───────────┬───────────┬────────────────┤");
        System.out.println("  │ Mode         │ Total ms │  Avg/op  │  P50/op   │  P99/op   │ Throughput     │");
        System.out.println("  ├──────────────┼──────────┼──────────┼───────────┼───────────┼────────────────┤");
        for (BenchmarkResult r : results) {
            long opsPerSec = r.readTotalMs > 0 ? (TOTAL_ENTRIES * 1000L) / r.readTotalMs : 0;
            System.out.printf("  │ %-12s │ %6d ms│ %6.1f µs│ %7.1f µs│ %7.1f µs│ %,10d op/s │%n",
                    r.label, r.readTotalMs,
                    r.readAvgUs, r.readP50Us, r.readP99Us, opsPerSec);
        }
        System.out.println("  └──────────────┴──────────┴──────────┴───────────┴───────────┴────────────────┘");

        // ── Comparison vs Baseline ──────────────────────────────────────
        System.out.println();
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                        COMPARISON vs InMemory                              │");
        System.out.println("  ├──────────────┬────────────────────────┬────────────────────────┬───────────┤");
        System.out.println("  │ Mode         │ Write (avg ratio)      │ Read (avg ratio)       │ Verdict   │");
        System.out.println("  ├──────────────┼────────────────────────┼────────────────────────┼───────────┤");
        for (int i = 1; i < results.size(); i++) {
            BenchmarkResult r = results.get(i);
            double wRatio = baseline.writeAvgUs > 0 ? r.writeAvgUs / baseline.writeAvgUs : 0;
            double rRatio = baseline.readAvgUs > 0 ? r.readAvgUs / baseline.readAvgUs : 0;
            String verdict = (wRatio <= 50 && rRatio <= 50) ? "   OK    "
                    : (wRatio > 100 || rRatio > 100) ? " LENTO   " : " ACEITÁVEL";
            System.out.printf("  │ %-12s │ %18.1fx     │ %18.1fx     │%s│%n",
                    r.label, wRatio, rRatio, verdict);
        }
        System.out.println("  └──────────────┴────────────────────────┴────────────────────────┴───────────┘");
        System.out.println();
    }

    // ── Benchmark runner ────────────────────────────────────────────────

    private BenchmarkResult runBenchmark(String label, NMapConfig config, String mapName) throws Exception {
        // Warm-up JIT/classloading
        try (NMap<String, String> warmup = NMap.open(tempDir, mapName + "-warmup", config)) {
            for (int i = 0; i < 100; i++) {
                warmup.put("w" + i, "v" + i);
            }
            for (int i = 0; i < 100; i++) {
                warmup.get("w" + i);
            }
        }

        try (NMap<String, String> map = NMap.open(tempDir, mapName, config)) {
            // ── Concurrent Writes ───────────────────────────────────
            long[] writeLatencies = new long[TOTAL_ENTRIES];
            ExecutorService writePool = Executors.newFixedThreadPool(THREADS);
            int perThread = TOTAL_ENTRIES / THREADS;
            CountDownLatch writeLatch = new CountDownLatch(1);
            List<Future<?>> writeFutures = new ArrayList<>();

            for (int t = 0; t < THREADS; t++) {
                int start = t * perThread;
                int end = (t == THREADS - 1) ? TOTAL_ENTRIES : start + perThread;
                writeFutures.add(writePool.submit(() -> {
                    try {
                        writeLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int i = start; i < end; i++) {
                        long t0 = System.nanoTime();
                        map.put("key-" + i, "value-" + i + "-payload-data-for-benchmark");
                        writeLatencies[i] = System.nanoTime() - t0;
                    }
                }));
            }

            long writeStart = System.nanoTime();
            writeLatch.countDown();
            for (Future<?> f : writeFutures) {
                f.get();
            }
            long writeTotalMs = (System.nanoTime() - writeStart) / 1_000_000;
            writePool.shutdown();

            assertEquals(TOTAL_ENTRIES, map.size(),
                    label + ": all entries should be present after writes");

            // ── Concurrent Reads ────────────────────────────────────
            long[] readLatencies = new long[TOTAL_ENTRIES];
            ExecutorService readPool = Executors.newFixedThreadPool(THREADS);
            CountDownLatch readLatch = new CountDownLatch(1);
            List<Future<?>> readFutures = new ArrayList<>();
            LongAdder misses = new LongAdder();

            for (int t = 0; t < THREADS; t++) {
                int start = t * perThread;
                int end = (t == THREADS - 1) ? TOTAL_ENTRIES : start + perThread;
                readFutures.add(readPool.submit(() -> {
                    try {
                        readLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int i = start; i < end; i++) {
                        long t0 = System.nanoTime();
                        var val = map.get("key-" + i);
                        readLatencies[i] = System.nanoTime() - t0;
                        if (val.isEmpty()) {
                            misses.increment();
                        }
                    }
                }));
            }

            long readStart = System.nanoTime();
            readLatch.countDown();
            for (Future<?> f : readFutures) {
                f.get();
            }
            long readTotalMs = (System.nanoTime() - readStart) / 1_000_000;
            readPool.shutdown();

            assertEquals(0, misses.sum(),
                    label + ": all entries should be readable");

            return new BenchmarkResult(
                    label,
                    writeTotalMs, readTotalMs,
                    avgUs(writeLatencies), avgUs(readLatencies),
                    percentileUs(writeLatencies, 50), percentileUs(readLatencies, 50),
                    percentileUs(writeLatencies, 99), percentileUs(readLatencies, 99)
            );
        }
    }

    // ── Stats helpers ───────────────────────────────────────────────────

    private double avgUs(long[] latenciesNs) {
        long sum = 0;
        for (long l : latenciesNs) {
            sum += l;
        }
        return (sum / (double) latenciesNs.length) / 1_000.0;
    }

    private double percentileUs(long[] latenciesNs, int percentile) {
        long[] sorted = latenciesNs.clone();
        Arrays.sort(sorted);
        int idx = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, idx)] / 1_000.0;
    }

    // ── Result holder ───────────────────────────────────────────────────

    private record BenchmarkResult(
            String label,
            long writeTotalMs, long readTotalMs,
            double writeAvgUs, double readAvgUs,
            double writeP50Us, double readP50Us,
            double writeP99Us, double readP99Us) {
    }
}
