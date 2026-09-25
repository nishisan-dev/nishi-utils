package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.api.*;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ContinuousIngestionRebalanceClusterTest {
    private static final long START = 1_747_339_200_000L;
    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata: {name: continuous-ingestion}
            spec:
              time: {baseStepSec: 1}
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags: [{name: id}]
              dataSources:
                - {name: value, type: GAUGE, heartbeatSec: 10}
              archives:
                rras:
                  - {name: raw, stepSec: 1, rows: 32768, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: blob
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    @Test
    void eightMigrationsKeepIngestingFiveThousandSamplesPerSecond(@TempDir Path base) throws Exception {
        try (var harness = NgrrdClusterTestHarness.start(base, 3, b -> b.rebalanceEnabled(false)
                .distributionMode(DistributionMode.CAPACITY).capacityBytes(1024L * 1024 * 1024)
                .maxConcurrentMigrations(8).migrationBytesPerSecond(1024 * 1024)
                .migrationStatusPollInterval(Duration.ofMillis(50)))) {
            harness.awaitNodeStatuses(3);
            try (var client = harness.connectClient(b -> b.batchMaxSamples(500).batchMaxDelay(Duration.ofMillis(20))
                    .requestTimeout(Duration.ofSeconds(5)).retryTimeout(Duration.ofSeconds(30))
                    .closeTimeout(Duration.ofSeconds(30)))) {
                List<NgrrdHandle> handles = new ArrayList<>();
                for (int i = 0; i < 32; i++) { handles.add(client.open(YAML, Map.of("id", Integer.toString(i)))); }
                var catalog = harness.leaderNode().catalog();
                var byOwner = handles.stream().collect(Collectors.groupingBy(
                        h -> catalog.placementStrong(h.seriesKey()).orElseThrow().ownerNodeId()));
                var source = byOwner.entrySet().stream().max(Comparator.comparingInt(e -> e.getValue().size())).orElseThrow();
                assertTrue(source.getValue().size() >= 9);
                String destination = harness.nodes().stream().map(n -> n.node().transport().local().nodeId().value())
                        .filter(id -> !id.equals(source.getKey())).findFirst().orElseThrow();
                int[] counts = new int[handles.size()];
                List<Long> baselineLatency = new ArrayList<>(), migrationLatency = new ArrayList<>();
                AtomicInteger phase = new AtomicInteger();
                var observing = new java.util.concurrent.atomic.AtomicBoolean(true);
                List<Long> baselineAckLatency = new ArrayList<>(), migrationAckLatency = new ArrayList<>();
                NgrrdHandle probe = source.getValue().get(8);
                NgrrdHandle movingProbe = source.getValue().getFirst();
                List<Long> movingAckLatency = new ArrayList<>();
                try (var workers = Executors.newFixedThreadPool(3)) {
                    var ackObserver = workers.submit(() -> {
                        while (observing.get()) {
                            long before = System.nanoTime();
                            probe.checkpoint();
                            (phase.get() == 0 ? baselineAckLatency : migrationAckLatency).add(System.nanoTime() - before);
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                        }
                    });
                    var movingObserver = workers.submit(() -> {
                        while (observing.get()) {
                            long before = System.nanoTime();
                            movingProbe.checkpoint();
                            if (phase.get() != 0) { movingAckLatency.add(System.nanoTime() - before); }
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                        }
                    });
                    var producer = workers.submit(() -> {
                        long start = System.nanoTime();
                        for (int batch = 0; batch < 1_000 && !Thread.currentThread().isInterrupted(); batch++) {
                            for (int j = 0; j < 100; j++) {
                                int index = (batch * 100 + j) % handles.size();
                                int sequence = ++counts[index];
                                long before = System.nanoTime();
                                handles.get(index).write("value", new Sample(START + sequence * 1000L, sequence));
                                (phase.get() == 0 ? baselineLatency : migrationLatency).add(System.nanoTime() - before);
                            }
                            long remaining = start + (batch + 1) * 20_000_000L - System.nanoTime();
                            if (remaining > 0) { LockSupport.parkNanos(remaining); }
                        }
                    });
                    try {
                        Thread.sleep(5_000);
                        long baselineSent = client.metrics().samplesSent();
                        assertTrue(baselineSent >= 20_000, "baseline must sustain the offered 5k/s load");
                        phase.set(1);
                        var coordinator = harness.leaderNode().migrationCoordinator();
                        var moves = source.getValue().stream().limit(8)
                                .map(h -> coordinator.migrate(h.seriesKey(), source.getKey(), destination)).toList();
                        long previous = baselineSent;
                        for (int second = 0; second < 10; second++) {
                            Thread.sleep(1_000);
                            long sent = client.metrics().samplesSent();
                            assertTrue(sent > previous, "ACK progress stopped during concurrent migration and ingestion");
                            previous = sent;
                        }
                        for (var move : moves) {
                            var result = move.get(15, TimeUnit.SECONDS);
                            assertEquals(MigrationOutcome.COMPLETED, result.outcome(), result.reason());
                        }
                        producer.get(15, TimeUnit.SECONDS);
                        observing.set(false);
                        ackObserver.get(5, TimeUnit.SECONDS);
                        movingObserver.get(5, TimeUnit.SECONDS);
                        client.flushAll();
                        assertEquals(100_000, client.metrics().samplesSent());
                        assertEquals(0, client.metrics().samplesFailed());
                        for (int i = 0; i < handles.size(); i++) {
                            var handle = handles.get(i);
                            int count = counts[i];
                            handle.write("value", new Sample(START + (count + 1) * 1000L, count + 1));
                            handle.checkpoint();
                            var result = handle.read("value", new ViewQuery(Duration.ofSeconds(count + 2), 1,
                                    ConsolidationFunction.AVERAGE, count + 2), START + (count + 2) * 1000L);
                            var values = result.points().stream().filter(p -> p.tsEpochMs() >= START + 1000L
                                    && p.tsEpochMs() <= START + count * 1000L).map(p -> p.value()).toList();
                            assertEquals(java.util.stream.IntStream.rangeClosed(1, count).mapToObj(n -> (double) n).toList(),
                                    values, "missing/reordered samples in " + handle.seriesKey());
                        }
                        long baselineP99 = p99(baselineLatency), migrationP99 = p99(migrationLatency);
                        System.out.printf("LIVE_INGESTION samples=100000 migrations=8 baselineAckRate=%.0f/s admissionP99Baseline=%.3fms admissionP99Migration=%.3fms%n",
                                baselineSent / 5.0, baselineP99 / 1_000_000.0, migrationP99 / 1_000_000.0);
                        long baselineAckP99 = p99(baselineAckLatency), migrationAckP99 = p99(migrationAckLatency);
                        System.out.printf("LIVE_ACK checkpointP99Baseline=%.3fms checkpointP99Migration=%.3fms%n",
                                baselineAckP99 / 1_000_000.0, migrationAckP99 / 1_000_000.0);
                        long movingMax = Collections.max(movingAckLatency);
                        System.out.printf("LIVE_MOVING_ACK checkpointP99=%.3fms checkpointMax=%.3fms%n",
                                p99(movingAckLatency) / 1_000_000.0, movingMax / 1_000_000.0);
                        assertTrue(movingMax < TimeUnit.SECONDS.toNanos(3), "moving-series cutover stalled ingestion");
                        assertTrue(migrationP99 < TimeUnit.MILLISECONDS.toNanos(20), "ingestion admissions stalled");
                        assertTrue(migrationAckP99 < Math.max(TimeUnit.MILLISECONDS.toNanos(250), baselineAckP99 * 3),
                                "healthy-series ACK/checkpoint latency degraded during migrations");
                    } finally {
                        observing.set(false);
                        producer.cancel(true);
                        ackObserver.cancel(true);
                        movingObserver.cancel(true);
                    }
                }
            }
        }
    }

    private static long p99(List<Long> samples) {
        samples.sort(Long::compare);
        return samples.get(Math.min(samples.size() - 1, (int) (samples.size() * 0.99)));
    }
}
