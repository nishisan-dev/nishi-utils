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

package dev.nishisan.utils.ngrid.soak;

import dev.nishisan.utils.ngrid.metrics.NGridOperationalSnapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Collects metrics snapshots and churn events during a soak test run,
 * then generates a Markdown report summarizing the results.
 *
 * <p>
 * Thread-safe: designed to be called from multiple soak test threads.
 * </p>
 *
 * @since 2.1.0
 */
public final class SoakTestReporter {

    /** Periodic operational snapshots. */
    private final CopyOnWriteArrayList<NGridOperationalSnapshot> snapshots = new CopyOnWriteArrayList<>();

    /** Churn events (leader kill/restart). */
    private final CopyOnWriteArrayList<ChurnEvent> churnEvents = new CopyOnWriteArrayList<>();

    /** Recorded offer latencies in nanoseconds. */
    private final CopyOnWriteArrayList<Long> offerLatenciesNanos = new CopyOnWriteArrayList<>();

    private final Instant startTime;
    private volatile Instant endTime;

    // Final counters
    private volatile long totalOffered;
    private volatile long totalConsumed;
    private volatile long duplicateCount;
    private volatile long lostCount;
    private volatile boolean passed;

    public SoakTestReporter() {
        this.startTime = Instant.now();
    }

    /** Records an operational snapshot captured during the test. */
    public void recordSnapshot(NGridOperationalSnapshot snapshot) {
        snapshots.add(snapshot);
    }

    /** Records a leader churn event (kill or restart). */
    public void recordChurnEvent(String type, String nodeId) {
        churnEvents.add(new ChurnEvent(Instant.now(), type, nodeId));
    }

    /** Records an offer latency measurement. */
    public void recordOfferLatency(long nanos) {
        offerLatenciesNanos.add(nanos);
    }

    /** Sets the final summary counters. */
    public void setFinalResults(long offered, long consumed, long duplicates, long lost, boolean pass) {
        this.totalOffered = offered;
        this.totalConsumed = consumed;
        this.duplicateCount = duplicates;
        this.lostCount = lost;
        this.passed = pass;
        this.endTime = Instant.now();
    }

    /**
     * Generates the soak test report as a Markdown string.
     */
    public String generateReport() {
        Duration totalDuration = Duration.between(startTime, endTime != null ? endTime : Instant.now());

        StringBuilder sb = new StringBuilder();
        sb.append("# NGrid Soak Test Report\n\n");
        sb.append("**Status:** ").append(passed ? "✅ PASSED" : "❌ FAILED").append("\n\n");

        // ── Summary
        sb.append("## Summary\n\n");
        sb.append("| Metric | Value |\n");
        sb.append("|---|---|\n");
        sb.append("| Duration | ").append(formatDuration(totalDuration)).append(" |\n");
        sb.append("| Total Offered | ").append(totalOffered).append(" |\n");
        sb.append("| Total Consumed | ").append(totalConsumed).append(" |\n");
        sb.append("| Duplicates | ").append(duplicateCount).append(" |\n");
        sb.append("| Lost Messages | ").append(lostCount).append(" |\n");
        sb.append("| Churn Events | ").append(churnEvents.size()).append(" |\n");
        sb.append("| Snapshots Captured | ").append(snapshots.size()).append(" |\n\n");

        // ── Latency
        if (!offerLatenciesNanos.isEmpty()) {
            sb.append("## Offer Latency\n\n");
            List<Long> sorted = new ArrayList<>(offerLatenciesNanos);
            Collections.sort(sorted);
            long p50 = percentile(sorted, 50);
            long p95 = percentile(sorted, 95);
            long p99 = percentile(sorted, 99);
            LongSummaryStatistics stats = sorted.stream().mapToLong(Long::longValue).summaryStatistics();

            sb.append("| Percentile | Value |\n");
            sb.append("|---|---|\n");
            sb.append("| Min | ").append(formatNanos(stats.getMin())).append(" |\n");
            sb.append("| P50 | ").append(formatNanos(p50)).append(" |\n");
            sb.append("| P95 | ").append(formatNanos(p95)).append(" |\n");
            sb.append("| P99 | ").append(formatNanos(p99)).append(" |\n");
            sb.append("| Max | ").append(formatNanos(stats.getMax())).append(" |\n");
            sb.append("| Count | ").append(stats.getCount()).append(" |\n\n");
        }

        // ── Replication Health (from last 10 snapshots)
        if (!snapshots.isEmpty()) {
            sb.append("## Replication Health (Last 10 Snapshots)\n\n");
            List<NGridOperationalSnapshot> tail = snapshots.subList(
                    Math.max(0, snapshots.size() - 10), snapshots.size());

            sb.append("| Time | Leader | Epoch | Active | Lag | Gaps | Resend | Fallback | Pending |\n");
            sb.append("|---|---|---|---|---|---|---|---|---|\n");
            for (NGridOperationalSnapshot s : tail) {
                sb.append("| ").append(s.capturedAt()).append(" | ");
                sb.append(s.leaderId()).append(" | ");
                sb.append(s.leaderEpoch()).append(" | ");
                sb.append(s.activeMembersCount()).append(" | ");
                sb.append(s.replicationLag()).append(" | ");
                sb.append(s.gapsDetected()).append(" | ");
                sb.append(s.resendSuccessCount()).append(" | ");
                sb.append(s.snapshotFallbackCount()).append(" | ");
                sb.append(s.pendingOperationsCount()).append(" |\n");
            }
            sb.append("\n");
        }

        // ── Churn Events
        if (!churnEvents.isEmpty()) {
            sb.append("## Churn Events\n\n");
            sb.append("| Time | Type | Node |\n");
            sb.append("|---|---|---|\n");
            for (ChurnEvent event : churnEvents) {
                sb.append("| ").append(event.timestamp()).append(" | ");
                sb.append(event.type()).append(" | ");
                sb.append(event.nodeId()).append(" |\n");
            }
            sb.append("\n");
        }

        sb.append("---\n");
        sb.append("*Report generated at ").append(Instant.now()).append("*\n");

        return sb.toString();
    }

    /**
     * Writes the report to the specified file path.
     */
    public void writeReport(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, generateReport());
    }

    // ── Internal Helpers ──

    private static long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty())
            return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static String formatNanos(long nanos) {
        if (nanos < 1_000)
            return nanos + " ns";
        if (nanos < 1_000_000)
            return String.format("%.1f µs", nanos / 1_000.0);
        if (nanos < 1_000_000_000)
            return String.format("%.1f ms", nanos / 1_000_000.0);
        return String.format("%.2f s", nanos / 1_000_000_000.0);
    }

    private static String formatDuration(Duration d) {
        long hours = d.toHours();
        long minutes = d.toMinutesPart();
        long seconds = d.toSecondsPart();
        if (hours > 0)
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        if (minutes > 0)
            return String.format("%dm %ds", minutes, seconds);
        return String.format("%ds", seconds);
    }

    /** Immutable churn event record. */
    public record ChurnEvent(Instant timestamp, String type, String nodeId) {
    }
}
