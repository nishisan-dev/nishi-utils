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

package dev.nishisan.utils.ngrid.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically captures an {@link NGridOperationalSnapshot} and dumps
 * a comprehensive YAML dashboard report to a file on disk.
 *
 * <p>
 * Designed as an evolution of {@code ClusterDebugLogger}, this reporter
 * includes full operational state (cluster, replication, health, I/O) in a
 * single human-readable file that can be consumed by monitoring scripts or
 * log aggregators.
 * </p>
 *
 * @since 2.1.0
 */
public final class NGridDashboardReporter implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(NGridDashboardReporter.class.getName());

    private final Supplier<NGridOperationalSnapshot> snapshotSupplier;
    private final ScheduledExecutorService scheduler;
    private final Path outputPath;
    private final Duration reportInterval;
    private volatile boolean running;
    private volatile ScheduledFuture<?> reportTask;

    private static final ObjectMapper YAML_MAPPER;

    static {
        YAMLFactory factory = YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
        YAML_MAPPER = new ObjectMapper(factory);
        YAML_MAPPER.findAndRegisterModules();
    }

    /**
     * @param snapshotSupplier supplies the operational snapshot on each tick
     * @param scheduler        shared scheduler for periodic execution
     * @param outputPath       path to write the YAML dashboard file
     * @param reportInterval   interval between reports
     */
    public NGridDashboardReporter(Supplier<NGridOperationalSnapshot> snapshotSupplier,
            ScheduledExecutorService scheduler,
            Path outputPath,
            Duration reportInterval) {
        this.snapshotSupplier = Objects.requireNonNull(snapshotSupplier, "snapshotSupplier");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.outputPath = Objects.requireNonNull(outputPath, "outputPath");
        this.reportInterval = Objects.requireNonNull(reportInterval, "reportInterval");
    }

    /**
     * Starts the periodic reporting loop.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        long periodMs = Math.max(1000L, reportInterval.toMillis());
        reportTask = scheduler.scheduleAtFixedRate(this::reportScheduled, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Captures a snapshot and writes the dashboard YAML. Can be called manually.
     */
    public void report() {
        try {
            NGridOperationalSnapshot snapshot = snapshotSupplier.get();
            if (snapshot == null) {
                return;
            }

            Map<String, Object> dashboard = buildDashboard(snapshot);
            String yaml = YAML_MAPPER.writeValueAsString(dashboard);

            Files.createDirectories(outputPath.getParent());
            Files.writeString(outputPath, yaml);

            LOGGER.fine(() -> "Dashboard report written to " + outputPath);

        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Dashboard report generation failed", t);
        }
    }

    private void reportScheduled() {
        if (!running) {
            return;
        }
        report();
    }

    /**
     * Builds the dashboard data structure from the snapshot.
     * Package-private for testing.
     */
    Map<String, Object> buildDashboard(NGridOperationalSnapshot snapshot) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("capturedAt", snapshot.capturedAt().toString());

        // Cluster section
        Map<String, Object> cluster = new LinkedHashMap<>();
        cluster.put("localNodeId", snapshot.localNodeId());
        cluster.put("leaderId", snapshot.leaderId());
        cluster.put("leaderEpoch", snapshot.leaderEpoch());
        cluster.put("trackedLeaderEpoch", snapshot.trackedLeaderEpoch());
        cluster.put("activeMembersCount", snapshot.activeMembersCount());
        cluster.put("isLeader", snapshot.isLeader());
        cluster.put("hasValidLease", snapshot.hasValidLease());
        cluster.put("trackedLeaderHighWatermark", snapshot.trackedLeaderHighWatermark());
        root.put("cluster", cluster);

        // Replication section
        Map<String, Object> replication = new LinkedHashMap<>();
        replication.put("globalSequence", snapshot.globalSequence());
        replication.put("lastAppliedSequence", snapshot.lastAppliedSequence());
        replication.put("replicationLag", snapshot.replicationLag());
        replication.put("gapsDetected", snapshot.gapsDetected());
        replication.put("resendSuccessCount", snapshot.resendSuccessCount());
        replication.put("snapshotFallbackCount", snapshot.snapshotFallbackCount());
        replication.put("averageConvergenceTimeMs", snapshot.averageConvergenceTimeMs());
        replication.put("pendingOperationsCount", snapshot.pendingOperationsCount());
        root.put("replication", replication);

        // Health section
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("reachableNodesCount", snapshot.reachableNodesCount());
        health.put("totalNodesCount", snapshot.totalNodesCount());
        double quorumRatio = snapshot.totalNodesCount() > 0
                ? (double) snapshot.reachableNodesCount() / snapshot.totalNodesCount()
                : 0.0;
        health.put("quorumRatio", Math.round(quorumRatio * 100.0) / 100.0);
        root.put("health", health);

        // I/O section (summarized)
        NGridStatsSnapshot io = snapshot.ioStats();
        if (io != null) {
            Map<String, Object> ioSection = new LinkedHashMap<>();
            ioSection.put("writesByNode", io.writesByNode());
            ioSection.put("ingressWritesByNode", io.ingressWritesByNode());
            ioSection.put("queueOffersByNode", io.queueOffersByNode());
            ioSection.put("queuePollsByNode", io.queuePollsByNode());
            ioSection.put("rttMsByNode", io.rttMsByNode());
            ioSection.put("rttFailuresByNode", io.rttFailuresByNode());
            root.put("io", ioSection);
        }

        return root;
    }

    /**
     * Returns the YAML string representation of the current dashboard.
     * Useful for programmatic access without touching the filesystem.
     *
     * @return YAML string, or null if snapshot is unavailable
     */
    public String toYaml() {
        NGridOperationalSnapshot snapshot = snapshotSupplier.get();
        if (snapshot == null) {
            return null;
        }
        try {
            return YAML_MAPPER.writeValueAsString(buildDashboard(snapshot));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to serialize dashboard to YAML", e);
            return null;
        }
    }

    @Override
    public void close() {
        running = false;
        ScheduledFuture<?> task = reportTask;
        if (task != null) {
            task.cancel(false);
            reportTask = null;
        }
    }
}
