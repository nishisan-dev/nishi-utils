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

package dev.nishisan.utils.ngrid.debug;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Utility class that periodically dumps the current cluster status (topology, leader, resources)
 * to a YAML file for debugging and monitoring purposes.
 * <p>
 * It also reacts to cluster events (leadership changes, membership changes, resource creation)
 * to update the status file immediately (with debounce).
 */
public class ClusterDebugLogger implements AutoCloseable {
    private final NGridNode node;
    private final Path outputPath;
    private final Duration interval;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean debounceScheduled = new AtomicBoolean(false);

    /**
     * Creates a new logger instance.
     *
     * @param node       the NGridNode to monitor
     * @param outputPath the file path where the YAML status will be written
     * @param interval   how often to force an update of the file
     */
    public ClusterDebugLogger(NGridNode node, Path outputPath, Duration interval) {
        this.node = node;
        this.outputPath = outputPath;
        this.interval = interval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-debug-logger");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the logging task. It schedules a periodic dump and registers listeners
     * for reactive updates.
     */
    public void start() {
        // Periodic update as a fallback
        scheduler.scheduleAtFixedRate(this::dump, 0, interval.toMillis(), TimeUnit.MILLISECONDS);

        // Reactive updates
        node.coordinator().addLeadershipListener(leaderId -> triggerDump());
        node.coordinator().addMembershipListener(this::triggerDump);
        node.addResourceListener(this::triggerDump);
    }

    /**
     * Schedules a dump to run shortly if one isn't already scheduled.
     * This prevents excessive IO during bursts of events (e.g. startup).
     */
    private void triggerDump() {
        if (debounceScheduled.compareAndSet(false, true)) {
            scheduler.schedule(() -> {
                try {
                    dump();
                } finally {
                    debounceScheduled.set(false);
                }
            }, 100, TimeUnit.MILLISECONDS);
        }
    }

    private void dump() {
        try {
            StringBuilder yaml = new StringBuilder();

            // Grid Status
            yaml.append("grid:\n");
            yaml.append("  nodes:\n");

            Collection<NodeInfo> members = node.coordinator().activeMembers();
            // Sort for stability in display
            var sortedMembers = members.stream()
                    .sorted((a, b) -> a.nodeId().value().compareTo(b.nodeId().value()))
                    .collect(Collectors.toList());

            for (NodeInfo member : sortedMembers) {
                yaml.append("    - name: \"").append(member.nodeId().value()).append("\"\n");
                yaml.append("      ip: \"").append(member.host()).append("\"\n");
                yaml.append("      port: ").append(member.port()).append("\n");
                yaml.append("\n");
            }

            Optional<NodeInfo> leader = node.coordinator().leaderInfo();
            yaml.append("  grid-leader: \"")
                    .append(leader.map(l -> l.nodeId().value()).orElse("unknown"))
                    .append("\"\n\n");

            // Queues
            Set<String> queues = node.getQueueNames();
            yaml.append("queues:\n");
            if (queues.isEmpty()) {
                yaml.append("  []\n");
            } else {
                queues.stream().sorted().forEach(q ->
                        yaml.append("  - \"").append(q).append("\"\n")
                );
            }
            yaml.append("\n");

            // Maps
            Set<String> maps = node.getMapNames();
            yaml.append("maps:\n");
            if (maps.isEmpty()) {
                yaml.append("  []\n");
            } else {
                maps.stream().sorted().forEach(m ->
                        yaml.append("  - \"").append(m).append("\"\n")
                );
            }

            Files.writeString(outputPath, yaml.toString(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

        } catch (Exception e) {
            // Log error but don't crash the monitoring thread
            System.err.println("[ClusterDebugLogger] Failed to write status file: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}