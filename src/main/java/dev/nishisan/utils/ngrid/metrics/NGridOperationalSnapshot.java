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

import java.time.Instant;

/**
 * Immutable operational snapshot of an NGrid node, aggregating cluster state,
 * replication health, and I/O metrics for dashboards and alerting.
 *
 * <p>
 * Designed to be captured periodically via
 * {@code NGridNode.operationalSnapshot()}
 * and consumed by {@link NGridAlertEngine} and {@link NGridDashboardReporter}.
 * </p>
 *
 * @since 2.1.0
 *
 * @param localNodeId                the local node identifier
 * @param leaderId                   the current leader identifier
 * @param leaderEpoch                the leader epoch
 * @param trackedLeaderEpoch         the tracked leader epoch
 * @param activeMembersCount         the number of active cluster members
 * @param isLeader                   whether the local node is the leader
 * @param hasValidLease              whether the leader lease is valid
 * @param trackedLeaderHighWatermark the tracked high watermark
 * @param globalSequence             the global replication sequence
 * @param lastAppliedSequence        the last applied sequence
 * @param replicationLag             the replication lag
 * @param gapsDetected               the number of gaps detected
 * @param resendSuccessCount         the number of successful resends
 * @param snapshotFallbackCount      the number of snapshot fallbacks
 * @param averageConvergenceTimeMs   the average convergence time in ms
 * @param pendingOperationsCount     the pending operations count
 * @param reachableNodesCount        the number of reachable nodes
 * @param totalNodesCount            the total number of nodes
 * @param ioStats                    the I/O statistics snapshot
 * @param capturedAt                 the timestamp when the snapshot was taken
 */
public record NGridOperationalSnapshot(
                // ── Cluster ──
                String localNodeId,
                String leaderId,
                long leaderEpoch,
                long trackedLeaderEpoch,
                int activeMembersCount,
                boolean isLeader,
                boolean hasValidLease,
                long trackedLeaderHighWatermark,

                // ── Replication ──
                long globalSequence,
                long lastAppliedSequence,
                long replicationLag,
                long gapsDetected,
                long resendSuccessCount,
                long snapshotFallbackCount,
                double averageConvergenceTimeMs,
                int pendingOperationsCount,

                // ── Health ──
                int reachableNodesCount,
                int totalNodesCount,

                // ── I/O Stats ──
                NGridStatsSnapshot ioStats,

                // ── Timestamp ──
                Instant capturedAt) {
}
