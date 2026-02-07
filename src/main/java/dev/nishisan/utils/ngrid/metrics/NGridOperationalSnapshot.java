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
