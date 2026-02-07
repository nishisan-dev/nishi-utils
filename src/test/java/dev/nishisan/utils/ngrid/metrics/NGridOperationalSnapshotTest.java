package dev.nishisan.utils.ngrid.metrics;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NGridOperationalSnapshot}.
 */
class NGridOperationalSnapshotTest {

    @Test
    void snapshotCapturesAllFields() {
        Instant now = Instant.now();
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                now,
                Map.of("node-1", 100L),
                Map.of("node-1", 50L),
                Map.of("node-1", 30L),
                Map.of("node-1", 10L),
                Map.of(),
                Map.of(),
                Map.of("node-1", 1.5),
                Map.of());

        NGridOperationalSnapshot snapshot = new NGridOperationalSnapshot(
                "node-1",
                "node-1",
                3L,
                3L,
                3,
                true,
                true,
                500L,
                500L,
                500L,
                0L,
                2L,
                1L,
                0L,
                12.5,
                0,
                3,
                3,
                ioStats,
                now);

        assertEquals("node-1", snapshot.localNodeId());
        assertEquals("node-1", snapshot.leaderId());
        assertEquals(3L, snapshot.leaderEpoch());
        assertEquals(3L, snapshot.trackedLeaderEpoch());
        assertEquals(3, snapshot.activeMembersCount());
        assertTrue(snapshot.isLeader());
        assertTrue(snapshot.hasValidLease());
        assertEquals(500L, snapshot.trackedLeaderHighWatermark());
        assertEquals(500L, snapshot.globalSequence());
        assertEquals(500L, snapshot.lastAppliedSequence());
        assertEquals(0L, snapshot.replicationLag());
        assertEquals(2L, snapshot.gapsDetected());
        assertEquals(1L, snapshot.resendSuccessCount());
        assertEquals(0L, snapshot.snapshotFallbackCount());
        assertEquals(12.5, snapshot.averageConvergenceTimeMs(), 0.01);
        assertEquals(0, snapshot.pendingOperationsCount());
        assertEquals(3, snapshot.reachableNodesCount());
        assertEquals(3, snapshot.totalNodesCount());
        assertSame(ioStats, snapshot.ioStats());
        assertEquals(now, snapshot.capturedAt());
    }

    @Test
    void replicationLagIsNonNegative() {
        Instant now = Instant.now();
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                now, Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of());

        // Even if lastApplied > highWatermark (leader node scenario), lag should be
        // computed
        // by the factory method with Math.max(0, ...). Here we test the record itself
        // stores
        // whatever was passed.
        NGridOperationalSnapshot snapshot = new NGridOperationalSnapshot(
                "node-1", "node-1", 1L, 1L, 1, true, true, 100L,
                200L, 200L, 0L, 0L, 0L, 0L, 0.0, 0, 1, 1, ioStats, now);

        assertEquals(0L, snapshot.replicationLag());
    }

    @Test
    void followerLagIsPositive() {
        Instant now = Instant.now();
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                now, Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of());

        // Simulate follower that hasn't caught up: highWatermark=500, lastApplied=300
        long lag = 200L;
        NGridOperationalSnapshot snapshot = new NGridOperationalSnapshot(
                "follower-1", "leader-1", 2L, 2L, 3, false, false, 500L,
                0L, 300L, lag, 5L, 3L, 1L, 25.0, 2, 2, 3, ioStats, now);

        assertEquals(200L, snapshot.replicationLag());
        assertFalse(snapshot.isLeader());
        assertEquals(2, snapshot.pendingOperationsCount());
        assertEquals(5L, snapshot.gapsDetected());
    }

    @Test
    void degradedClusterHealth() {
        Instant now = Instant.now();
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                now, Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of());

        // Only 1 of 3 nodes reachable
        NGridOperationalSnapshot snapshot = new NGridOperationalSnapshot(
                "node-1", "node-1", 5L, 5L, 3, true, false, 1000L,
                1000L, 1000L, 0L, 0L, 0L, 0L, 0.0, 5, 1, 3, ioStats, now);

        assertEquals(1, snapshot.reachableNodesCount());
        assertEquals(3, snapshot.totalNodesCount());
        assertFalse(snapshot.hasValidLease());
        assertEquals(5, snapshot.pendingOperationsCount());
    }
}
