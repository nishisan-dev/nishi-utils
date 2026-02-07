package dev.nishisan.utils.ngrid.metrics;

import dev.nishisan.utils.ngrid.map.PersistenceHealthListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NGridAlertEngine}.
 */
class NGridAlertEngineTest {

    private ScheduledExecutorService scheduler;
    private List<NGridAlert> captured;
    private AtomicReference<NGridOperationalSnapshot> currentSnapshot;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        captured = new ArrayList<>();
        currentSnapshot = new AtomicReference<>();
    }

    private NGridAlertEngine buildEngine(long lagWarn, long lagCrit, long gapThreshold, long fallbackThreshold) {
        return NGridAlertEngine.builder(currentSnapshot::get, scheduler)
                .evaluationInterval(Duration.ofSeconds(60)) // won't auto-fire, we call evaluate() manually
                .alertCooldown(Duration.ZERO) // no cooldown for tests
                .lagWarningThreshold(lagWarn)
                .lagCriticalThreshold(lagCrit)
                .gapRateThreshold(gapThreshold)
                .snapshotFallbackThreshold(fallbackThreshold)
                .build();
    }

    private NGridOperationalSnapshot snapshot(long lag, boolean isLeader, boolean hasLease,
            int reachable, int total,
            long gaps, long fallbacks) {
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                Instant.now(), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of());
        return new NGridOperationalSnapshot(
                "node-1", "node-1", 1L, 1L, total,
                isLeader, hasLease, 1000L,
                1000L, 1000L - lag, lag,
                gaps, 0L, fallbacks, 0.0, 0,
                reachable, total, ioStats, Instant.now());
    }

    @Test
    void highReplicationLagWarning() {
        NGridAlertEngine engine = buildEngine(50, 500, 100, 100);
        engine.addListener(captured::add);

        currentSnapshot.set(snapshot(100, false, false, 3, 3, 0, 0));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.HIGH_REPLICATION_LAG, captured.get(0).alertType());
        assertEquals(AlertSeverity.WARNING, captured.get(0).severity());
    }

    @Test
    void highReplicationLagCritical() {
        NGridAlertEngine engine = buildEngine(50, 500, 100, 100);
        engine.addListener(captured::add);

        currentSnapshot.set(snapshot(600, false, false, 3, 3, 0, 0));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.HIGH_REPLICATION_LAG, captured.get(0).alertType());
        assertEquals(AlertSeverity.CRITICAL, captured.get(0).severity());
    }

    @Test
    void leaderLeaseExpired() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        engine.addListener(captured::add);

        currentSnapshot.set(snapshot(0, true, false, 3, 3, 0, 0));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.LEADER_LEASE_EXPIRED, captured.get(0).alertType());
        assertEquals(AlertSeverity.CRITICAL, captured.get(0).severity());
    }

    @Test
    void leaseExpiredNotFiredForFollower() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        engine.addListener(captured::add);

        // Follower with no valid lease should NOT fire LEADER_LEASE_EXPIRED
        currentSnapshot.set(snapshot(0, false, false, 3, 3, 0, 0));
        engine.evaluate();

        assertTrue(captured.isEmpty());
    }

    @Test
    void lowQuorumWarning() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        engine.addListener(captured::add);

        // 1 of 3 reachable, majority requires 2
        currentSnapshot.set(snapshot(0, true, true, 1, 3, 0, 0));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.LOW_QUORUM, captured.get(0).alertType());
        assertEquals(AlertSeverity.WARNING, captured.get(0).severity());
    }

    @Test
    void quorumOkNoAlert() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        engine.addListener(captured::add);

        // 2 of 3 reachable, majority requires 2
        currentSnapshot.set(snapshot(0, true, true, 2, 3, 0, 0));
        engine.evaluate();

        assertTrue(captured.isEmpty());
    }

    @Test
    void highGapRateDetection() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 5, 100);
        engine.addListener(captured::add);

        // First evaluation sets baseline
        currentSnapshot.set(snapshot(0, true, true, 3, 3, 10, 0));
        engine.evaluate();
        assertTrue(captured.isEmpty());

        // Second evaluation detects delta of 20 (> threshold 5)
        currentSnapshot.set(snapshot(0, true, true, 3, 3, 30, 0));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.HIGH_GAP_RATE, captured.get(0).alertType());
    }

    @Test
    void snapshotFallbackSpikeDetection() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 1);
        engine.addListener(captured::add);

        // Baseline
        currentSnapshot.set(snapshot(0, true, true, 3, 3, 0, 0));
        engine.evaluate();
        assertTrue(captured.isEmpty());

        // Spike: 3 new fallbacks (> threshold 1)
        currentSnapshot.set(snapshot(0, true, true, 3, 3, 0, 3));
        engine.evaluate();

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.SNAPSHOT_FALLBACK_SPIKE, captured.get(0).alertType());
    }

    @Test
    void cooldownPreventsAlertStorm() {
        NGridAlertEngine engine = NGridAlertEngine.builder(currentSnapshot::get, scheduler)
                .evaluationInterval(Duration.ofSeconds(60))
                .alertCooldown(Duration.ofMinutes(5)) // long cooldown
                .lagWarningThreshold(50)
                .lagCriticalThreshold(500)
                .gapRateThreshold(100)
                .snapshotFallbackThreshold(100)
                .build();
        engine.addListener(captured::add);

        currentSnapshot.set(snapshot(200, false, false, 3, 3, 0, 0));

        // First evaluation fires
        engine.evaluate();
        assertEquals(1, captured.size());

        // Second evaluation within cooldown should be suppressed
        engine.evaluate();
        assertEquals(1, captured.size()); // still 1
    }

    @Test
    void persistenceFailureAlert() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        engine.addListener(captured::add);

        engine.firePersistenceFailure(
                PersistenceHealthListener.PersistenceFailureType.WAL_WRITE,
                new RuntimeException("disk full"));

        assertEquals(1, captured.size());
        assertEquals(NGridAlertEngine.PERSISTENCE_FAILURE, captured.get(0).alertType());
        assertEquals(AlertSeverity.CRITICAL, captured.get(0).severity());
        assertTrue(captured.get(0).message().contains("WAL_WRITE"));
        assertEquals("RuntimeException", captured.get(0).metadata().get("exception"));
    }

    @Test
    void listenerManagement() {
        NGridAlertEngine engine = buildEngine(1000, 5000, 100, 100);
        NGridAlertListener listener = captured::add;

        engine.addListener(listener);
        assertEquals(1, engine.listenerCount());

        engine.removeListener(listener);
        assertEquals(0, engine.listenerCount());
    }

    @Test
    void multipleAlertsInSingleEvaluation() {
        NGridAlertEngine engine = buildEngine(50, 500, 100, 100);
        engine.addListener(captured::add);

        // Snapshot with high lag (CRITICAL) + expired lease + low quorum
        NGridStatsSnapshot ioStats = new NGridStatsSnapshot(
                Instant.now(), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of());
        NGridOperationalSnapshot bad = new NGridOperationalSnapshot(
                "node-1", "node-1", 1L, 1L, 3,
                true, false, 1000L,
                1000L, 400L, 600L,
                0L, 0L, 0L, 0.0, 0,
                1, 3, ioStats, Instant.now());
        currentSnapshot.set(bad);
        engine.evaluate();

        // Should fire: HIGH_REPLICATION_LAG (CRITICAL) + LEADER_LEASE_EXPIRED +
        // LOW_QUORUM
        assertEquals(3, captured.size());
        assertTrue(captured.stream().anyMatch(a -> a.alertType().equals(NGridAlertEngine.HIGH_REPLICATION_LAG)));
        assertTrue(captured.stream().anyMatch(a -> a.alertType().equals(NGridAlertEngine.LEADER_LEASE_EXPIRED)));
        assertTrue(captured.stream().anyMatch(a -> a.alertType().equals(NGridAlertEngine.LOW_QUORUM)));
    }

    @Test
    void nullSnapshotDoesNotThrow() {
        NGridAlertEngine engine = buildEngine(50, 500, 10, 2);
        engine.addListener(captured::add);

        currentSnapshot.set(null);
        assertDoesNotThrow(() -> engine.evaluate());
        assertTrue(captured.isEmpty());
    }
}
