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

import dev.nishisan.utils.ngrid.map.PersistenceHealthListener;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lightweight alert engine that periodically evaluates operational rules
 * against an {@link NGridOperationalSnapshot} and dispatches
 * {@link NGridAlert} events to registered {@link NGridAlertListener listeners}.
 *
 * <h2>Built-in Alert Types</h2>
 * <ul>
 * <li>{@code PERSISTENCE_FAILURE} — triggered via
 * {@link PersistenceHealthListener} callback (CRITICAL)</li>
 * <li>{@code HIGH_REPLICATION_LAG} — replication lag exceeds threshold
 * (WARNING/CRITICAL)</li>
 * <li>{@code LEADER_LEASE_EXPIRED} — leader has no valid lease (CRITICAL)</li>
 * <li>{@code LOW_QUORUM} — reachable nodes below majority (WARNING)</li>
 * <li>{@code HIGH_GAP_RATE} — sequence gaps growing above threshold
 * (WARNING)</li>
 * <li>{@code SNAPSHOT_FALLBACK_SPIKE} — full sync fallbacks growing above
 * threshold (WARNING)</li>
 * </ul>
 *
 * <p>
 * Each alert type has a configurable cooldown to prevent alert storms.
 * </p>
 *
 * @since 2.1.0
 */
public final class NGridAlertEngine implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(NGridAlertEngine.class.getName());

    // Alert type constants
    /** Alert type for persistence failures. */
    public static final String PERSISTENCE_FAILURE = "PERSISTENCE_FAILURE";
    /** Alert type for high replication lag. */
    public static final String HIGH_REPLICATION_LAG = "HIGH_REPLICATION_LAG";
    /** Alert type for expired leader leases. */
    public static final String LEADER_LEASE_EXPIRED = "LEADER_LEASE_EXPIRED";
    /** Alert type for low quorum. */
    public static final String LOW_QUORUM = "LOW_QUORUM";
    /** Alert type for high gap rate. */
    public static final String HIGH_GAP_RATE = "HIGH_GAP_RATE";
    /** Alert type for snapshot fallback spikes. */
    public static final String SNAPSHOT_FALLBACK_SPIKE = "SNAPSHOT_FALLBACK_SPIKE";

    private final Supplier<NGridOperationalSnapshot> snapshotSupplier;
    private final ScheduledExecutorService scheduler;
    private final Duration evaluationInterval;
    private final Duration alertCooldown;
    private final long lagWarningThreshold;
    private final long lagCriticalThreshold;
    private final long gapRateThreshold;
    private final long snapshotFallbackThreshold;

    private final List<NGridAlertListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Long> lastAlertTimestamps = new ConcurrentHashMap<>();

    // Previous snapshot values for rate detection
    private volatile long previousGapsDetected = -1;
    private volatile long previousSnapshotFallbackCount = -1;
    private volatile boolean running;
    private volatile ScheduledFuture<?> evaluationTask;

    private NGridAlertEngine(Builder builder) {
        this.snapshotSupplier = Objects.requireNonNull(builder.snapshotSupplier, "snapshotSupplier");
        this.scheduler = Objects.requireNonNull(builder.scheduler, "scheduler");
        this.evaluationInterval = builder.evaluationInterval;
        this.alertCooldown = builder.alertCooldown;
        this.lagWarningThreshold = builder.lagWarningThreshold;
        this.lagCriticalThreshold = builder.lagCriticalThreshold;
        this.gapRateThreshold = builder.gapRateThreshold;
        this.snapshotFallbackThreshold = builder.snapshotFallbackThreshold;
    }

    /**
     * Starts the periodic evaluation loop.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        long periodMs = Math.max(500L, evaluationInterval.toMillis());
        evaluationTask = scheduler.scheduleAtFixedRate(this::evaluateScheduled, periodMs, periodMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Registers an alert listener.
     *
     * @param listener the listener to add
     */
    public void addListener(NGridAlertListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Removes a previously registered listener.
     *
     * @param listener the listener to remove
     */
    public void removeListener(NGridAlertListener listener) {
        listeners.remove(listener);
    }

    /**
     * Manually fires a persistence failure alert. Intended to be called by the
     * {@link PersistenceHealthListener} callback.
     *
     * @param failureType the persistence failure type
     * @param cause       the exception that caused the failure
     */
    public void firePersistenceFailure(PersistenceHealthListener.PersistenceFailureType failureType,
            Throwable cause) {
        dispatch(NGridAlert.of(
                PERSISTENCE_FAILURE,
                AlertSeverity.CRITICAL,
                "Persistence failure: " + failureType + " — " + cause.getMessage(),
                Map.of("failureType", failureType.name(),
                        "exception", cause.getClass().getSimpleName())));
    }

    /**
     * Core evaluation — examines the current snapshot and fires alerts
     * for any violated conditions. Can be called manually or by the periodic
     * scheduler.
     */
    void evaluate() {
        try {
            NGridOperationalSnapshot snapshot = snapshotSupplier.get();
            if (snapshot == null) {
                return;
            }

            evaluateReplicationLag(snapshot);
            evaluateLeaderLease(snapshot);
            evaluateQuorum(snapshot);
            evaluateGapRate(snapshot);
            evaluateSnapshotFallback(snapshot);

        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Alert evaluation failed", t);
        }
    }

    private void evaluateScheduled() {
        if (!running) {
            return;
        }
        evaluate();
    }

    private void evaluateReplicationLag(NGridOperationalSnapshot snapshot) {
        long lag = snapshot.replicationLag();
        if (lag >= lagCriticalThreshold) {
            dispatchIfCooldownExpired(NGridAlert.of(
                    HIGH_REPLICATION_LAG,
                    AlertSeverity.CRITICAL,
                    "Replication lag CRITICAL: " + lag + " (threshold: " + lagCriticalThreshold + ")",
                    Map.of("lag", String.valueOf(lag),
                            "threshold", String.valueOf(lagCriticalThreshold))));
        } else if (lag >= lagWarningThreshold) {
            dispatchIfCooldownExpired(NGridAlert.of(
                    HIGH_REPLICATION_LAG,
                    AlertSeverity.WARNING,
                    "Replication lag WARNING: " + lag + " (threshold: " + lagWarningThreshold + ")",
                    Map.of("lag", String.valueOf(lag),
                            "threshold", String.valueOf(lagWarningThreshold))));
        }
    }

    private void evaluateLeaderLease(NGridOperationalSnapshot snapshot) {
        if (snapshot.isLeader() && !snapshot.hasValidLease()) {
            dispatchIfCooldownExpired(NGridAlert.of(
                    LEADER_LEASE_EXPIRED,
                    AlertSeverity.CRITICAL,
                    "Leader lease expired on node " + snapshot.localNodeId(),
                    Map.of("nodeId", snapshot.localNodeId(),
                            "epoch", String.valueOf(snapshot.leaderEpoch()))));
        }
    }

    private void evaluateQuorum(NGridOperationalSnapshot snapshot) {
        int total = snapshot.totalNodesCount();
        int reachable = snapshot.reachableNodesCount();
        int majority = (total / 2) + 1;
        if (total > 1 && reachable < majority) {
            dispatchIfCooldownExpired(NGridAlert.of(
                    LOW_QUORUM,
                    AlertSeverity.WARNING,
                    "Low quorum: " + reachable + "/" + total + " reachable (majority requires " + majority + ")",
                    Map.of("reachable", String.valueOf(reachable),
                            "total", String.valueOf(total),
                            "majority", String.valueOf(majority))));
        }
    }

    private void evaluateGapRate(NGridOperationalSnapshot snapshot) {
        long current = snapshot.gapsDetected();
        if (previousGapsDetected >= 0) {
            long delta = current - previousGapsDetected;
            if (delta > gapRateThreshold) {
                dispatchIfCooldownExpired(NGridAlert.of(
                        HIGH_GAP_RATE,
                        AlertSeverity.WARNING,
                        "High gap rate: " + delta + " new gaps detected (threshold: " + gapRateThreshold + ")",
                        Map.of("delta", String.valueOf(delta),
                                "total", String.valueOf(current))));
            }
        }
        previousGapsDetected = current;
    }

    private void evaluateSnapshotFallback(NGridOperationalSnapshot snapshot) {
        long current = snapshot.snapshotFallbackCount();
        if (previousSnapshotFallbackCount >= 0) {
            long delta = current - previousSnapshotFallbackCount;
            if (delta > snapshotFallbackThreshold) {
                dispatchIfCooldownExpired(NGridAlert.of(
                        SNAPSHOT_FALLBACK_SPIKE,
                        AlertSeverity.WARNING,
                        "Snapshot fallback spike: " + delta + " new full syncs (threshold: " + snapshotFallbackThreshold
                                + ")",
                        Map.of("delta", String.valueOf(delta),
                                "total", String.valueOf(current))));
            }
        }
        previousSnapshotFallbackCount = current;
    }

    private void dispatchIfCooldownExpired(NGridAlert alert) {
        long now = System.currentTimeMillis();
        Long last = lastAlertTimestamps.get(alert.alertType());
        if (last != null && (now - last) < alertCooldown.toMillis()) {
            return; // Still in cooldown
        }
        lastAlertTimestamps.put(alert.alertType(), now);
        dispatch(alert);
    }

    private void dispatch(NGridAlert alert) {
        LOGGER.info(() -> "[ALERT] " + alert.severity() + " " + alert.alertType() + ": " + alert.message());
        for (NGridAlertListener listener : listeners) {
            try {
                listener.onAlert(alert);
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Alert listener threw exception", t);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        ScheduledFuture<?> task = evaluationTask;
        if (task != null) {
            task.cancel(false);
            evaluationTask = null;
        }
    }

    /**
     * Returns the number of currently registered listeners.
     * Mainly for testing.
     *
     * @return the listener count
     */
    public int listenerCount() {
        return listeners.size();
    }

    // ── Builder ──

    /**
     * Creates a builder for an alert engine.
     *
     * @param snapshotSupplier supplier of operational snapshots
     * @param scheduler        the scheduler for periodic evaluation
     * @return the builder
     */
    public static Builder builder(Supplier<NGridOperationalSnapshot> snapshotSupplier,
            ScheduledExecutorService scheduler) {
        return new Builder(snapshotSupplier, scheduler);
    }

    /**
     * Builder for {@link NGridAlertEngine}.
     */
    public static final class Builder {
        private final Supplier<NGridOperationalSnapshot> snapshotSupplier;
        private final ScheduledExecutorService scheduler;
        private Duration evaluationInterval = Duration.ofSeconds(5);
        private Duration alertCooldown = Duration.ofSeconds(30);
        private long lagWarningThreshold = 100;
        private long lagCriticalThreshold = 500;
        private long gapRateThreshold = 10;
        private long snapshotFallbackThreshold = 2;

        private Builder(Supplier<NGridOperationalSnapshot> snapshotSupplier,
                ScheduledExecutorService scheduler) {
            this.snapshotSupplier = snapshotSupplier;
            this.scheduler = scheduler;
        }

        /**
         * Sets the evaluation interval.
         * 
         * @param interval the interval
         * @return this builder
         */
        public Builder evaluationInterval(Duration interval) {
            this.evaluationInterval = Objects.requireNonNull(interval);
            return this;
        }

        /**
         * Sets the alert cooldown.
         * 
         * @param cooldown the cooldown
         * @return this builder
         */
        public Builder alertCooldown(Duration cooldown) {
            this.alertCooldown = Objects.requireNonNull(cooldown);
            return this;
        }

        /**
         * Sets the lag warning threshold.
         * 
         * @param threshold the threshold
         * @return this builder
         */
        public Builder lagWarningThreshold(long threshold) {
            this.lagWarningThreshold = threshold;
            return this;
        }

        /**
         * Sets the lag critical threshold.
         * 
         * @param threshold the threshold
         * @return this builder
         */
        public Builder lagCriticalThreshold(long threshold) {
            this.lagCriticalThreshold = threshold;
            return this;
        }

        /**
         * Sets the gap rate threshold.
         * 
         * @param threshold the threshold
         * @return this builder
         */
        public Builder gapRateThreshold(long threshold) {
            this.gapRateThreshold = threshold;
            return this;
        }

        /**
         * Sets the snapshot fallback threshold.
         * 
         * @param threshold the threshold
         * @return this builder
         */
        public Builder snapshotFallbackThreshold(long threshold) {
            this.snapshotFallbackThreshold = threshold;
            return this;
        }

        /**
         * Builds the alert engine.
         * 
         * @return the engine
         */
        public NGridAlertEngine build() {
            return new NGridAlertEngine(this);
        }
    }
}
