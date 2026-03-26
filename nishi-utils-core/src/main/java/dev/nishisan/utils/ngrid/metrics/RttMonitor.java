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

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodically measures round-trip time to peers using lightweight ping messages.
 */
public final class RttMonitor implements TransportListener, Closeable {
    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final StatsUtils stats;
    private final ScheduledExecutorService scheduler;
    private final Duration interval;
    private final AtomicLong pingCounter = new AtomicLong();
    private volatile boolean running;

    public RttMonitor(Transport transport,
                      ClusterCoordinator coordinator,
                      StatsUtils stats,
                      ScheduledExecutorService scheduler,
                      Duration interval) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.interval = Objects.requireNonNull(interval, "interval");
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        transport.addListener(this);
        if (interval.isZero() || interval.isNegative()) {
            return;
        }
        long periodMs = Math.max(100L, interval.toMillis());
        scheduler.scheduleAtFixedRate(this::probePeers, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
    }

    private void probePeers() {
        try {
            if (!running) {
                return;
            }
            for (NodeInfo peer : coordinator.activeMembers()) {
                if (peer.nodeId().equals(transport.local().nodeId())) {
                    continue;
                }
                sendPing(peer.nodeId());
            }
        } catch (Throwable t) {
            java.util.logging.Logger.getLogger(RttMonitor.class.getName()).log(java.util.logging.Level.SEVERE, "Unexpected error in RTT probe task", t);
        }
    }

    /**
     * Generates a deterministic UUID for PING messages using a monotonic counter.
     * Avoids the CPU cost of {@link UUID#randomUUID()} and SecureRandom entropy
     * while maintaining uniqueness within this node for request-response correlation.
     *
     * @return a new deterministic UUID based on the counter value
     */
    private UUID nextPingId() {
        return new UUID(0, pingCounter.incrementAndGet());
    }

    private void sendPing(NodeId nodeId) {
        long start = System.nanoTime();
        ClusterMessage request = new ClusterMessage(
                nextPingId(),
                null,
                MessageType.PING,
                "rtt",
                transport.local().nodeId(),
                nodeId,
                HeartbeatPayload.now(),
                1);
        CompletableFuture<ClusterMessage> future = transport.sendAndAwait(request);
        future.whenComplete((response, error) -> {
            if (error != null) {
                stats.notifyHitCounter(NGridMetrics.rttFailure(nodeId));
                return;
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            stats.notifyAverageCounter(NGridMetrics.rttMs(nodeId), elapsedMs);
        });
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        // no-op
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() != MessageType.PING) {
            return;
        }
        ClusterMessage response = ClusterMessage.response(message, message.payload());
        transport.send(response);
    }

    @Override
    public void close() {
        stop();
    }
}
