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
import dev.nishisan.utils.ngrid.common.LeaderScorePayload;
import dev.nishisan.utils.ngrid.common.LeaderSuggestionPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Broadcasts local write rates and suggests leader changes based on ingress write load.
 */
public final class LeaderReelectionService implements TransportListener, Closeable {
    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final StatsUtils stats;
    private final ScheduledExecutorService scheduler;
    private final Duration interval;
    private final Duration cooldown;
    private final Duration suggestionTtl;
    private final double minDelta;
    private final Map<NodeId, Double> rates = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> lastCounts = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> lastTimestamps = new ConcurrentHashMap<>();
    private volatile long lastSuggestionAtMs;
    private volatile boolean running;

    public LeaderReelectionService(Transport transport,
                                   ClusterCoordinator coordinator,
                                   StatsUtils stats,
                                   ScheduledExecutorService scheduler,
                                   Duration interval,
                                   Duration cooldown,
                                   Duration suggestionTtl,
                                   double minDelta) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.cooldown = Objects.requireNonNull(cooldown, "cooldown");
        this.suggestionTtl = Objects.requireNonNull(suggestionTtl, "suggestionTtl");
        this.minDelta = minDelta;
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
        long periodMs = Math.max(200L, interval.toMillis());
        scheduler.scheduleAtFixedRate(this::tick, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
    }

    private void tick() {
        if (!running) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        NodeId localId = transport.local().nodeId();
        Double localRate = computeLocalRate(localId, now);
        if (localRate != null) {
            rates.put(localId, localRate);
            LeaderScorePayload payload = new LeaderScorePayload(localId, localRate, now);
            ClusterMessage message = ClusterMessage.request(MessageType.LEADER_SCORE,
                    "leader-score",
                    localId,
                    null,
                    payload);
            transport.broadcast(message);
        }
        if (coordinator.isLeader()) {
            evaluateLeaderChange(now);
        }
    }

    private Double computeLocalRate(NodeId nodeId, long now) {
        Long current = stats.getCounterValueOrNull(NGridMetrics.ingressWrite(nodeId));
        if (current == null) {
            return null;
        }
        Long last = lastCounts.put(nodeId, current);
        Long lastTs = lastTimestamps.put(nodeId, now);
        if (last == null || lastTs == null) {
            return null;
        }
        long delta = current - last;
        long elapsedMs = now - lastTs;
        if (elapsedMs <= 0) {
            return null;
        }
        return delta / (elapsedMs / 1000.0);
    }

    private void evaluateLeaderChange(long now) {
        if (cooldown.toMillis() > 0 && now - lastSuggestionAtMs < cooldown.toMillis()) {
            return;
        }
        NodeId currentLeader = coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null);
        if (currentLeader == null) {
            return;
        }
        NodeId best = currentLeader;
        double leaderRate = rates.getOrDefault(currentLeader, 0.0);
        double bestRate = leaderRate;
        for (NodeInfo member : coordinator.activeMembers()) {
            NodeId id = member.nodeId();
            double rate = rates.getOrDefault(id, 0.0);
            if (rate > bestRate) {
                bestRate = rate;
                best = id;
            }
        }
        if (!best.equals(currentLeader) && bestRate >= leaderRate + minDelta) {
            LeaderSuggestionPayload payload = new LeaderSuggestionPayload(best, bestRate, now);
            ClusterMessage message = ClusterMessage.request(MessageType.LEADER_SUGGESTION,
                    "leader-suggest",
                    transport.local().nodeId(),
                    null,
                    payload);
            transport.broadcast(message);
            coordinator.setPreferredLeader(best, suggestionTtl);
            lastSuggestionAtMs = now;
        }
    }

    private double bestRate(NodeId nodeId) {
        return rates.getOrDefault(nodeId, 0.0);
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        rates.remove(peerId);
        lastCounts.remove(peerId);
        lastTimestamps.remove(peerId);
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.LEADER_SCORE) {
            LeaderScorePayload payload = message.payload(LeaderScorePayload.class);
            if (!payload.nodeId().equals(message.source())) {
                return;
            }
            rates.put(payload.nodeId(), payload.writeRate());
            return;
        }
        if (message.type() == MessageType.LEADER_SUGGESTION) {
            LeaderSuggestionPayload payload = message.payload(LeaderSuggestionPayload.class);
            coordinator.setPreferredLeader(payload.leaderId(), suggestionTtl);
        }
    }

    @Override
    public void close() {
        stop();
    }
}
