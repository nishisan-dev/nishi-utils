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

package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.OperationStatus;
import dev.nishisan.utils.ngrid.common.ReplicationAckPayload;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Coordinates quorum based replication leveraging the transport. The manager handles both
 * leader initiated operations and replication requests coming from other nodes.
 */
public class ReplicationManager implements TransportListener, LeadershipListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());
    private static final long SYNC_THRESHOLD = 500; // Trigger sync if lag > 500 ops

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final ReplicationConfig config;
    private final Map<String, ReplicationHandler> handlers = new ConcurrentHashMap<>();
    private final Map<UUID, PendingOperation> pending = new ConcurrentHashMap<>();
    private final Map<UUID, ReplicatedRecord> log = new ConcurrentHashMap<>();
    private final Set<UUID> applied = new CopyOnWriteArraySet<>();
    private final Set<UUID> processing = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong globalSequence = new java.util.concurrent.atomic.AtomicLong(0);
    private volatile long lastAppliedSequence = 0;
    private final Set<String> syncingTopics = ConcurrentHashMap.newKeySet();

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ngrid-replication");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ngrid-replication-timeout");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;

    private static record Failure(PendingOperation operation, Throwable error) {}

    public ReplicationManager(Transport transport, ClusterCoordinator coordinator, ReplicationConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Protected constructor for testing purposes only.
     */
    protected ReplicationManager() {
        this.transport = null;
        this.coordinator = null;
        this.config = null;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        transport.addListener(this);
        coordinator.addLeadershipListener(this);
        Duration timeout = config.operationTimeout();
        long periodMs = Math.max(100L, timeout.toMillis() / 2);
        timeoutScheduler.scheduleAtFixedRate(this::checkTimeouts, periodMs, periodMs, TimeUnit.MILLISECONDS);
        Duration retryInterval = config.retryInterval();
        long retryMs = Math.max(100L, retryInterval.toMillis());
        timeoutScheduler.scheduleAtFixedRate(this::retryPending, retryMs, retryMs, TimeUnit.MILLISECONDS);
        timeoutScheduler.scheduleAtFixedRate(this::checkLagAndSync, 2000, 2000, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
        coordinator.removeLeadershipListener(this);
        failAllPending("ReplicationManager stopped");
    }

    public void registerHandler(String topic, ReplicationHandler handler) {
        handlers.put(topic, handler);
    }

    public long getGlobalSequence() {
        return globalSequence.get();
    }

    public long getLastAppliedSequence() {
        return lastAppliedSequence;
    }

    private void checkLagAndSync() {
        if (!running || coordinator.isLeader()) {
            return;
        }
        long leaderWatermark = coordinator.getTrackedLeaderHighWatermark();
        if (leaderWatermark < 0) {
            return;
        }
        long lag = leaderWatermark - lastAppliedSequence;
        if (lag > SYNC_THRESHOLD) {
            for (String topic : handlers.keySet()) {
                if (syncingTopics.add(topic)) {
                    requestSync(topic);
                }
            }
        }
    }

    /**
     * Exposes the configured replication operation timeout for callers that need to
     * bound their waiting time (e.g. higher-level services calling {@code future.get(...)}).
     */
    public Duration operationTimeout() {
        return config.operationTimeout();
    }

    public CompletableFuture<ReplicationResult> replicate(String topic, Serializable payload) {
        return replicate(topic, payload, null);
    }

    public CompletableFuture<ReplicationResult> replicate(String topic, Serializable payload, Integer quorumOverride) {
        if (!coordinator.isLeader()) {
            throw new IllegalStateException("Replication can only be initiated by the leader");
        }
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            throw new IllegalArgumentException("No replication handler registered for topic: " + topic);
        }
        UUID operationId = UUID.randomUUID();
        PendingOperation operation = new PendingOperation(operationId, topic, payload, requiredQuorum(quorumOverride));
        pending.put(operationId, operation);
        log.put(operationId, new ReplicatedRecord(operationId, topic, payload, OperationStatus.PENDING));
        
        // Local node acknowledges receipt, but defers application until quorum
        operation.ack(transport.local().nodeId());
        
        replicateToFollowers(operation);
        return operation.future();
    }

    private int requiredQuorum(Integer override) {
        int requested = override != null ? override : config.quorum();
        if (requested < 1) {
            requested = 1;
        }
        if (config.strictConsistency()) {
            return requested;
        }
        int members = coordinator.activeMembers().size();
        if (members == 0) {
            members = 1;
        }
        return Math.max(1, Math.min(requested, members));
    }

    private void replicateToFollowers(PendingOperation operation) {
        long seq = globalSequence.incrementAndGet();
        operation.sequence = seq;
        ReplicationPayload payload = new ReplicationPayload(operation.operationId, seq, operation.topic, operation.payload);
        for (NodeInfo member : coordinator.activeMembers()) {
            if (member.nodeId().equals(transport.local().nodeId())) {
                continue;
            }
            ClusterMessage message = ClusterMessage.request(MessageType.REPLICATION_REQUEST,
                    operation.topic,
                    transport.local().nodeId(),
                    member.nodeId(),
                    payload);
            transport.send(message);
        }
        checkCompletion(operation);
    }

    private void retryPending() {
        try {
            if (!running || !coordinator.isLeader()) {
                return;
            }
            for (PendingOperation operation : pending.values()) {
                if (operation.isDone()) {
                    continue;
                }
                checkCompletion(operation);
                if (operation.isDone()) {
                    continue;
                }
                ReplicationPayload payload = new ReplicationPayload(operation.operationId, operation.sequence, operation.topic, operation.payload);
                for (NodeInfo member : coordinator.activeMembers()) {
                    NodeId nodeId = member.nodeId();
                    if (nodeId.equals(transport.local().nodeId())) {
                        continue;
                    }
                    if (operation.isAcked(nodeId)) {
                        continue;
                    }
                    if (!transport.isReachable(nodeId)) {
                        continue;
                    }
                    ClusterMessage message = ClusterMessage.request(MessageType.REPLICATION_REQUEST,
                            operation.topic,
                            transport.local().nodeId(),
                            nodeId,
                            payload);
                    transport.send(message);
                }
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Unexpected error in retryPending loop", t);
        }
    }

    private void checkCompletion(PendingOperation operation) {
        if (operation.isCommitted()) {
            return;
        }
        if (operation.ackCount() >= operation.quorum) {
            if (!operation.localApplied) {
                ReplicationHandler handler = handlers.get(operation.topic);
                if (handler != null) {
                    try {
                        handler.apply(operation.operationId, operation.payload);
                        lastAppliedSequence = Math.max(lastAppliedSequence, operation.sequence);
                        applied.add(operation.operationId);
                        operation.localApplied = true;
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Failed to apply committed operation locally", e);
                        failOperation(operation, e);
                        return;
                    }
                }
            }
            
            operation.complete(OperationStatus.COMMITTED);
            log.computeIfPresent(operation.operationId, (id, record) -> {
                record.status(OperationStatus.COMMITTED);
                return record;
            });
            pending.remove(operation.operationId);
        }
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // No-op
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        for (PendingOperation operation : pending.values()) {
            if (operation.isDone()) {
                continue;
            }
            checkCompletion(operation);
        }
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.REPLICATION_REQUEST) {
            handleReplicationRequest(message);
        } else if (message.type() == MessageType.REPLICATION_ACK) {
            handleReplicationAck(message);
        } else if (message.type() == MessageType.SYNC_REQUEST) {
            handleSyncRequest(message);
        } else if (message.type() == MessageType.SYNC_RESPONSE) {
            handleSyncResponse(message);
        }
    }

    private void handleSyncRequest(ClusterMessage message) {
        if (!coordinator.isLeader()) return;
        SyncRequestPayload payload = message.payload(SyncRequestPayload.class);
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) return;

        ReplicationHandler.SnapshotChunk chunk = handler.getSnapshotChunk(payload.chunkIndex());
        if (chunk == null) return;

        long seq = globalSequence.get();
        SyncResponsePayload responsePayload = new SyncResponsePayload(payload.topic(), seq, payload.chunkIndex(), chunk.hasMore(), chunk.data());
        ClusterMessage response = new ClusterMessage(UUID.randomUUID(),
                message.messageId(),
                MessageType.SYNC_RESPONSE,
                message.qualifier(),
                transport.local().nodeId(),
                message.source(),
                responsePayload,
                5);
        transport.send(response);
    }

    private void handleSyncResponse(ClusterMessage message) {
        SyncResponsePayload payload = message.payload(SyncResponsePayload.class);
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) return;

        executor.submit(() -> {
            try {
                if (payload.chunkIndex() == 0) {
                    LOGGER.info(() -> "Starting sync for " + payload.topic() + " at sequence " + payload.sequence());
                    handler.resetState();
                }
                handler.installSnapshot(payload.data());
                
                if (payload.hasMore()) {
                    requestSync(payload.topic(), payload.chunkIndex() + 1);
                } else {
                    LOGGER.info(() -> "Sync completed for " + payload.topic() + ". Final sequence: " + payload.sequence());
                    lastAppliedSequence = Math.max(lastAppliedSequence, payload.sequence());
                    syncingTopics.remove(payload.topic());
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to install snapshot chunk", e);
                syncingTopics.remove(payload.topic()); // allow retry
            }
        });
    }

    private void requestSync(String topic) {
        requestSync(topic, 0);
    }

    private void requestSync(String topic, int chunkIndex) {
        coordinator.leaderInfo().ifPresent(leader -> {
            if (chunkIndex == 0) {
                LOGGER.info(() -> "Lag detected (" + (coordinator.getTrackedLeaderHighWatermark() - lastAppliedSequence) + "). Requesting sync for " + topic);
            }
            SyncRequestPayload payload = new SyncRequestPayload(topic, chunkIndex);
            ClusterMessage request = ClusterMessage.request(MessageType.SYNC_REQUEST,
                    "sync",
                    transport.local().nodeId(),
                    leader.nodeId(),
                    payload);
            transport.send(request);
        });
    }

    private void handleReplicationRequest(ClusterMessage message) {
        ReplicationPayload payload = message.payload(ReplicationPayload.class);
        UUID opId = payload.operationId();
        if (applied.contains(opId)) {
            sendAck(opId, message.source());
            return;
        }
        if (!processing.add(opId)) {
            return;
        }
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) {
            processing.remove(opId);
            LOGGER.log(Level.WARNING, "No handler registered for topic {0}", payload.topic());
            return;
        }
        executor.submit(() -> {
            try {
                handler.apply(opId, payload.data());
                lastAppliedSequence = Math.max(lastAppliedSequence, payload.sequence());
                log.putIfAbsent(opId, new ReplicatedRecord(opId, payload.topic(), payload.data(), OperationStatus.COMMITTED));
                applied.add(opId);
                sendAck(opId, message.source());
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to apply replicated operation", e);
            } finally {
                processing.remove(opId);
            }
        });
    }

    private void sendAck(UUID operationId, NodeId destination) {
        ReplicationAckPayload ackPayload = new ReplicationAckPayload(operationId, true);
        ClusterMessage ack = ClusterMessage.request(MessageType.REPLICATION_ACK,
                "ack",
                transport.local().nodeId(),
                destination,
                ackPayload);
        transport.send(ack);
    }

    private void handleReplicationAck(ClusterMessage message) {
        ReplicationAckPayload payload = message.payload(ReplicationAckPayload.class);
        PendingOperation operation = pending.get(payload.operationId());
        if (operation == null) {
            return;
        }
        operation.ack(message.source());
        checkCompletion(operation);
    }

    @Override
    public void close() throws IOException {
        stop();
        executor.shutdownNow();
        timeoutScheduler.shutdownNow();
        try {
            executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private int reachableMembersCount() {
        int reachable = 1; // local node
        for (NodeInfo member : coordinator.activeMembers()) {
            NodeId id = member.nodeId();
            if (id.equals(transport.local().nodeId())) {
                continue;
            }
            if (transport.isReachable(id)) {
                reachable++;
            }
        }
        return reachable;
    }

    private void checkTimeouts() {
        try {
            if (!running) {
                return;
            }
            Duration timeout = config.operationTimeout();
            if (timeout.isZero() || timeout.isNegative()) {
                return;
            }
            Instant now = Instant.now();
            List<Failure> toFail = new ArrayList<>();
            for (PendingOperation operation : pending.values()) {
                if (operation.isDone()) {
                    continue;
                }
                if (operation.isExpired(now, timeout)) {
                    TimeoutException ex = new TimeoutException(String.format(
                            "Replication operation timed out operationId=%s timeout=%s",
                            operation.operationId,
                            timeout));
                    toFail.add(new Failure(operation, ex));
                }
            }
            for (Failure failure : toFail) {
                failOperation(failure.operation(), failure.error());
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Unexpected error in checkTimeouts loop", t);
        }
    }

    private void failOperation(PendingOperation operation, Throwable error) {
        if (!pending.remove(operation.operationId, operation)) {
            return;
        }
        operation.fail(error);
        log.computeIfPresent(operation.operationId, (id, record) -> {
            record.status(OperationStatus.REJECTED);
            return record;
        });
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        if (!transport.local().nodeId().equals(newLeader)) {
            failAllPending("Lost leadership to " + newLeader);
        }
    }

    private void failAllPending(String reason) {
        if (pending.isEmpty()) {
            return;
        }
        IllegalStateException ex = new IllegalStateException(reason);
        List<PendingOperation> toFail = new ArrayList<>(pending.values());
        for (PendingOperation op : toFail) {
            failOperation(op, ex);
        }
    }

    private static final class PendingOperation {
        private final UUID operationId;
        private final String topic;
        private final Serializable payload;
        private final int quorum;
        private volatile long sequence;
        private final Set<NodeId> acknowledgements = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
        private volatile OperationStatus status = OperationStatus.PENDING;
        private final Instant createdAt = Instant.now();
        private volatile boolean localApplied = false;

        private PendingOperation(UUID operationId, String topic, Serializable payload, int quorum) {
            this.operationId = operationId;
            this.topic = topic;
            this.payload = payload;
            this.quorum = quorum;
        }

        void ack(NodeId nodeId) {
            acknowledgements.add(nodeId);
        }

        int ackCount() {
            return acknowledgements.size();
        }

        Set<NodeId> acknowledgementsSnapshot() {
            return Set.copyOf(acknowledgements);
        }

        boolean isAcked(NodeId nodeId) {
            return acknowledgements.contains(nodeId);
        }

        boolean isCommitted() {
            return status == OperationStatus.COMMITTED;
        }

        boolean isDone() {
            return future.isDone();
        }

        void complete(OperationStatus status) {
            if (isCommitted()) {
                return;
            }
            this.status = status;
            future.complete(new ReplicationResult(operationId, status));
        }

        void fail(Throwable error) {
            if (future.isDone()) {
                return;
            }
            this.status = OperationStatus.REJECTED;
            future.completeExceptionally(error);
        }

        boolean isExpired(Instant now, Duration timeout) {
            return createdAt.plus(timeout).isBefore(now);
        }

        CompletableFuture<ReplicationResult> future() {
            return future;
        }
    }
}
