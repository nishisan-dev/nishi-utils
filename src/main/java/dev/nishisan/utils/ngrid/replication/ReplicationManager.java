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
import dev.nishisan.utils.ngrid.common.SequenceResendRequestPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendResponsePayload;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coordinates quorum based replication leveraging the transport. The manager
 * handles both
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
    private final Map<String, java.util.concurrent.atomic.AtomicLong> sequenceByTopic = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong appliedSequence = new java.util.concurrent.atomic.AtomicLong(
            0);
    private volatile long lastAppliedSequence = 0;
    private final Set<String> syncingTopics = ConcurrentHashMap.newKeySet();

    // Multi-thread executor to prevent starvation when callbacks recursively submit
    // tasks
    // (e.g., processSequenceBuffer calling applyReplication which callbacks to
    // processSequenceBuffer)
    private final ExecutorService executor = Executors.newFixedThreadPool(4, r -> {
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

    // Sequence ordering structures - PER TOPIC
    private final Map<String, PriorityQueue<BufferedReplication>> sequenceBufferByTopic = new ConcurrentHashMap<>();
    private final Map<String, Long> nextExpectedSequenceByTopic = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, Instant>> sequenceWaitStartByTopic = new ConcurrentHashMap<>();
    private static final Duration SEQUENCE_WAIT_TIMEOUT = Duration.ofSeconds(1);
    private final Path sequenceStatePath;
    private final ReentrantLock sequenceBufferLock = new ReentrantLock();

    private static record Failure(PendingOperation operation, Throwable error) {
    }

    /**
     * Buffered replication operation waiting for its sequence turn.
     */
    private static record BufferedReplication(
            ReplicationPayload payload,
            ClusterMessage originalMessage,
            Instant receivedAt) implements Comparable<BufferedReplication> {
        long sequence() {
            return payload.sequence();
        }

        @Override
        public int compareTo(BufferedReplication other) {
            return Long.compare(this.sequence(), other.sequence());
        }
    }

    public ReplicationManager(Transport transport, ClusterCoordinator coordinator, ReplicationConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.config = Objects.requireNonNull(config, "config");
        this.sequenceStatePath = config.dataDirectory().resolve("sequence-state.dat");
        loadSequenceState();
    }

    /**
     * Protected constructor for testing purposes only.
     */
    protected ReplicationManager() {
        this.transport = null;
        this.coordinator = null;
        this.config = null;
        this.sequenceStatePath = null;
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
     * bound their waiting time (e.g. higher-level services calling
     * {@code future.get(...)}).
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
        globalSequence.incrementAndGet();
        long seq = nextSequenceForTopic(operation.topic);
        operation.sequence = seq;

        // Persist sequence state for leader recovery
        saveSequenceState();

        ReplicationPayload payload = new ReplicationPayload(operation.operationId, seq, operation.topic,
                operation.payload);
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
                ReplicationPayload payload = new ReplicationPayload(operation.operationId, operation.sequence,
                        operation.topic, operation.payload);
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
            ReplicationHandler handler = handlers.get(operation.topic);
            if (handler != null) {
                if (operation.markLocalApplyStarted()) {
                    // LEADER PATH: Execute apply ASYNCHRONOUSLY to avoid deadlock
                    executor.submit(() -> {
                        try {
                            handler.apply(operation.operationId, operation.payload);
                            recordApplied();
                            applied.add(operation.operationId);
                            operation.markLocalApplied();

                            // Complete operation after successful apply
                            completeOperation(operation);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Failed to apply committed operation locally", e);
                            failOperation(operation, e);
                        }
                    });
                }
                return; // Completion happens in callback
            }

            completeOperation(operation);
        }
    }

    private void completeOperation(PendingOperation operation) {
        operation.complete(OperationStatus.COMMITTED);
        log.computeIfPresent(operation.operationId, (id, record) -> {
            record.status(OperationStatus.COMMITTED);
            return record;
        });
        pending.remove(operation.operationId);
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
            // Dynamically adjust quorum based on reachable members
            if (!config.strictConsistency()) {
                int newQuorum = computeQuorumForReachable(operation.originalQuorum);
                operation.updateQuorum(newQuorum);
                LOGGER.info(() -> String.format(
                        "Peer %s disconnected, adjusted quorum from %d to %d for operation %s",
                        peerId, operation.originalQuorum, newQuorum, operation.operationId));
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
        if (!coordinator.isLeader())
            return;
        SyncRequestPayload payload = message.payload(SyncRequestPayload.class);
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null)
            return;

        ReplicationHandler.SnapshotChunk chunk = handler.getSnapshotChunk(payload.chunkIndex());
        if (chunk == null)
            return;

        long seq = globalSequence.get();
        SyncResponsePayload responsePayload = new SyncResponsePayload(payload.topic(), seq, payload.chunkIndex(),
                chunk.hasMore(), chunk.data());
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
        if (handler == null)
            return;

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
                    LOGGER.info(
                            () -> "Sync completed for " + payload.topic() + ". Final sequence: " + payload.sequence());
                    appliedSequence.updateAndGet(current -> Math.max(current, payload.sequence()));
                    lastAppliedSequence = appliedSequence.get();
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
                LOGGER.info(() -> "Lag detected (" + (coordinator.getTrackedLeaderHighWatermark() - lastAppliedSequence)
                        + "). Requesting sync for " + topic);
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
        long seq = payload.sequence();

        LOGGER.info(() -> String.format(
                "Replication request opId=%s seq=%d topic=%s from=%s",
                opId, seq, payload.topic(), message.source()));

        // Already applied previously
        if (applied.contains(opId)) {
            sendAck(opId, message.source());
            return;
        }

        // Lock to manipulate buffer and sequence
        String topic = payload.topic();
        sequenceBufferLock.lock();
        try {
            // Initialize per-topic structures if needed
            long nextExpected = nextExpectedSequenceByTopic.computeIfAbsent(topic, k -> 1L);
            PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.computeIfAbsent(
                    topic, k -> new PriorityQueue<>());
            Map<Long, Instant> waitStart = sequenceWaitStartByTopic.computeIfAbsent(
                    topic, k -> new ConcurrentHashMap<>());

            if (seq == nextExpected) {
                // Create callback to execute AFTER successful apply
                Runnable onSuccess = () -> {
                    sequenceBufferLock.lock();
                    try {
                        // Update state ONLY if still the expected sequence (idempotency check)
                        long current = nextExpectedSequenceByTopic.get(topic);
                        if (current == nextExpected) {
                            recordApplied();
                            log.putIfAbsent(opId, new ReplicatedRecord(
                                    opId, payload.topic(), payload.data(), OperationStatus.COMMITTED));
                            applied.add(opId);

                            // SEND ACK (only after successful apply)
                            sendAck(opId, message.source());

                            // Advance sequence
                            nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
                            saveSequenceState();

                            // Process buffer recursively
                            processSequenceBuffer(topic);
                        }
                    } finally {
                        sequenceBufferLock.unlock();
                    }
                };

                // Apply asynchronously with callback
                applyReplication(payload, message, onSuccess);
            } else if (seq > nextExpected) {
                // Future sequence, add to buffer
                BufferedReplication buffered = new BufferedReplication(
                        payload, message, Instant.now());
                buffer.add(buffered);
                waitStart.putIfAbsent(seq, Instant.now());

                LOGGER.info(() -> String.format(
                        "Buffered future sequence seq=%d (expecting=%d) for topic=%s",
                        seq, nextExpected, topic));

                // Check if we have gaps and if timeout expired
                checkForMissingSequences(topic);
            } else {
                // Old sequence (duplicate or already processed), ignore
                LOGGER.warning(() -> String.format(
                        "Received old sequence seq=%d (expecting=%d) for topic=%s, ignoring",
                        seq, nextExpected, topic));
            }
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Applies a replication operation ASYNCHRONOUSLY (must be called with
     * sequenceBufferLock held, but releases it before applying).
     * Executes handler.apply() in the executor thread pool, then invokes the
     * onSuccess callback to advance sequences.
     */
    private void applyReplication(ReplicationPayload payload, ClusterMessage message, Runnable onSuccess) {
        UUID opId = payload.operationId();

        if (!processing.add(opId)) {
            return; // Already being processed
        }

        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) {
            processing.remove(opId);
            LOGGER.log(Level.WARNING, "No handler registered for topic {0}", payload.topic());
            return;
        }

        // Submit to executor and return IMMEDIATELY (without holding any locks)
        executor.submit(() -> {
            try {
                // Execute apply WITHOUT holding sequenceBufferLock
                handler.apply(opId, payload.data());

                // CALLBACK: re-acquire lock and advance state ONLY after successful apply
                if (onSuccess != null) {
                    onSuccess.run();
                }

                LOGGER.info(() -> String.format(
                        "Applied replication opId=%s seq=%d", opId, payload.sequence()));
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to apply replicated operation", e);
            } finally {
                processing.remove(opId);
            }
        });
    }

    /**
     * Processes buffered sequences in order (must be called with sequenceBufferLock
     * held). Recursively processes ONE buffered item at a time via callbacks.
     */
    private void processSequenceBuffer(String topic) {
        PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.get(topic);
        Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(topic);

        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        BufferedReplication next = buffer.peek();
        long nextExpected = nextExpectedSequenceByTopic.get(topic);

        if (next.sequence() != nextExpected) {
            // Next in buffer is not the expected one, stop
            return;
        }

        // Remove from buffer
        buffer.poll();
        if (waitStart != null) {
            waitStart.remove(next.sequence());
        }

        // Create callback for recursive processing
        Runnable onSuccess = () -> {
            sequenceBufferLock.lock();
            try {
                // Update state ONLY if still the expected sequence (idempotency check)
                long current = nextExpectedSequenceByTopic.get(topic);
                if (current == nextExpected) {
                    recordApplied();
                    log.putIfAbsent(next.payload().operationId(),
                            new ReplicatedRecord(next.payload().operationId(),
                                    next.payload().topic(), next.payload().data(),
                                    OperationStatus.COMMITTED));
                    applied.add(next.payload().operationId());

                    // SEND ACK
                    sendAck(next.payload().operationId(), next.originalMessage().source());

                    // Advance sequence
                    nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
                    saveSequenceState();

                    LOGGER.info(() -> String.format(
                            "Processed buffered sequence seq=%d for topic=%s", next.sequence(), topic));

                    // RECURSION: Process next buffered item
                    processSequenceBuffer(topic);
                }
            } finally {
                sequenceBufferLock.unlock();
            }
        };

        // Apply asynchronously with recursive callback
        applyReplication(next.payload(), next.originalMessage(), onSuccess);
    }

    /**
     * Checks for missing sequences and requests them if timeout expired
     * (must be called with sequenceBufferLock held).
     */
    private void checkForMissingSequences(String topic) {
        PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.get(topic);
        Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(topic);

        if (buffer == null || buffer.isEmpty() || waitStart == null) {
            return;
        }

        Instant now = Instant.now();
        long nextInBuffer = buffer.peek().sequence();
        long nextExpected = nextExpectedSequenceByTopic.get(topic);
        long gap = nextInBuffer - nextExpected;

        if (gap > 0) {
            // We have a gap, check timeout
            Instant waitStartTime = waitStart.get(nextInBuffer);

            if (waitStartTime != null) {
                Duration waited = Duration.between(waitStartTime, now);

                if (waited.compareTo(SEQUENCE_WAIT_TIMEOUT) > 0) {
                    LOGGER.warning(() -> String.format(
                            "Timeout waiting for seq=%d (waited %dms) for topic=%s, will request from leader",
                            nextExpected, waited.toMillis(), topic));

                    // Note: Request will be implemented in Phase 3
                    // For now, just log the gap
                }
            }
        }
    }

    private void sendAck(UUID operationId, NodeId destination) {
        LOGGER.info(() -> "Sending replication ack for " + operationId + " to " + destination);
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
        LOGGER.info(() -> "Replication ack for " + payload.operationId() + " from " + message.source());
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

    /**
     * Computes the effective quorum based on currently reachable members.
     * In non-strict consistency mode, this allows operations to complete
     * with a reduced quorum when peers disconnect (e.g., during failover).
     *
     * @param originalQuorum the originally requested quorum
     * @return the adjusted quorum, at least 1 and at most originalQuorum
     */
    private int computeQuorumForReachable(int originalQuorum) {
        if (config.strictConsistency()) {
            return originalQuorum;
        }
        int reachable = reachableMembersCount();
        return Math.max(1, Math.min(originalQuorum, reachable));
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
        private final int originalQuorum;
        private volatile int quorum;
        private volatile long sequence;
        private final Set<NodeId> acknowledgements = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
        private volatile OperationStatus status = OperationStatus.PENDING;
        private final Instant createdAt = Instant.now();
        private final java.util.concurrent.atomic.AtomicBoolean localApplied = new java.util.concurrent.atomic.AtomicBoolean(
                false);
        private final java.util.concurrent.atomic.AtomicBoolean localApplyStarted = new java.util.concurrent.atomic.AtomicBoolean(
                false);

        private PendingOperation(UUID operationId, String topic, Serializable payload, int quorum) {
            this.operationId = operationId;
            this.topic = topic;
            this.payload = payload;
            this.originalQuorum = quorum;
            this.quorum = quorum;
        }

        /**
         * Updates the quorum to a new value, typically when peers disconnect.
         * The new quorum cannot exceed the original quorum and must be at least 1.
         */
        void updateQuorum(int newQuorum) {
            this.quorum = Math.max(1, Math.min(newQuorum, originalQuorum));
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

        boolean markLocalApplyStarted() {
            return localApplyStarted.compareAndSet(false, true);
        }

        void markLocalApplied() {
            localApplied.set(true);
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

    private long nextSequenceForTopic(String topic) {
        java.util.concurrent.atomic.AtomicLong counter = sequenceByTopic.computeIfAbsent(topic,
                key -> new java.util.concurrent.atomic.AtomicLong(0));
        long baseline = nextExpectedSequenceByTopic.getOrDefault(topic, 1L) - 1;
        counter.updateAndGet(current -> Math.max(current, baseline));
        return counter.incrementAndGet();
    }

    private void recordApplied() {
        lastAppliedSequence = appliedSequence.incrementAndGet();
    }

    /**
     * Loads the last saved sequence state from disk.
     */
    private void loadSequenceState() {
        if (!Files.exists(sequenceStatePath)) {
            LOGGER.info(() -> "No saved sequence state found, starting from sequence 1");
            return;
        }

        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                Files.newInputStream(sequenceStatePath))) {
            @SuppressWarnings("unchecked")
            Map<String, Long> loaded = (Map<String, Long>) ois.readObject();
            nextExpectedSequenceByTopic.putAll(loaded);

            // Load globalSequence (stored as "_global" key for compatibility)
            Long savedGlobal = loaded.get("_global");
            if (savedGlobal != null) {
                globalSequence.set(savedGlobal);
            }

            // Load per-topic sequences (keys starting with "_topic:")
            loaded.entrySet().stream()
                    .filter(e -> e.getKey().startsWith("_topic:"))
                    .forEach(e -> {
                        String topic = e.getKey().substring(7); // Remove "_topic:" prefix
                        sequenceByTopic.computeIfAbsent(topic,
                                k -> new java.util.concurrent.atomic.AtomicLong(0))
                                .set(e.getValue());
                    });

            LOGGER.info(() -> "Loaded sequence state: global=" + globalSequence.get() +
                    ", topics=" + sequenceByTopic.keySet());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load sequence state, starting from 1", e);
        }
    }

    /**
     * Saves the current sequence state to disk.
     * Saves:
     * - nextExpectedSequenceByTopic (for followers)
     * - globalSequence (for leader, stored as "_global" key)
     * - sequenceByTopic (for leader, stored as "_topic:{topic}" keys)
     */
    private void saveSequenceState() {
        if (sequenceStatePath == null) {
            return; // Test mode, no persistence
        }
        try {
            Files.createDirectories(sequenceStatePath.getParent());
            Map<String, Long> toSave = new HashMap<>(nextExpectedSequenceByTopic);

            // Add global sequence
            toSave.put("_global", globalSequence.get());

            // Add per-topic sequences
            sequenceByTopic.forEach((topic, seq) -> toSave.put("_topic:" + topic, seq.get()));

            try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                    Files.newOutputStream(sequenceStatePath))) {
                oos.writeObject(toSave);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to save sequence state", e);
        }
    }
}
