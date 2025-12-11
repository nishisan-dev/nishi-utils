package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.OperationStatus;
import dev.nishisan.utils.ngrid.common.ReplicationAckPayload;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coordinates quorum based replication leveraging the transport. The manager handles both
 * leader initiated operations and replication requests coming from other nodes.
 */
public final class ReplicationManager implements TransportListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final ReplicationConfig config;
    private final Map<String, ReplicationHandler> handlers = new ConcurrentHashMap<>();
    private final Map<UUID, PendingOperation> pending = new ConcurrentHashMap<>();
    private final Map<UUID, ReplicatedRecord> log = new ConcurrentHashMap<>();
    private final Set<UUID> applied = new CopyOnWriteArraySet<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ngrid-replication");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;

    public ReplicationManager(Transport transport, ClusterCoordinator coordinator, ReplicationConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.config = Objects.requireNonNull(config, "config");
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        transport.addListener(this);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
    }

    public void registerHandler(String topic, ReplicationHandler handler) {
        handlers.put(topic, handler);
    }

    public CompletableFuture<ReplicationResult> replicate(String topic, Serializable payload) {
        if (!coordinator.isLeader()) {
            throw new IllegalStateException("Replication can only be initiated by the leader");
        }
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            throw new IllegalArgumentException("No replication handler registered for topic: " + topic);
        }
        UUID operationId = UUID.randomUUID();
        PendingOperation operation = new PendingOperation(operationId, topic, payload, requiredQuorum());
        pending.put(operationId, operation);
        log.put(operationId, new ReplicatedRecord(operationId, topic, payload, OperationStatus.PENDING));
        try {
            handler.apply(operationId, payload);
            applied.add(operationId);
        } catch (Exception e) {
            pending.remove(operationId);
            throw new RuntimeException("Failed to apply local operation", e);
        }
        operation.ack(transport.local().nodeId());
        replicateToFollowers(operation);
        return operation.future();
    }

    private int requiredQuorum() {
        int members = coordinator.activeMembers().size();
        if (members == 0) {
            members = 1;
        }
        return Math.max(1, Math.min(config.quorum(), members));
    }

    private void replicateToFollowers(PendingOperation operation) {
        ReplicationPayload payload = new ReplicationPayload(operation.operationId, operation.topic, operation.payload);
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

    private void checkCompletion(PendingOperation operation) {
        if (operation.isCommitted()) {
            return;
        }
        if (operation.ackCount() >= operation.quorum) {
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
        // No-op for now, pending operations will timeout if quorum unattainable
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.REPLICATION_REQUEST) {
            handleReplicationRequest(message);
        } else if (message.type() == MessageType.REPLICATION_ACK) {
            handleReplicationAck(message);
        }
    }

    private void handleReplicationRequest(ClusterMessage message) {
        ReplicationPayload payload = message.payload(ReplicationPayload.class);
        if (!applied.add(payload.operationId())) {
            sendAck(payload.operationId(), message.source());
            return;
        }
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) {
            LOGGER.log(Level.WARNING, "No handler registered for topic {0}", payload.topic());
            return;
        }
        executor.submit(() -> {
            try {
                handler.apply(payload.operationId(), payload.data());
                log.putIfAbsent(payload.operationId(), new ReplicatedRecord(payload.operationId(), payload.topic(), payload.data(), OperationStatus.COMMITTED));
                sendAck(payload.operationId(), message.source());
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to apply replicated operation", e);
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
    }

    private static final class PendingOperation {
        private final UUID operationId;
        private final String topic;
        private final Serializable payload;
        private final int quorum;
        private final Set<NodeId> acknowledgements = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
        private volatile OperationStatus status = OperationStatus.PENDING;

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

        boolean isCommitted() {
            return status == OperationStatus.COMMITTED;
        }

        void complete(OperationStatus status) {
            if (isCommitted()) {
                return;
            }
            this.status = status;
            future.complete(new ReplicationResult(operationId, status));
        }

        CompletableFuture<ReplicationResult> future() {
            return future;
        }
    }
}
