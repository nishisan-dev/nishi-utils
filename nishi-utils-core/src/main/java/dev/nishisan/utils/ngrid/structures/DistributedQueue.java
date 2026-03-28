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

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.QueueNotifyPayload;
import dev.nishisan.utils.ngrid.common.QueueSubscribePayload;
import dev.nishisan.utils.ngrid.common.QueueUnsubscribePayload;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.ngrid.queue.QueueConsumerCursor;
import dev.nishisan.utils.queue.NQueueHeaders;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.IOException;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Public facing distributed queue API. It routes client calls to the current
 * leader and processes remote requests when the local node is in charge.
 *
 * @param <T> the element type
 */
public final class DistributedQueue<T>
        implements TransportListener, LeadershipListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(DistributedQueue.class.getName());
    private static final String COMMAND_PREFIX_OFFER = "queue.offer:";
    private static final String COMMAND_PREFIX_POLL = "queue.poll:";
    private static final String COMMAND_PREFIX_PEEK = "queue.peek:";

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final QueueClusterService<T> queueService;
    private final String queueName;
    private final String queueOfferCommand;
    private final String queuePollCommand;
    private final String queuePeekCommand;
    private final StatsUtils stats;
    private final NodeId localNodeId;
    private final Set<NodeId> subscribers = ConcurrentHashMap.newKeySet();
    private final Object notificationMonitor = new Object();
    private final AtomicBoolean notified = new AtomicBoolean(false);
    private volatile NodeId subscribedLeader;

    /**
     * Creates a distributed queue with no stats tracking.
     *
     * @param transport    the cluster transport
     * @param coordinator  the cluster coordinator
     * @param queueService the underlying queue cluster service
     * @param queueName    the queue name
     */
    public DistributedQueue(Transport transport,
            ClusterCoordinator coordinator,
            QueueClusterService<T> queueService,
            String queueName) {
        this(transport, coordinator, queueService, queueName, null);
    }

    /**
     * Creates a distributed queue with optional stats tracking.
     *
     * @param transport    the cluster transport
     * @param coordinator  the cluster coordinator
     * @param queueService the underlying queue cluster service
     * @param queueName    the queue name
     * @param stats        stats utility, or {@code null}
     */
    public DistributedQueue(Transport transport,
            ClusterCoordinator coordinator,
            QueueClusterService<T> queueService,
            String queueName,
            StatsUtils stats) {
        this.transport = transport;
        this.coordinator = coordinator;
        this.queueService = queueService;
        this.queueName = queueName;
        this.queueOfferCommand = COMMAND_PREFIX_OFFER + queueName;
        this.queuePollCommand = COMMAND_PREFIX_POLL + queueName;
        this.queuePeekCommand = COMMAND_PREFIX_PEEK + queueName;
        this.stats = stats;
        this.localNodeId = transport.local().nodeId();
        transport.addListener(this);
        coordinator.addLeadershipListener(this);
        subscribeIfLeaderPresent();
    }

    /**
     * Returns the underlying {@link QueueClusterService}.
     *
     * <p>
     * <b>For testing only.</b> Do not use in production code.
     */
    public QueueClusterService<T> queueService() {
        return queueService;
    }

    /**
     * Offers an element to the distributed queue without key or headers.
     *
     * @param value the element to offer
     */
    public void offer(T value) {
        offer(null, NQueueHeaders.empty(), value);
    }

    /**
     * Offers an element with a routing key.
     *
     * @param key   optional routing/partitioning key
     * @param value the element to offer
     */
    public void offer(byte[] key, T value) {
        offer(key, NQueueHeaders.empty(), value);
    }

    /**
     * Offers an element with a routing key and custom headers.
     *
     * @param key     optional routing/partitioning key
     * @param headers record headers
     * @param value   the element to offer
     */
    public void offer(byte[] key, NQueueHeaders headers, T value) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            recordQueueOffer();
            queueService.offer(key, headers, value);
            notifySubscribers();
        } else {
            // Forward to leader with full metadata via OfferPayload envelope.
            invokeLeader(queueOfferCommand, new OfferPayload<>(key, headers, value));
        }
    }

    /**
     * Polls the next element from the distributed queue.
     *
     * @return the next element, or empty if the queue is empty
     */
    @SuppressWarnings("unchecked")
    public Optional<T> poll() {
        return poll(QueueConsumerCursor.legacy(localNodeId));
    }

    Optional<T> poll(QueueConsumerCursor cursor) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            Long hintOffset = queueService.getCurrentOffset(cursor.offsetStoreKey());
            return queueService.poll(cursor, hintOffset);
        }
        queueService.syncOffsetsIfNeeded();
        Long offsetHint = queueService.getCurrentOffset(cursor.offsetStoreKey());
        Object result = invokeLeader(queuePollCommand, new QueueReadRequestPayload(cursor.offsetStoreKey(), offsetHint));
        if (result instanceof SerializableOptional) {
            SerializableOptional<?> opt = (SerializableOptional<?>) result;
            if (opt.isPresent()) {
                Object val = opt.value();
                if (val instanceof QueueClusterService.QueueRecord) {
                    QueueClusterService.QueueRecord<T> record = (QueueClusterService.QueueRecord<T>) val;
                    queueService.updateLocalOffset(cursor.offsetStoreKey(), record.offset() + 1);
                    return Optional.of(record.value());
                }
                // Fallback: Jackson may deserialize QueueRecord as Map when type info is lost
                if (val instanceof java.util.Map<?,?> mapVal) {
                    Object rawOffset = mapVal.get("offset");
                    Object rawValue = mapVal.get("value");
                    if (rawOffset instanceof Number && rawValue != null) {
                        long offset = ((Number) rawOffset).longValue();
                        queueService.updateLocalOffset(cursor.offsetStoreKey(), offset + 1);
                        return Optional.of((T) rawValue);
                    }
                }
                return Optional.of((T) val);
            }
            return Optional.empty();
        }
        return Optional.empty();
    }

    /**
     * Peeks at the head of the distributed queue without removing it.
     *
     * @return the head element, or empty if the queue is empty
     */
    @SuppressWarnings("unchecked")
    public Optional<T> peek() {
        if (coordinator.isLeader()) {
            return queueService.peek();
        }
        SerializableOptional<T> result = (SerializableOptional<T>) invokeLeader(queuePeekCommand, (Object) null);
        return result.toOptional();
    }

    Optional<T> peek(QueueConsumerCursor cursor) {
        if (coordinator.isLeader()) {
            return queueService.peek(cursor);
        }
        queueService.syncOffsetsIfNeeded();
        SerializableOptional<T> result = (SerializableOptional<T>) invokeLeader(queuePeekCommand,
                new QueueReadRequestPayload(cursor.offsetStoreKey(), null));
        return result.toOptional();
    }

    /**
     * Opens a logical consumer bound to a stable {@code groupId}/{@code consumerId}
     * pair. This is the recommended stream-style API for TIME_BASED queues.
     *
     * @param groupId logical consumer group
     * @param consumerId stable consumer identity
     * @return the consumer handle
     */
    public DistributedQueueConsumer<T> openConsumer(String groupId, String consumerId) {
        return new DistributedQueueConsumer<>(this, QueueConsumerCursor.of(groupId, consumerId));
    }

    long position(QueueConsumerCursor cursor) {
        queueService.syncOffsetsIfNeeded();
        return queueService.position(cursor);
    }

    void seek(QueueConsumerCursor cursor, long offset) {
        queueService.seek(cursor, offset);
    }

    /**
     * Polls when an element becomes available, blocking up to the given timeout.
     *
     * @param timeout the maximum duration to wait
     * @return the next element, or empty if the timeout expired
     */
    public Optional<T> pollWhenAvailable(Duration timeout) {
        return pollWhenAvailable(QueueConsumerCursor.legacy(localNodeId), timeout);
    }

    Optional<T> pollWhenAvailable(QueueConsumerCursor cursor, Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            return poll(cursor);
        }
        if (coordinator.isLeader()) {
            return poll(cursor);
        }
        Optional<T> immediate = poll(cursor);
        if (immediate.isPresent()) {
            return immediate;
        }
        subscribeIfLeaderPresent();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (consumeNotification()) {
                return poll(cursor);
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            waitForNotification(remaining);
        }
        return Optional.empty();
    }

    /**
     * Subscribes to queue notifications from the current leader.
     */
    public void subscribe() {
        subscribeIfLeaderPresent();
    }

    /**
     * Unsubscribes from queue notifications.
     */
    public void unsubscribe() {
        NodeInfo leaderInfo = coordinator.leaderInfo().orElse(null);
        if (leaderInfo == null || leaderInfo.nodeId().equals(localNodeId)) {
            return;
        }
        QueueUnsubscribePayload payload = new QueueUnsubscribePayload(queueName);
        ClusterMessage request = ClusterMessage.request(MessageType.QUEUE_UNSUBSCRIBE,
                "queue-unsubscribe",
                localNodeId,
                leaderInfo.nodeId(),
                payload);
        transport.send(request);
        subscribedLeader = null;
    }

    private Object invokeLeader(String command, Object body) {
        int attempts = 0;
        while (attempts < 3) {
            attempts++;
            NodeInfo leaderInfo = coordinator.leaderInfo()
                    .orElseThrow(() -> new IllegalStateException("No leader available"));
            if (leaderInfo.nodeId().equals(transport.local().nodeId())) {
                return executeLocal(command, body, transport.local().nodeId());
            }
            ClientRequestPayload payload = new ClientRequestPayload(UUID.randomUUID(), command, body);
            ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST,
                    command,
                    transport.local().nodeId(),
                    leaderInfo.nodeId(),
                    payload);
            try {
                CompletableFuture<ClusterMessage> future = transport.sendAndAwait(request);
                ClusterMessage response = future.join();
                ClientResponsePayload responsePayload = response.payload(ClientResponsePayload.class);
                if (!responsePayload.success()) {
                    if ("Not the leader".equals(responsePayload.error()) && attempts < 3) {
                        LOGGER.info(() -> "Leader rejected request; retrying " + command + " from "
                                + transport.local().nodeId());
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("Interrupted during retry", e);
                        }
                        continue;
                    }
                    throw new IllegalStateException(responsePayload.error());
                }
                return (Object) responsePayload.body();
            } catch (Exception e) {
                if (attempts >= 3) {
                    throw e;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted during retry", ie);
                }
            }
        }
        throw new IllegalStateException("Failed to invoke leader after retries");
    }

    @SuppressWarnings("unchecked")
    private Object executeLocal(String command, Object body, NodeId requestingNode) {
        if (queueOfferCommand.equals(command)) {
            recordQueueOffer();
            if (body instanceof OfferPayload<?>) {
                OfferPayload<T> payload = (OfferPayload<T>) body;
                queueService.offer(payload.key(), payload.headers(), payload.value());
            } else {
                // Legacy path: plain value without key/headers
                queueService.offer((T) body);
            }
            notifySubscribers();
            return Boolean.TRUE;
        }
        if (queuePollCommand.equals(command)) {
            String consumerKey;
            Long hintOffset;
            if (body instanceof QueueReadRequestPayload readPayload) {
                consumerKey = readPayload.consumerKey();
                hintOffset = readPayload.hintOffset();
            } else {
                consumerKey = QueueConsumerCursor.legacy(requestingNode).offsetStoreKey();
                hintOffset = body instanceof Long ? (Long) body : null;
            }
            Optional<QueueClusterService.QueueRecord<T>> recordOpt = queueService.pollRecord(consumerKey, hintOffset);
            if (recordOpt.isPresent()) {
                recordQueuePoll();
                return SerializableOptional.of(recordOpt.get());
            }
            return SerializableOptional.empty();
        }
        if (queuePeekCommand.equals(command)) {
            if (body instanceof QueueReadRequestPayload readPayload) {
                return (Object) queueService.peek(QueueConsumerCursor.fromOffsetStoreKey(readPayload.consumerKey()))
                        .map(SerializableOptional::of)
                        .orElseGet(SerializableOptional::empty);
            }
            return (Object) queueService.peek()
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        throw new IllegalArgumentException("Unknown command: " + command);
    }

    /** {@inheritDoc} */
    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override
    public void onPeerDisconnected(NodeId peerId) {
        subscribers.remove(peerId);
        if (peerId != null && peerId.equals(subscribedLeader)) {
            subscribedLeader = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.QUEUE_NOTIFY) {
            QueueNotifyPayload payload = message.payload(QueueNotifyPayload.class);
            if (queueName.equals(payload.queueName())) {
                markNotified();
            }
            return;
        }
        if (message.type() == MessageType.QUEUE_SUBSCRIBE) {
            if (!coordinator.isLeader()) {
                return;
            }
            QueueSubscribePayload payload = message.payload(QueueSubscribePayload.class);
            if (!queueName.equals(payload.queueName())) {
                return;
            }
            subscribers.add(message.source());
            if (queueService.peek().isPresent()) {
                notifySubscriber(message.source());
            }
            return;
        }
        if (message.type() == MessageType.QUEUE_UNSUBSCRIBE) {
            if (!coordinator.isLeader()) {
                return;
            }
            QueueUnsubscribePayload payload = message.payload(QueueUnsubscribePayload.class);
            if (!queueName.equals(payload.queueName())) {
                return;
            }
            subscribers.remove(message.source());
            return;
        }
        if (message.type() != MessageType.CLIENT_REQUEST) {
            return;
        }
        ClientRequestPayload payload = message.payload(ClientRequestPayload.class);
        if (!Set.of(queueOfferCommand, queuePollCommand, queuePeekCommand).contains(payload.command())) {
            return;
        }
        LOGGER.info(() -> "Received client request " + payload.command() + " for queue " + queueName + " from "
                + message.source() + " (leader=" + coordinator.isLeader() + ")");
        ClientResponsePayload responsePayload;
        if (!coordinator.isLeader()) {
            LOGGER.info(() -> "Rejecting client request (not leader) for " + payload.command() + " from "
                    + message.source());
            responsePayload = new ClientResponsePayload(payload.requestId(), false, null, "Not the leader");
        } else {
            try {
                Object result = executeLocal(payload.command(), payload.body(), message.source());
                if (queuePollCommand.equals(payload.command())) {
                    subscribers.add(message.source());
                }
                responsePayload = new ClientResponsePayload(payload.requestId(), true, result, null);
            } catch (RuntimeException e) {
                String messageText = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                responsePayload = new ClientResponsePayload(payload.requestId(), false, null, messageText);
            }
        }
        ClusterMessage response = ClusterMessage.response(message, responsePayload);
        transport.send(response);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        transport.removeListener(this);
        coordinator.removeLeadershipListener(this);
        unsubscribe();
        queueService.close();
    }

    private void recordQueueOffer() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.writeNode(localNodeId));
        stats.notifyHitCounter(NGridMetrics.queueOffer(localNodeId));
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderChanged(NodeId newLeader) {
        if (localNodeId.equals(newLeader)) {
            subscribedLeader = null;
            return;
        }
        subscribedLeader = null;
        queueService.syncOffsetsIfNeeded();
        subscribeIfLeaderPresent();
    }

    private void subscribeIfLeaderPresent() {
        NodeInfo leaderInfo = coordinator.leaderInfo().orElse(null);
        if (leaderInfo == null) {
            return;
        }
        if (leaderInfo.nodeId().equals(localNodeId)) {
            return;
        }
        if (leaderInfo.nodeId().equals(subscribedLeader)) {
            return;
        }
        queueService.syncOffsetsIfNeeded();
        QueueSubscribePayload payload = new QueueSubscribePayload(queueName);
        ClusterMessage request = ClusterMessage.request(MessageType.QUEUE_SUBSCRIBE,
                "queue-subscribe",
                localNodeId,
                leaderInfo.nodeId(),
                payload);
        transport.send(request);
        markNotified();
        subscribedLeader = leaderInfo.nodeId();
    }

    private void notifySubscribers() {
        if (!coordinator.isLeader()) {
            return;
        }
        for (NodeId subscriber : subscribers) {
            notifySubscriber(subscriber);
        }
    }

    private void notifySubscriber(NodeId subscriber) {
        if (subscriber == null || subscriber.equals(localNodeId)) {
            return;
        }
        QueueNotifyPayload payload = new QueueNotifyPayload(queueName);
        ClusterMessage notify = ClusterMessage.request(MessageType.QUEUE_NOTIFY,
                "queue-notify",
                localNodeId,
                subscriber,
                payload);
        transport.send(notify);
    }

    private void markNotified() {
        notified.set(true);
        synchronized (notificationMonitor) {
            notificationMonitor.notifyAll();
        }
    }

    private boolean consumeNotification() {
        return notified.getAndSet(false);
    }

    private void waitForNotification(long millis) {
        synchronized (notificationMonitor) {
            if (notified.get()) {
                return;
            }
            try {
                notificationMonitor.wait(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void recordQueuePoll() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.writeNode(localNodeId));
        stats.notifyHitCounter(NGridMetrics.queuePoll(localNodeId));
    }

    private void recordIngressWrite() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.ingressWrite(localNodeId));
    }
}
