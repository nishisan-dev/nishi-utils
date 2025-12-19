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
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Public facing distributed queue API. It routes client calls to the current leader and
 * processes remote requests when the local node is in charge.
 */
public final class DistributedQueue<T extends Serializable> implements TransportListener, Closeable {
    private static final String QUEUE_OFFER = "queue.offer";
    private static final String QUEUE_POLL = "queue.poll";
    private static final String QUEUE_PEEK = "queue.peek";

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final QueueClusterService<T> queueService;
    private final StatsUtils stats;
    private final NodeId localNodeId;

    public DistributedQueue(Transport transport, ClusterCoordinator coordinator, QueueClusterService<T> queueService) {
        this(transport, coordinator, queueService, null);
    }

    public DistributedQueue(Transport transport, ClusterCoordinator coordinator, QueueClusterService<T> queueService, StatsUtils stats) {
        this.transport = transport;
        this.coordinator = coordinator;
        this.queueService = queueService;
        this.stats = stats;
        this.localNodeId = transport.local().nodeId();
        transport.addListener(this);
    }

    public void offer(T value) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            recordQueueOffer();
            queueService.offer(value);
        } else {
            invokeLeader(QUEUE_OFFER, value);
        }
    }

    @SuppressWarnings("unchecked")
    public Optional<T> poll() {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            Optional<T> value = queueService.poll();
            if (value.isPresent()) {
                recordQueuePoll();
            }
            return value;
        }
        SerializableOptional<T> result = (SerializableOptional<T>) invokeLeader(QUEUE_POLL, null);
        return result.toOptional();
    }

    @SuppressWarnings("unchecked")
    public Optional<T> peek() {
        if (coordinator.isLeader()) {
            return queueService.peek();
        }
        SerializableOptional<T> result = (SerializableOptional<T>) invokeLeader(QUEUE_PEEK, null);
        return result.toOptional();
    }

    private Serializable invokeLeader(String command, Serializable body) {
        int attempts = 0;
        while (attempts < 3) {
            attempts++;
            NodeInfo leaderInfo = coordinator.leaderInfo().orElseThrow(() -> new IllegalStateException("No leader available"));
            if (leaderInfo.nodeId().equals(transport.local().nodeId())) {
                return executeLocal(command, body);
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
                return responsePayload.body();
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
    private Serializable executeLocal(String command, Serializable body) {
        return switch (command) {
            case QUEUE_OFFER -> {
                recordQueueOffer();
                queueService.offer((T) body);
                yield Boolean.TRUE;
            }
            case QUEUE_POLL -> {
                Optional<T> value = queueService.poll();
                if (value.isPresent()) {
                    recordQueuePoll();
                }
                yield (Serializable) value
                        .map(SerializableOptional::of)
                        .orElseGet(SerializableOptional::empty);
            }
            case QUEUE_PEEK -> (Serializable) queueService.peek()
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        };
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
        if (message.type() != MessageType.CLIENT_REQUEST) {
            return;
        }
        ClientRequestPayload payload = message.payload(ClientRequestPayload.class);
        if (!Set.of(QUEUE_OFFER, QUEUE_POLL, QUEUE_PEEK).contains(payload.command())) {
            return;
        }
        ClientResponsePayload responsePayload;
        if (!coordinator.isLeader()) {
            responsePayload = new ClientResponsePayload(payload.requestId(), false, null, "Not the leader");
        } else {
            try {
                Serializable result = executeLocal(payload.command(), payload.body());
                responsePayload = new ClientResponsePayload(payload.requestId(), true, result, null);
            } catch (RuntimeException e) {
                String messageText = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                responsePayload = new ClientResponsePayload(payload.requestId(), false, null, messageText);
            }
        }
        ClusterMessage response = ClusterMessage.response(message, responsePayload);
        transport.send(response);
    }

    @Override
    public void close() throws IOException {
        transport.removeListener(this);
        queueService.close();
    }

    private void recordQueueOffer() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.writeNode(localNodeId));
        stats.notifyHitCounter(NGridMetrics.queueOffer(localNodeId));
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
