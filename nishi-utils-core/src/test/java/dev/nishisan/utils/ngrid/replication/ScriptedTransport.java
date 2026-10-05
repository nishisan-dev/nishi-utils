/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Transporte em memória para testes do {@link ReplicationManager} sem TCP: grava tudo o que o nó
 * envia e entrega mensagens "de peers" direto aos listeners registrados.
 */
final class ScriptedTransport implements Transport {

    private final NodeInfo local;
    private final List<NodeInfo> peers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
    private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
    private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

    ScriptedTransport(NodeInfo local, List<NodeInfo> initialPeers) {
        this.local = local;
        for (NodeInfo peer : initialPeers) {
            peers.add(peer);
            connected.put(peer.nodeId(), true);
        }
    }

    /** Registra um peer como conectado e avisa os listeners (handshake concluído). */
    void connect(NodeInfo peer) {
        peers.removeIf(p -> p.nodeId().equals(peer.nodeId()));
        peers.add(peer);
        connected.put(peer.nodeId(), true);
        for (TransportListener listener : listeners) {
            listener.onPeerConnected(peer);
        }
    }

    /** Entrega uma mensagem a todos os listeners, como se tivesse chegado da rede. */
    void deliver(ClusterMessage message) {
        for (TransportListener listener : listeners) {
            listener.onMessage(message);
        }
    }

    /** Mensagens do tipo dado enviadas pelo nó local, em ordem. */
    List<ClusterMessage> sentOfType(MessageType type) {
        List<ClusterMessage> out = new ArrayList<>();
        for (ClusterMessage message : sent) {
            if (message.type() == type) {
                out.add(message);
            }
        }
        return out;
    }

    /** Builds a wire-shaped snapshot response correlated to the latest requested topic/chunk. */
    static ClusterMessage syncResponse(Collection<ClusterMessage> sent, NodeId source,
                                       dev.nishisan.utils.ngrid.common.SyncResponsePayload payload) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            ClusterMessage request = null;
            for (ClusterMessage message : sent) {
                if (message.type() != MessageType.SYNC_REQUEST) continue;
                var requested = message.payload(dev.nishisan.utils.ngrid.common.SyncRequestPayload.class);
                if (requested.topic().equals(payload.topic()) && requested.chunkIndex() == payload.chunkIndex()) {
                    request = message;
                }
            }
            if (request != null) {
                return new ClusterMessage(null, request.messageId(), MessageType.SYNC_RESPONSE,
                        request.qualifier(), source, request.source(), payload, 5);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting snapshot request", e);
            }
        }
        throw new AssertionError("no snapshot request for " + payload.topic() + " chunk " + payload.chunkIndex());
    }

    /** Descarta o histórico de mensagens enviadas. */
    void clearSent() {
        sent.clear();
    }

    @Override
    public void start() {
    }

    @Override
    public NodeInfo local() {
        return local;
    }

    @Override
    public Collection<NodeInfo> peers() {
        List<NodeInfo> all = new ArrayList<>();
        all.add(local);
        all.addAll(peers);
        return all;
    }

    @Override
    public void addPeer(NodeInfo peer) {
        peers.add(peer);
        connected.put(peer.nodeId(), true);
    }

    @Override
    public void addListener(TransportListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(TransportListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void broadcast(ClusterMessage message) {
        sent.add(message);
    }

    @Override
    public void send(ClusterMessage message) {
        sent.add(message);
    }

    @Override
    public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
        sent.add(message);
        CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("not used"));
        return future;
    }

    @Override
    public boolean isConnected(NodeId nodeId) {
        return Boolean.TRUE.equals(connected.get(nodeId));
    }

    @Override
    public boolean isReachable(NodeId nodeId) {
        return isConnected(nodeId);
    }

    @Override
    public void close() {
    }
}
