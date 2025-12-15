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

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TCP based transport implementation. It provides best-effort reconnection semantics and
 * a simple handshake protocol that exchanges the list of known peers to gradually form a
 * mesh between all nodes.
 */
public final class TcpTransport implements Transport {
    private static final Logger LOGGER = Logger.getLogger(TcpTransport.class.getName());

    private final TcpTransportConfig config;
    private final Map<NodeId, NodeInfo> knownPeers = new ConcurrentHashMap<>();
    private final Map<NodeId, Connection> connections = new ConcurrentHashMap<>();
    private final Set<TransportListener> listeners = new CopyOnWriteArraySet<>();
    private final Map<UUID, PendingResponse> pendingResponses = new ConcurrentHashMap<>();
    private final ExecutorService workerPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "ngrid-transport-worker");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ngrid-transport-scheduler");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private ServerSocket serverSocket;

    public TcpTransport(TcpTransportConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        knownPeers.put(config.local().nodeId(), config.local());
        config.initialPeers().forEach(p -> knownPeers.putIfAbsent(p.nodeId(), p));
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(config.local().host(), config.local().port()));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to bind TCP transport", e);
        }
        running = true;
        workerPool.submit(this::acceptLoop);
        scheduler.scheduleAtFixedRate(this::reconnectLoop,
                config.reconnectInterval().toMillis(),
                config.reconnectInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        // attempt initial outbound connections
        config.initialPeers().forEach(this::ensureConnectionAsync);
    }

    @Override
    public NodeInfo local() {
        return config.local();
    }

    @Override
    public Collection<NodeInfo> peers() {
        return Collections.unmodifiableCollection(knownPeers.values());
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
        for (NodeId nodeId : knownPeers.keySet()) {
            if (!nodeId.equals(config.local().nodeId())) {
                send(messageForPeer(message, nodeId));
            }
        }
    }

    private ClusterMessage messageForPeer(ClusterMessage original, NodeId destination) {
        return new ClusterMessage(UUID.randomUUID(),
                original.correlationId().orElse(null),
                original.type(),
                original.qualifier(),
                original.source(),
                destination,
                original.payload());
    }

    @Override
    public void send(ClusterMessage message) {
        NodeId destination = message.destination();
        if (destination == null) {
            return;
        }
        Connection connection = ensureConnection(destination);
        if (connection != null) {
            connection.send(message);
        } else {
            LOGGER.log(Level.WARNING, "No connection available for {0}", destination);
        }
    }

    @Override
    public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
        CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
        NodeId destination = message.destination();
        if (destination == null) {
            future.completeExceptionally(new IOException("sendAndAwait requires a destination"));
            return future;
        }
        UUID requestId = message.messageId();
        PendingResponse response = new PendingResponse(destination, future);
        pendingResponses.put(requestId, response);

        Connection connection = ensureConnection(destination);
        if (connection == null) {
            pendingResponses.remove(requestId, response);
            future.completeExceptionally(new IOException("No connection available for " + destination));
            return future;
        }
        connection.send(message);

        if (!config.requestTimeout().isZero() && !config.requestTimeout().isNegative()) {
            long timeoutMs = config.requestTimeout().toMillis();
            ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
                PendingResponse pr = pendingResponses.remove(requestId);
                if (pr != null) {
                    pr.clearTimeoutTask();
                    pr.future.completeExceptionally(new TimeoutException(String.format(
                            "Request timed out requestId=%s destination=%s timeout=%s",
                            requestId,
                            destination,
                            config.requestTimeout())));
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
            response.setTimeoutTask(timeoutTask);
        }
        return future;
    }

    @Override
    public boolean isConnected(NodeId nodeId) {
        Connection conn = connections.get(nodeId);
        return conn != null && conn.isOpen();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                registerConnection(socket, null);
            } catch (SocketException se) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Server socket closed unexpectedly", se);
                }
                break;
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Error accepting connection", e);
                }
            }
        }
    }

    private void reconnectLoop() {
        if (!running) {
            return;
        }
        for (NodeInfo peer : knownPeers.values()) {
            if (peer.nodeId().equals(config.local().nodeId())) {
                continue;
            }
            ensureConnectionAsync(peer);
        }
    }

    private void ensureConnectionAsync(NodeInfo peer) {
        workerPool.submit(() -> ensureConnection(peer.nodeId()));
    }

    private Connection ensureConnection(NodeId nodeId) {
        if (nodeId.equals(config.local().nodeId())) {
            return null;
        }
        Connection current = connections.get(nodeId);
        if (current != null && current.isOpen()) {
            return current;
        }
        NodeInfo nodeInfo = knownPeers.get(nodeId);
        if (nodeInfo == null) {
            return null;
        }
        synchronized (getLockFor(nodeId)) {
            current = connections.get(nodeId);
            if (current != null && current.isOpen()) {
                return current;
            }
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(nodeInfo.host(), nodeInfo.port()),
                        (int) config.connectTimeout().toMillis());
                Connection connection = registerConnection(socket, nodeInfo);
                sendHandshake(connection);
                return connection;
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "Unable to connect to {0}: {1}", new Object[]{nodeInfo, e.getMessage()});
                return null;
            }
        }
    }

    private final Map<NodeId, Object> connectionLocks = new ConcurrentHashMap<>();

    private Object getLockFor(NodeId nodeId) {
        return connectionLocks.computeIfAbsent(nodeId, id -> new Object());
    }

    private Connection registerConnection(Socket socket, NodeInfo preResolved) throws IOException {
        socket.setTcpNoDelay(true);
        Connection connection = new Connection(socket);
        if (preResolved != null) {
            connection.setRemote(preResolved);
            connections.put(preResolved.nodeId(), connection);
        }
        workerPool.submit(connection::readLoop);
        return connection;
    }

    private void sendHandshake(Connection connection) {
        NodeInfo localInfo = config.local();
        Set<NodeInfo> peers = Set.copyOf(knownPeers.values());
        HandshakePayload payload = new HandshakePayload(localInfo, peers);
        ClusterMessage message = ClusterMessage.request(MessageType.HANDSHAKE,
                "hello",
                localInfo.nodeId(),
                connection.remoteId().orElse(null),
                payload);
        connection.send(message);
    }

    private void handleHandshake(Connection connection, ClusterMessage message) {
        HandshakePayload payload = message.payload(HandshakePayload.class);
        NodeInfo remoteInfo = payload.local();
        connection.setRemote(remoteInfo);
        knownPeers.putIfAbsent(remoteInfo.nodeId(), remoteInfo);
        connections.put(remoteInfo.nodeId(), connection);
        listeners.forEach(listener -> listener.onPeerConnected(remoteInfo));
        // Merge peers and attempt connections
        payload.peers().forEach(peer -> {
            if (peer.nodeId().equals(config.local().nodeId())) {
                return;
            }
            boolean added = knownPeers.putIfAbsent(peer.nodeId(), peer) == null;
            if (added && !isConnected(peer.nodeId())) {
                scheduler.schedule(() -> ensureConnectionAsync(peer), 100, TimeUnit.MILLISECONDS);
            }
        });
        broadcastPeerList();
        // respond with our handshake if inbound connection
        if (!message.source().equals(config.local().nodeId())) {
            sendHandshake(connection);
        }
    }

    private void broadcastPeerList() {
        PeerUpdatePayload payload = new PeerUpdatePayload(Set.copyOf(knownPeers.values()));
        ClusterMessage update = ClusterMessage.request(MessageType.PEER_UPDATE,
                "peer-update",
                config.local().nodeId(),
                null,
                payload);
        broadcast(update);
    }

    private void handlePeerUpdate(ClusterMessage message) {
        PeerUpdatePayload payload = message.payload(PeerUpdatePayload.class);
        for (NodeInfo peer : payload.peers()) {
            if (!peer.nodeId().equals(config.local().nodeId())) {
                boolean added = knownPeers.putIfAbsent(peer.nodeId(), peer) == null;
                if (added && !isConnected(peer.nodeId())) {
                    scheduler.schedule(() -> ensureConnectionAsync(peer), 100, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void handleMessage(ClusterMessage message) {
        if (message.type() == MessageType.HANDSHAKE) {
            return;
        }
        if (message.type() == MessageType.PEER_UPDATE) {
            handlePeerUpdate(message);
            return;
        }
        Optional<UUID> maybeCorrelation = message.correlationId();
        if (maybeCorrelation.isPresent()) {
            PendingResponse response = pendingResponses.remove(maybeCorrelation.get());
            if (response != null) {
                response.cancelTimeout();
                response.future.complete(message);
                return;
            }
        }
        listeners.forEach(listener -> listener.onMessage(message));
    }

    private void handleDisconnect(Connection connection) {
        connection.remoteId().ifPresent(nodeId -> {
            connections.remove(nodeId, connection);
            List<Map.Entry<UUID, PendingResponse>> toFail = new ArrayList<>();
            for (Map.Entry<UUID, PendingResponse> entry : pendingResponses.entrySet()) {
                if (nodeId.equals(entry.getValue().destination)) {
                    toFail.add(entry);
                }
            }
            for (Map.Entry<UUID, PendingResponse> entry : toFail) {
                UUID requestId = entry.getKey();
                PendingResponse pending = entry.getValue();
                if (pendingResponses.remove(requestId, pending)) {
                    pending.cancelTimeout();
                    pending.future.completeExceptionally(new PeerDisconnectedException(nodeId, requestId));
                }
            }
            listeners.forEach(listener -> listener.onPeerDisconnected(nodeId));
        });
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        scheduler.shutdownNow();
        workerPool.shutdownNow();
        for (Connection connection : connections.values()) {
            connection.close();
        }
        connections.clear();
        pendingResponses.values().forEach(pending -> {
            pending.cancelTimeout();
            pending.future.completeExceptionally(new IOException("Transport closed"));
        });
        pendingResponses.clear();
    }

    private final class Connection implements Closeable {
        private final Socket socket;
        private final ObjectOutputStream outputStream;
        private final ObjectInputStream inputStream;
        private final ConcurrentLinkedQueue<ClusterMessage> outbound = new ConcurrentLinkedQueue<>();
        private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ngrid-transport-writer");
            t.setDaemon(true);
            return t;
        });
        private volatile NodeInfo remote;
        private volatile boolean open = true;

        private Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.outputStream = new ObjectOutputStream(socket.getOutputStream());
            this.outputStream.flush();
            this.inputStream = new ObjectInputStream(socket.getInputStream());
            writer.submit(this::drainOutbound);
        }

        void setRemote(NodeInfo remote) {
            this.remote = remote;
        }

        Optional<NodeId> remoteId() {
            return Optional.ofNullable(remote).map(NodeInfo::nodeId);
        }

        boolean isOpen() {
            return open && !socket.isClosed();
        }

        void send(ClusterMessage message) {
            if (!isOpen()) {
                return;
            }
            outbound.offer(message);
        }

        private void drainOutbound() {
            try {
                while (isOpen()) {
                    ClusterMessage message = outbound.poll();
                    if (message == null) {
                        TimeUnit.MILLISECONDS.sleep(5);
                        continue;
                    }
                    synchronized (outputStream) {
                        outputStream.writeObject(message);
                        outputStream.flush();
                    }
                }
            } catch (Exception e) {
                if (open) {
                    LOGGER.log(Level.FINE, "Writer terminating for {0}: {1}", new Object[]{remote, e.getMessage()});
                }
                closeQuietly();
            }
        }

        void readLoop() {
            try {
                while (isOpen()) {
                    ClusterMessage message = (ClusterMessage) inputStream.readObject();
                    if (message.type() == MessageType.HANDSHAKE) {
                        handleHandshake(this, message);
                    } else {
                        handleMessage(message);
                    }
                }
            } catch (Exception e) {
                if (open) {
                    LOGGER.log(Level.FINE, "Connection closed {0} at {1}: {2}", new Object[]{remote, Instant.now(), e.getMessage()});
                }
            } finally {
                closeQuietly();
                handleDisconnect(this);
            }
        }

        @Override
        public void close() throws IOException {
            open = false;
            writer.shutdownNow();
            socket.close();
        }

        private void closeQuietly() {
            open = false;
            writer.shutdownNow();
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class PendingResponse {
        private final NodeId destination;
        private final CompletableFuture<ClusterMessage> future;
        private volatile ScheduledFuture<?> timeoutTask;

        private PendingResponse(NodeId destination, CompletableFuture<ClusterMessage> future) {
            this.destination = destination;
            this.future = future;
        }

        private void cancelTimeout() {
            ScheduledFuture<?> task;
            synchronized (this) {
                task = timeoutTask;
                timeoutTask = null;
            }
            if (task != null) {
                task.cancel(false);
            }
        }

        private void setTimeoutTask(ScheduledFuture<?> task) {
            if (task == null) {
                return;
            }
            boolean cancelNow = false;
            synchronized (this) {
                if (future.isDone()) {
                    cancelNow = true;
                } else {
                    timeoutTask = task;
                }
            }
            if (cancelNow) {
                task.cancel(false);
            }
        }

        private void clearTimeoutTask() {
            synchronized (this) {
                timeoutTask = null;
            }
        }
    }
}
