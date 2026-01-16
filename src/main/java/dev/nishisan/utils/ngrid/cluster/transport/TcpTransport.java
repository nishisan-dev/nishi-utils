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
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.EOFException;
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
import java.util.HashMap;
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
import java.util.concurrent.ThreadFactory;
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
 * <p>
 * This implementation uses Virtual Threads (Java 21+) to handle concurrency efficiently.
 */
public final class TcpTransport implements Transport {
    private static final Logger LOGGER = Logger.getLogger(TcpTransport.class.getName());

    private final TcpTransportConfig config;
    private final StatsUtils stats;
    private final Map<NodeId, NodeInfo> knownPeers = new ConcurrentHashMap<>();
    private final Map<NodeId, Connection> connections = new ConcurrentHashMap<>();
    private final Set<TransportListener> listeners = new CopyOnWriteArraySet<>();
    private final Map<UUID, PendingResponse> pendingResponses = new ConcurrentHashMap<>();
    // Use Virtual Threads for per-task execution
    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
    private final NetworkRouter router = new NetworkRouter(this::collectLatencies);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ngrid-transport-scheduler");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private ServerSocket serverSocket;

    public TcpTransport(TcpTransportConfig config) {
        this(config, null);
    }

    public TcpTransport(TcpTransportConfig config, StatsUtils stats) {
        this.config = Objects.requireNonNull(config, "config");
        this.stats = stats;
        knownPeers.put(config.local().nodeId(), config.local());
        config.initialPeers().forEach(p -> knownPeers.putIfAbsent(p.nodeId(), p));
    }

    // For testing purposes
    NetworkRouter getRouter() {
        return router;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(config.local().host(), config.local().port()), 100);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to bind TCP transport", e);
        }
        running = true;
        
        // Use a Virtual Thread for the accept loop
        Thread.ofVirtual().name("ngrid-transport-accept").start(this::acceptLoop);

        scheduler.scheduleAtFixedRate(this::reconnectLoop,
                config.reconnectInterval().toMillis(),
                config.reconnectInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        // Opportunistic probe for promoted routes (every 3s)
        scheduler.scheduleAtFixedRate(this::probeLoop, 3_000, 3_000, TimeUnit.MILLISECONDS);
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
        send(message, null);
    }

    private void send(ClusterMessage message, NodeId exclude) {
        NodeId destination = message.destination();
        if (destination == null) {
            return;
        }
        
        // Priority 1: If we have an active, open connection to this node, use it!
        Connection activeConn = connections.get(destination);
        if (activeConn != null && activeConn.isOpen()) {
            activeConn.send(message);
            return;
        } else if (activeConn != null) {
            LOGGER.fine(() -> "Connection found for " + destination + " but it is CLOSED");
        } else {
            LOGGER.fine(() -> "No direct connection found in map for " + destination + ". Map keys: " + connections.keySet());
        }

        Optional<NodeId> nextHop = router.nextHop(destination, exclude);
        if (nextHop.isEmpty()) {
            LOGGER.log(Level.WARNING, "No route available for {0} (excluding {1})", new Object[]{destination, exclude});
            return;
        }
        
        NodeId target = nextHop.get();
        Connection connection = ensureConnection(target);
        
        if (connection != null) {
            // Check if we are proxying (target != destination)
            // If so, we might want to decrement TTL here, but it's cleaner to do it on the receiving end before forwarding.
            connection.send(message);
        } else {
            if (target.equals(destination)) {
                // Direct connection failed. Mark failure and try fallback immediately.
                LOGGER.warning("Direct connection failed for " + destination + ". Searching for proxy.");
                router.markDirectFailure(destination);
                
                // Retry with new route
                Optional<NodeId> fallback = router.nextHop(destination, exclude);
                if (fallback.isPresent() && !fallback.get().equals(destination)) {
                    Connection proxyConn = ensureConnection(fallback.get());
                    if (proxyConn != null) {
                        LOGGER.info("Failing over to proxy " + fallback.get() + " for destination " + destination);
                        proxyConn.send(message);
                        return;
                    }
                }
            }
            LOGGER.log(Level.WARNING, "No connection available for {0} (via {1}, excluding {2})", new Object[]{destination, target, exclude});
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

        // Routing Logic
        Optional<NodeId> nextHop = router.nextHop(destination, null);
        if (nextHop.isEmpty()) {
            pendingResponses.remove(requestId, response);
            future.completeExceptionally(new IOException("No route available for " + destination));
            return future;
        }

        NodeId target = nextHop.get();
        Connection connection = ensureConnection(target);
        
        if (connection != null) {
            connection.send(message);
        } else {
            if (target.equals(destination)) {
                // Direct failed, try immediate fallback
                router.markDirectFailure(destination);
                Optional<NodeId> fallback = router.nextHop(destination, null);
                if (fallback.isPresent() && !fallback.get().equals(destination)) {
                    Connection proxyConn = ensureConnection(fallback.get());
                    if (proxyConn != null) {
                        proxyConn.send(message);
                    } else {
                        pendingResponses.remove(requestId, response);
                        future.completeExceptionally(new IOException("No connection available for " + destination + " (via " + fallback.get() + ")"));
                        return future;
                    }
                } else {
                    pendingResponses.remove(requestId, response);
                    future.completeExceptionally(new IOException("No connection available for " + destination));
                    return future;
                }
            } else {
                pendingResponses.remove(requestId, response);
                future.completeExceptionally(new IOException("No connection available for " + destination + " (via " + target + ")"));
                return future;
            }
        }

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

    @Override
    public boolean isReachable(NodeId nodeId) {
        if (isConnected(nodeId)) {
            return true;
        }
        return router.nextHop(nodeId).isPresent();
    }

    @Override
    public void addPeer(NodeInfo peer) {
        if (peer == null || peer.nodeId().equals(config.local().nodeId())) {
            return;
        }
        if (peer.host().equals(config.local().host()) && peer.port() == config.local().port()) {
            return;
        }
        for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
            NodeInfo existing = entry.getValue();
            if (!entry.getKey().equals(peer.nodeId())
                    && existing.host().equals(peer.host())
                    && existing.port() == peer.port()) {
                knownPeers.remove(entry.getKey());
            }
        }
        boolean added = knownPeers.putIfAbsent(peer.nodeId(), peer) == null;
        if (added && running && shouldInitiate(peer)) {
            scheduler.schedule(() -> ensureConnectionAsync(peer), 0, TimeUnit.MILLISECONDS);
        }
        if (added && running) {
            broadcastPeerList();
        }
    }

    private void acceptLoop() {
        LOGGER.info("Transport accept loop started on port " + config.local().port());
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                LOGGER.fine(() -> "Accepted connection from " + socket.getRemoteSocketAddress());
                registerConnection(socket, null);
            } catch (SocketException se) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Server socket closed unexpectedly", se);
                }
                break;
            } catch (EOFException e) {
                if (running) {
                    LOGGER.log(Level.INFO, "Connection closed immediately during handshake (EOF): {0}", e.getMessage());
                }
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
        List<NodeInfo> peers = new ArrayList<>(knownPeers.values());
        Collections.shuffle(peers); // Randomize order to reduce contention
        for (NodeInfo peer : peers) {
            if (peer.nodeId().equals(config.local().nodeId()) || peer.port() <= 0) {
                continue;
            }
            if (!shouldInitiate(peer)) {
                continue;
            }
            ensureConnectionAsync(peer);
        }
    }

    private void ensureConnectionAsync(NodeInfo peer) {
        workerPool.submit(() -> {
            try {
                ensureConnection(peer.nodeId());
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Unexpected error in connection task for " + peer, t);
            }
        });
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
                LOGGER.fine(() -> "Initiating connection to " + nodeInfo);
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(nodeInfo.host(), nodeInfo.port()),
                        (int) config.connectTimeout().toMillis());
                Connection connection = registerConnection(socket, nodeInfo);
                sendHandshake(connection);
                LOGGER.fine(() -> "Connected to " + nodeInfo);
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
        Connection connection = new Connection(socket, preResolved != null);
        if (preResolved != null) {
            connection.setRemote(preResolved);
            connections.put(preResolved.nodeId(), connection);
        }
        // Use Virtual Thread for reading
        Thread.ofVirtual().name("ngrid-transport-reader").start(connection::readLoop);
        LOGGER.fine(() -> "Registered connection: " + socket.getRemoteSocketAddress());
        return connection;
    }

    private void probeLoop() {
        if (!running) {
            return;
        }
        for (Map.Entry<NodeId, NetworkRouter.Route> entry : router.routesSnapshot().entrySet()) {
            if (entry.getValue().type() == NetworkRouter.RouteType.PROXY) {
                NodeId target = entry.getKey();
                workerPool.submit(() -> tryPromoteRoute(target));
            }
        }
    }

    private void tryPromoteRoute(NodeId target) {
        NodeInfo info = knownPeers.get(target);
        if (info == null) {
            return;
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(info.host(), info.port()), 2000);
            // If connected successfully, promote route.
            router.promoteToDirect(target);
            LOGGER.info("Route promoted to DIRECT for " + target);
        } catch (IOException e) {
            // Still unreachable directly, stay on proxy.
        }
    }

    private void sendHandshake(Connection connection) {
        NodeInfo localInfo = config.local();
        Set<NodeInfo> peers = Set.copyOf(knownPeers.values());
        HandshakePayload payload = new HandshakePayload(localInfo, peers, collectLatencies());
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
        boolean firstHandshakeOnThisConnection = connection.remoteId().isEmpty();
        connection.setRemote(remoteInfo);
        List<NodeId> staleIds = new ArrayList<>();
        for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
            NodeInfo existing = entry.getValue();
            if (!entry.getKey().equals(remoteInfo.nodeId())
                    && existing.host().equals(remoteInfo.host())
                    && existing.port() == remoteInfo.port()) {
                staleIds.add(entry.getKey());
            }
        }
        for (NodeId staleId : staleIds) {
            knownPeers.remove(staleId);
            Connection staleConn = connections.remove(staleId);
            if (staleConn != null) {
                staleConn.closeQuietly();
            }
        }
        knownPeers.put(remoteInfo.nodeId(), remoteInfo);
        
        Connection previous = connections.put(remoteInfo.nodeId(), connection);
        if (previous != null && previous != connection) {
            boolean preferOutbound = shouldInitiate(remoteInfo);
            boolean previousOutbound = previous.outboundInitiated;
            boolean connectionOutbound = connection.outboundInitiated;
            if (preferOutbound && !connectionOutbound && previousOutbound) {
                connections.put(remoteInfo.nodeId(), previous);
                LOGGER.info("Keeping existing outbound connection for " + remoteInfo.nodeId() + " and closing new inbound connection");
                connection.closeQuietly();
                return;
            }
            if (!preferOutbound && connectionOutbound && !previousOutbound) {
                connections.put(remoteInfo.nodeId(), previous);
                LOGGER.info("Keeping existing inbound connection for " + remoteInfo.nodeId() + " and closing new outbound connection");
                connection.closeQuietly();
                return;
            }
            LOGGER.info("Replaced existing connection for " + remoteInfo.nodeId() + " with new connection");
            previous.closeQuietly();
        } else {
            LOGGER.info("Registered new connection for " + remoteInfo.nodeId());
        }
        
        // Feed router with reachability info
        router.updateReachability(remoteInfo.nodeId(), payload.peers(), payload.latencies());

        listeners.forEach(listener -> listener.onPeerConnected(remoteInfo));
        // Merge peers and attempt connections
        payload.peers().forEach(peer -> {
            if (peer.nodeId().equals(config.local().nodeId())) {
                return;
            }
            if (peer.host().equals(config.local().host()) && peer.port() == config.local().port()) {
                return;
            }
            for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
                NodeInfo existing = entry.getValue();
                if (!entry.getKey().equals(peer.nodeId())
                        && existing.host().equals(peer.host())
                        && existing.port() == peer.port()) {
                    knownPeers.remove(entry.getKey());
                }
            }
            boolean added = knownPeers.putIfAbsent(peer.nodeId(), peer) == null;
            
            // Only attempt reverse connection if the peer has a valid port (not a discovery client)
            if (added && !isConnected(peer.nodeId()) && peer.port() > 0 && shouldInitiate(peer)) {
                scheduler.schedule(() -> ensureConnectionAsync(peer), 100, TimeUnit.MILLISECONDS);
            }
        });
        broadcastPeerList();
        // Respond with our handshake only once when this was an inbound connection.
        if (firstHandshakeOnThisConnection) {
            sendHandshake(connection);
        }
    }

    private void broadcastPeerList() {
        PeerUpdatePayload payload = new PeerUpdatePayload(Set.copyOf(knownPeers.values()), collectLatencies());
        ClusterMessage update = ClusterMessage.request(MessageType.PEER_UPDATE,
                "peer-update",
                config.local().nodeId(),
                null,
                payload);
        broadcast(update);
    }

    private Map<NodeId, Double> collectLatencies() {
        if (stats == null) {
            return Collections.emptyMap();
        }
        Map<NodeId, Double> latencies = new HashMap<>();
        for (NodeId nodeId : knownPeers.keySet()) {
            Double rtt = stats.getAverageOrNull(dev.nishisan.utils.ngrid.metrics.NGridMetrics.rttMs(nodeId));
            if (rtt != null) {
                latencies.put(nodeId, rtt);
            }
        }
        return latencies;
    }

    private void handlePeerUpdate(ClusterMessage message) {
        PeerUpdatePayload payload = message.payload(PeerUpdatePayload.class);
        
        // Feed router with reachability info
        router.updateReachability(message.source(), payload.peers(), payload.latencies());

        for (NodeInfo peer : payload.peers()) {
            if (!peer.nodeId().equals(config.local().nodeId())) {
                if (peer.host().equals(config.local().host()) && peer.port() == config.local().port()) {
                    continue;
                }
                for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
                    NodeInfo existing = entry.getValue();
                    if (!entry.getKey().equals(peer.nodeId())
                            && existing.host().equals(peer.host())
                            && existing.port() == peer.port()) {
                        knownPeers.remove(entry.getKey());
                    }
                }
                boolean added = knownPeers.compute(peer.nodeId(), (id, existing) -> {
                    if (existing == null || !existing.equals(peer)) {
                        return peer;
                    }
                    return existing;
                }) == peer;
                if (added && !isConnected(peer.nodeId()) && shouldInitiate(peer)) {
                    scheduler.schedule(() -> ensureConnectionAsync(peer), 100, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private boolean shouldInitiate(NodeInfo peer) {
        return config.local().nodeId().compareTo(peer.nodeId()) < 0;
    }

    private void handleMessage(NodeId senderId, ClusterMessage message) {
        if (message.type() == MessageType.HANDSHAKE) {
            return;
        }
        if (message.type() == MessageType.PEER_UPDATE) {
            handlePeerUpdate(message);
            return;
        }
        
        // Relay logic
        NodeId localId = config.local().nodeId();
        if (message.destination() != null && !message.destination().equals(localId)) {
            if (message.ttl() <= 0) {
                LOGGER.fine(() -> "Dropping message with expired TTL from " + message.source());
                return;
            }
            // Forwarding - exclude the node that just sent us this message to avoid simple loops
            send(message.nextHop(), senderId);
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
        listeners.forEach(listener -> workerPool.submit(() -> listener.onMessage(message)));
    }

    private void handleDisconnect(Connection connection) {
        connection.remoteId().ifPresent(nodeId -> {
            LOGGER.info(() -> "Handling disconnect from " + nodeId + " (remote=" + connection.remote + ", open=" + connection.isOpen() + ")");
            // Only treat the peer as disconnected when the currently tracked connection goes away.
            // In rare races, nodes can end up with multiple TCP connections; a stale connection closing
            // must NOT fail in-flight request/response calls if there is still an active connection.
            boolean removedActive = connections.remove(nodeId, connection);
            Connection current = connections.get(nodeId);
            if (!removedActive && current != null && current.isOpen()) {
                LOGGER.info(() -> "Ignoring disconnect for " + nodeId + " because an active connection remains");
                return;
            }
            if (current != null && current.isOpen()) {
                return;
            }
            LOGGER.info(() -> "Disconnect confirmed for " + nodeId + "; failing pending responses");
            // Best-effort cleanup: if the peer is no longer connected, drop the per-peer lock.
            // It will be recreated if we reconnect.
            connectionLocks.remove(nodeId);
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
        // Accept thread will die when socket closes or running is false
        
        scheduler.shutdownNow();
        workerPool.shutdownNow(); // Virtual threads will be interrupted
        try {
            workerPool.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Connection connection : connections.values()) {
            connection.close();
        }
        connections.clear();
        connectionLocks.clear();
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
        private final boolean outboundInitiated;
        private volatile NodeInfo remote;
        private volatile boolean open = true;

        private Connection(Socket socket, boolean outboundInitiated) throws IOException {
            this.socket = socket;
            this.outboundInitiated = outboundInitiated;
            this.outputStream = new ObjectOutputStream(socket.getOutputStream());
            this.outputStream.flush();
            this.inputStream = new ObjectInputStream(socket.getInputStream());
            // Use Virtual Thread for writing (one per connection to ensure order)
            Thread.ofVirtual().name("ngrid-transport-writer").start(this::drainOutbound);
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
                        try {
                            Thread.sleep(5); // Virtual threads sleep efficiently
                        } catch (InterruptedException e) {
                             Thread.currentThread().interrupt();
                             break;
                        }
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
                        handleMessage(remoteId().orElse(null), message);
                    }
                }
            } catch (Exception e) {
                if (open) {
                    LOGGER.log(Level.INFO, "Connection closed {0} at {1}", new Object[]{remote, Instant.now()});
                    LOGGER.log(Level.INFO, "Connection close exception", e);
                }
            } finally {
                closeQuietly();
                handleDisconnect(this);
            }
        }

        @Override
        public void close() throws IOException {
            if (open) {
                LOGGER.fine(() -> "Closing connection to " + remote);
            }
            open = false;
            // No need to shutdown writer executor anymore, the flag + socket close will kill it
            socket.close();
        }

        private void closeQuietly() {
            if (open) {
                LOGGER.fine(() -> "Quietly closing connection to " + remote);
            }
            open = false;
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
