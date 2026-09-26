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
import dev.nishisan.utils.ngrid.common.LeavePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;
import dev.nishisan.utils.ngrid.common.UndeliverablePayload;
import dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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
    // Ids confirmed first-hand by a direct handshake (the remote's own NodeInfo). Second-hand
    // gossip never displaces them (see mergeGossipedPeer). Always a subset of knownPeers' keys.
    private final Set<NodeId> verifiedPeers = ConcurrentHashMap.newKeySet();
    // Serializes structural changes of knownPeers/verifiedPeers (address-collision cleanup and
    // merges), so a concurrent handshake and gossip merge cannot interleave half-way. In-memory
    // only: never held across I/O.
    private final ReentrantLock peerTableLock = new ReentrantLock();
    // Ids of the configured bootstrap peers; until verified they are not gossiped (gossipablePeers).
    private final Set<NodeId> initialPeerIds;
    // Tombstones of forgotten (departed) peers: id -> expiry (epoch millis). While present, second-hand
    // sources cannot re-admit the id (see forget). Written under peerTableLock; purged lazily.
    private final Map<NodeId, Long> departedPeers = new ConcurrentHashMap<>();
    // Since when (epoch millis) each known ephemeral peer has had no open connection; drives the
    // forget-after-disconnection backstop (forgetLongDisconnectedEphemeralPeers). Scheduler thread only.
    private final Map<NodeId, Long> disconnectedSince = new ConcurrentHashMap<>();
    // Last time (epoch millis) any message sourced by each peer arrived, directly or through a relay.
    // A peer this node cannot dial (partial mesh) may still be alive and talking through a relay; the
    // forget-after-disconnection backstop must not take it for gone.
    private final Map<NodeId, Long> lastInboundFromPeer = new ConcurrentHashMap<>();
    private final Map<NodeId, Connection> connections = new ConcurrentHashMap<>();
    // Includes accepted sockets that have not supplied a handshake/peer identity yet.
    private final Set<Connection> liveSockets = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Set<TransportListener> listeners = new CopyOnWriteArraySet<>();
    private final Map<UUID, PendingResponse> pendingResponses = new ConcurrentHashMap<>();
    // Use Virtual Threads for per-task execution
    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
    // A relay must be a peer we currently hold an open connection to (see NetworkRouter.relayCandidate).
    private final NetworkRouter router = new NetworkRouter(this::collectLatencies, this::isConnected);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ngrid-transport-scheduler");
        t.setDaemon(true);
        return t;
    });

    // Test seam: runs just before an outbound dial. Lets a test deterministically simulate a slow
    // or stuck connect for a given peer (e.g. a node that just died) without relying on real
    // network timeouts. No-op in production.
    private volatile java.util.function.Consumer<NodeId> beforeDialHook = id -> { };
    // Test seam: runs in handleHandshake between identifying the connection and publishing it, so a
    // test can hold a second connection of the same peer in that window. No-op in production.
    private volatile java.util.function.Consumer<NodeId> handshakeIdentityHook = id -> { };

    private volatile boolean running;
    // Set by close() before it announces the departure (LEAVE): from then on no new connection is
    // registered or published, so the set of peers told about the departure is final.
    private volatile boolean leaving;
    private ServerSocket serverSocket;

    public TcpTransport(TcpTransportConfig config) {
        this(config, null);
    }

    public TcpTransport(TcpTransportConfig config, StatsUtils stats) {
        this.config = Objects.requireNonNull(config, "config");
        this.stats = stats;
        knownPeers.put(config.local().nodeId(), config.local());
        config.initialPeers().forEach(p -> knownPeers.putIfAbsent(p.nodeId(), p));
        Set<NodeId> seedIds = new HashSet<>();
        config.initialPeers().forEach(p -> seedIds.add(p.nodeId()));
        this.initialPeerIds = Collections.unmodifiableSet(seedIds);
    }

    // For testing purposes
    NetworkRouter getRouter() {
        return router;
    }

    // Visible for tests in this package: hook invoked right before each outbound dial.
    void setBeforeDialHook(java.util.function.Consumer<NodeId> hook) {
        this.beforeDialHook = Objects.requireNonNull(hook, "hook");
    }

    // Visible for tests in this package: hook invoked once an incoming handshake has set the
    // connection's remote identity, before the connection is published for that peer.
    void setHandshakeIdentityHook(java.util.function.Consumer<NodeId> hook) {
        this.handshakeIdentityHook = Objects.requireNonNull(hook, "hook");
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
        // Opportunistic probe for promoted routes
        long probeMs = config.routeProbeInterval().toMillis();
        scheduler.scheduleAtFixedRate(this::probeLoop, probeMs, probeMs, TimeUnit.MILLISECONDS);
        // Attempt initial outbound connections. Initial peers are bootstrap seeds and must
        // be dialed unconditionally (a seed does not yet know the joining node, so it cannot
        // dial back). The simultaneous-open collision this may cause is reconciled
        // deterministically in registerLiveConnection, so both endpoints converge on a single
        // shared connection instead of flapping.
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
        // Fan out per-peer sends on the worker pool (virtual threads) instead of inline in this
        // loop. send() does a BLOCKING connect (up to connectTimeout, twice when it also tries a
        // proxy fallback) for any peer without a live connection — e.g. a peer that just died.
        // Done inline, that blocks the caller (the heartbeat scheduler thread) for ~10s, delaying
        // the heartbeat to every *live* peer iterated after the dead one. Under a failover that
        // starves the survivors' heartbeats past the eviction window, collapsing quorum. Dispatching
        // each send independently means a dead peer's slow dial never delays a live peer's heartbeat.
        if (!running) {
            return; // best-effort: the transport is stopped/closing
        }
        for (NodeId nodeId : knownPeers.keySet()) {
            if (!nodeId.equals(config.local().nodeId())) {
                ClusterMessage perPeer = message.withDestination(nodeId);
                try {
                    workerPool.submit(() -> send(perPeer));
                } catch (RejectedExecutionException e) {
                    // close() raced this broadcast and already shut the pool down. Broadcast is
                    // fire-and-forget, so drop the remaining sends instead of propagating — otherwise
                    // the exception would bubble to schedulers like LeaderReelectionService.tick()
                    // (no try/catch) and cancel their recurring task.
                    return;
                }
            }
        }
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
        if (isDeparted(destination) && !isConnected(destination)) {
            // A peer forgotten for good (e.g. a late notification to a client that left): nothing to
            // route to, and trying would recreate its routing state and log a connection failure.
            LOGGER.fine(() -> "Dropping " + message.type() + " to departed peer " + destination);
            return;
        }
        
        // Priority 1: If we have an active, open connection to this node, use it!
        // This bypasses routing logic for already connected peers (including discovery clients).
        Connection activeConn = connections.get(destination);
        if (activeConn != null && activeConn.isOpen()) {
            activeConn.send(message);
            return;
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
            } else {
                // The PROXY route is unusable (the relay is gone). Before dropping the message, try
                // the destination DIRECTLY: a route demoted to proxy by one failed dial never got a
                // second chance here, so every message to a peer that had merely blinked kept going
                // to a dead relay — its heartbeats included, which evicted a live member.
                Connection direct = ensureConnection(destination);
                if (direct != null) {
                    router.promoteToDirect(destination);
                    LOGGER.info(() -> "Relay " + target + " unavailable; direct connection to " + destination
                            + " restored");
                    direct.send(message);
                    return;
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
        if (isDeparted(destination) && !isConnected(destination)) {
            future.completeExceptionally(new IOException("Peer " + destination + " departed"));
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
    public boolean isProxied(NodeId nodeId) {
        return router.isProxy(nodeId);
    }

    @Override
    public Map<NodeId, Integer> outboundQueueDepths() {
        Map<NodeId, Integer> depths = new HashMap<>();
        connections.forEach((nodeId, conn) -> depths.put(nodeId, conn.outboundDepth()));
        return depths;
    }

    @Override
    public Map<NodeId, Long> outboundDropped() {
        Map<NodeId, Long> dropped = new HashMap<>();
        connections.forEach((nodeId, conn) -> dropped.put(nodeId, conn.outboundDropped()));
        return dropped;
    }

    @Override
    public void addPeer(NodeInfo peer) {
        // An explicit join is first-hand intent: it lifts a tombstone left by an earlier departure.
        if (peer != null) {
            peerTableLock.lock();
            try {
                departedPeers.remove(peer.nodeId());
            } finally {
                peerTableLock.unlock();
            }
        }
        boolean added = mergeGossipedPeer(peer);
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
        long nowMs = System.currentTimeMillis();
        purgeExpiredTombstones(nowMs);
        forgetLongDisconnectedEphemeralPeers(nowMs);
        // Fast path: skip iteration if all known peers are connected
        boolean allConnected = knownPeers.values().stream()
                .filter(p -> !p.nodeId().equals(config.local().nodeId()) && p.port() > 0)
                .allMatch(p -> isConnected(p.nodeId()));
        if (allConnected) {
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
        // Skip non-listening placeholders (discovery clients / gossip entries carry port 0):
        // dialing them would just fail and add noise to the scheduler/probe loops.
        if (nodeInfo.port() <= 0) {
            return null;
        }
        ReentrantLock peerLock = getLockFor(nodeId);
        peerLock.lock();
        try {
            current = connections.get(nodeId);
            if (current != null && current.isOpen()) {
                return current;
            }
            if (!knownPeers.containsKey(nodeId)) {
                return null; // forgotten while this caller waited for the peer lock
            }
            Connection unpublished = unpublishedLinkTo(nodeInfo);
            if (unpublished != null) {
                return unpublished;
            }
            try {
                beforeDialHook.accept(nodeId);
                LOGGER.fine(() -> "Initiating connection to " + nodeInfo);
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(nodeInfo.host(), nodeInfo.port()),
                        (int) config.connectTimeout().toMillis());
                Connection connection = registerConnection(socket, nodeInfo);
                // Register through the single reconciliation point. If a connection to this
                // peer already won (e.g. an inbound one that arrived concurrently), we adopt
                // it and let our just-opened socket be closed, so both endpoints converge on
                // the same physical link instead of clobbering each other.
                Connection live = registerLiveConnection(nodeId, connection);
                if (live == connection) {
                    sendHandshake(connection);
                }
                LOGGER.fine(() -> "Connected to " + nodeInfo);
                return live;
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "Unable to connect to {0}: {1}", new Object[]{nodeInfo, e.getMessage()});
                return null;
            }
        } finally {
            peerLock.unlock();
        }
    }

    // One lock per peer id, serializing dials to it and publication of its live connection. An
    // entry must outlive disconnects: dropping it while a dial still holds the old lock let the
    // next caller create a fresh one and dial the same peer concurrently. Entries are removed only
    // on close() and when the peer is forgotten for good (forget / dropConnectionLock), the one
    // place allowed to drop a single peer's entry.
    private final Map<NodeId, ReentrantLock> connectionLocks = new ConcurrentHashMap<>();

    /**
     * An open socket that already leads to {@code target}'s process but is not published under its
     * id: the link to a bootstrap seed whose handshake reply has not resolved the seed's provisional
     * alias yet (same listen address), or a link to that very id still in the middle of its own
     * handshake. Messages sent over it reach the right process. Dialing again instead opened a
     * duplicate connection that each endpoint registered in a different order (the dialer re-keys
     * the seed link only when the reply arrives), so the tie-break in registerLiveConnection kept a
     * different socket on each side and the link dropped.
     */
    private Connection unpublishedLinkTo(NodeInfo target) {
        for (Connection candidate : liveSockets) {
            NodeInfo remote = candidate.remote;
            if (remote == null || !candidate.isOpen()) {
                continue;
            }
            if (remote.nodeId().equals(target.nodeId())) {
                return candidate;
            }
            if (sameAddress(remote, target) && initialPeerIds.contains(remote.nodeId())
                    && !verifiedPeers.contains(remote.nodeId())) {
                return candidate;
            }
        }
        return null;
    }

    private ReentrantLock getLockFor(NodeId nodeId) {
        return connectionLocks.computeIfAbsent(nodeId, id -> new ReentrantLock());
    }

    // Visible for tests in this package: whether close() would announce LEAVE to this peer (its tracked
    // connection is open and the peer's handshake on it announced support).
    boolean announcesLeaveTo(NodeId nodeId) {
        Connection connection = connections.get(nodeId);
        return connection != null && connection.isOpen() && connection.handshaked() && connection.peerSupportsLeave();
    }

    // Visible for tests in this package: the per-peer lock serializing dials/publication.
    ReentrantLock connectionLockFor(NodeId nodeId) {
        return getLockFor(nodeId);
    }

    private Connection registerConnection(Socket socket, NodeInfo preResolved) throws IOException {
        Connection connection;
        lifecycleLock.lock();
        try {
            if (!running || leaving) { throw new IOException("Transport closed during connect"); }
            socket.setTcpNoDelay(true);
            connection = new Connection(socket, preResolved != null);
            liveSockets.add(connection);
            if (preResolved != null) {
                connection.setRemote(preResolved);
            }
            // Peer publication still goes through registerLiveConnection. Track the socket
            // immediately so shutdown also closes a reader waiting for its first handshake.
        } catch (IOException e) {
            try { socket.close(); } catch (IOException suppressed) { e.addSuppressed(suppressed); }
            throw e;
        } finally {
            lifecycleLock.unlock();
        }
        // Use Virtual Thread for reading
        Thread.ofVirtual().name("ngrid-transport-reader").start(connection::readLoop);
        LOGGER.fine(() -> "Registered connection: " + socket.getRemoteSocketAddress());
        return connection;
    }

    /**
     * Publishes {@code candidate} as the live connection for {@code remoteId}, reconciling
     * concurrent (simultaneous-open) connections deterministically: when two live sockets
     * exist for the same peer, the one initiated by the lower {@link NodeId} wins. Both
     * endpoints compute the same winner, so they converge on a single physical connection.
     *
     * @return the connection that is now live for the peer (may be a pre-existing one if the
     *         candidate lost the tie-break; the candidate is closed in that case), or null
     *         when shutdown or a closed socket prevents publication
     */
    private Connection registerLiveConnection(NodeId remoteId, Connection candidate) {
        ReentrantLock peerLock = getLockFor(remoteId);
        peerLock.lock();
        lifecycleLock.lock();
        try {
            if (!running || !candidate.isOpen()) {
                candidate.closeQuietly();
                return null;
            }
            if (isDeparted(remoteId)) {
                // A forgotten peer is re-admitted only by its own handshake, which lifts the tombstone
                // before publishing. A dial that raced the forget, or an inbound socket identified
                // just by the source of its first message, must not bring it back.
                LOGGER.fine(() -> "Not publishing a connection for departed peer " + remoteId);
                candidate.closeQuietly();
                return null;
            }
            Connection existing = connections.get(remoteId);
            if (existing == candidate) {
                return candidate;
            }
            if (leaving) {
                // Closing: the peers to announce the departure to were already chosen.
                candidate.closeQuietly();
                return null;
            }
            if (existing == null || !existing.isOpen()) {
                connections.put(remoteId, candidate);
                return candidate;
            }
            // Simultaneous open: keep the connection initiated by the lower NodeId.
            boolean keepCandidate =
                    (config.local().nodeId().compareTo(remoteId) < 0) == candidate.outboundInitiated;
            if (keepCandidate) {
                connections.put(remoteId, candidate);
                existing.closeQuietly();
                return candidate;
            }
            candidate.closeQuietly();
            return existing;
        } finally {
            lifecycleLock.unlock();
            peerLock.unlock();
        }
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
        if (info == null || info.port() <= 0) {
            // Non-listening placeholder (port 0): never promotable to a direct link, and
            // probing it would loop forever calling ensureConnection() that always fails.
            return;
        }
        // Re-establish a REAL handshaked connection (not a throwaway probe socket) before
        // promoting: a bare socket test would flip the route to DIRECT while no usable
        // connection exists, so the very next send collapses it back to PROXY (flapping).
        // Any simultaneous-open caused by both ends probing is reconciled deterministically
        // in registerLiveConnection, so this is safe regardless of NodeId ordering.
        Connection connection = ensureConnection(target);
        if (connection != null && connection.isOpen()) {
            router.promoteToDirect(target);
            LOGGER.fine(() -> "Route promoted to DIRECT for " + target);
        }
    }

    /**
     * Peers this node vouches for in its handshake and PEER_UPDATE gossip: every known peer except
     * bootstrap entries from the configuration ({@link TcpTransportConfig#initialPeers()}) not yet
     * confirmed by a direct handshake. Such an entry is usually a provisional seed alias
     * ({@code host:port}, {@code seed-host:port}) whose real id is still unknown; gossiping it made
     * receivers replace the canonical id of that same process with the alias (issue #169). Once
     * the seed answers, the alias is replaced by its verified canonical id, which is gossiped.
     */
    private Set<NodeInfo> gossipablePeers() {
        Set<NodeInfo> peers = new HashSet<>();
        for (NodeInfo peer : knownPeers.values()) {
            NodeId id = peer.nodeId();
            if (initialPeerIds.contains(id) && !verifiedPeers.contains(id)) {
                continue;
            }
            peers.add(peer);
        }
        return peers;
    }

    private void sendHandshake(Connection connection) {
        NodeInfo localInfo = config.local();
        Set<NodeInfo> peers = gossipablePeers();
        HandshakePayload payload = new HandshakePayload(localInfo, peers, collectLatencies(),
                config.compressionEnabled(), true, true);
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
        // Negotiate outbound compression for this connection based on the peer's advertised
        // capability. Done before any early return so the connection that ends up winning a
        // simultaneous-open tie-break already has the correct flag.
        connection.setPeerSupportsCompression(payload.supportsCompression());
        connection.setPeerSupportsUndeliverable(payload.supportsUndeliverable());
        connection.setPeerSupportsLeave(payload.supportsLeave());
        connection.markHandshaked();
        handshakeIdentityHook.accept(remoteInfo.nodeId());
        // A direct handshake is first-hand and authoritative: whatever other id we held for the
        // remote's listen address (typically the provisional "host:port" seed alias) is replaced
        // by the canonical one, which becomes verified.
        List<Connection> staleConnections = new ArrayList<>();
        peerTableLock.lock();
        try {
            List<NodeId> staleIds = new ArrayList<>();
            for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
                if (!entry.getKey().equals(remoteInfo.nodeId()) && sameAddress(entry.getValue(), remoteInfo)) {
                    staleIds.add(entry.getKey());
                }
            }
            for (NodeId staleId : staleIds) {
                knownPeers.remove(staleId);
                verifiedPeers.remove(staleId);
                Connection staleConn = connections.remove(staleId);
                // An outbound seed connection is initially indexed by host:port. Learning
                // its canonical ID moves that same socket; it must not close itself here.
                if (staleConn != null && staleConn != connection) {
                    staleConnections.add(staleConn);
                }
            }
            // A direct handshake from a departed id is a new incarnation: first-hand, it lifts the
            // tombstone whatever its origin (own LEAVE, disconnect timeout or gossip).
            if (departedPeers.remove(remoteInfo.nodeId()) != null) {
                LOGGER.info(() -> "Departed peer " + remoteInfo.nodeId() + " is back on "
                        + config.local().nodeId() + " (direct handshake); tombstone cleared");
            }
            knownPeers.put(remoteInfo.nodeId(), remoteInfo);
            verifiedPeers.add(remoteInfo.nodeId());
        } finally {
            peerTableLock.unlock();
        }
        staleConnections.forEach(Connection::closeQuietly);

        // Publish this connection through the single reconciliation point. If a concurrent
        // (simultaneous-open) connection already won the deterministic tie-break, we lost:
        // record reachability and bail out without responding — the winning connection
        // already drives this peer.
        NodeId remoteNodeId = remoteInfo.nodeId();
        Connection live = registerLiveConnection(remoteNodeId, connection);
        if (live == null) { return; }
        if (live != connection) {
            router.updateReachability(remoteNodeId, admissible(payload.peers()), admissible(payload.latencies()));
            return;
        }

        // A direct connection now exists: self-heal any stale PROXY route to this peer.
        router.promoteToDirect(remoteNodeId);

        // Feed router with reachability info
        router.updateReachability(remoteInfo.nodeId(), admissible(payload.peers()), admissible(payload.latencies()));

        listeners.forEach(listener -> listener.onPeerConnected(remoteInfo));
        // Merge peers and attempt connections
        payload.peers().forEach(peer -> {
            if (mergeGossipedPeer(peer)) {
                scheduleDialIfInitiator(peer);
            }
        });
        broadcastPeerList();
        // Respond with our handshake only once when this was an inbound connection.
        if (firstHandshakeOnThisConnection) {
            sendHandshake(connection);
        }
    }

    private void broadcastPeerList() {
        PeerUpdatePayload payload = new PeerUpdatePayload(gossipablePeers(), collectLatencies(), departedSnapshot());
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
        // Departures first, so the same update cannot re-admit what it reports as gone.
        payload.departed().forEach((id, remainingMs) -> learnDepartureSecondHand(id, remainingMs, message.source()));
        
        // Feed router with reachability info
        router.updateReachability(message.source(), admissible(payload.peers()), admissible(payload.latencies()));

        for (NodeInfo peer : payload.peers()) {
            if (mergeGossipedPeer(peer)) {
                scheduleDialIfInitiator(peer);
            }
        }
    }

    /**
     * Single admission point for peers learned second-hand: explicit {@link #addPeer} joins, the
     * peer list carried by a handshake and PEER_UPDATE gossip. Every indirect source goes through
     * here, so admission filters (e.g. ids that announced a graceful departure) plug in once.
     * <p>
     * Rules:
     * <ul>
     *   <li>the local node (by id or by listen address) is never merged;</li>
     *   <li>an entry carrying a different id at the listen address of a peer <b>verified</b> by a
     *       direct handshake is dropped. It is a provisional seed alias ({@code host:port}) of that
     *       same process, gossiped by a node that has not resolved it yet. Letting it replace the
     *       canonical id made the next send dial the same process again under the alias key; the
     *       remote then closed the original link in the duplicate-connection tie-break, and every
     *       request in flight on it failed with {@link PeerDisconnectedException} (issue #169);</li>
     *   <li>otherwise a different id at the same address replaces the unverified entry (a seed
     *       alias learning its canonical id second-hand, or a restarted process with a new id);</li>
     *   <li>a verified peer's own entry is not overwritten by gossip while a direct connection to it
     *       is open: its handshake is first-hand and fresher;</li>
     *   <li>a departed (tombstoned) id is rejected: gossip still listing a peer that left must not
     *       bring it back (see {@link #forget(NodeId)}).</li>
     * </ul>
     *
     * @return {@code true} when the entry was added or changed
     */
    private boolean mergeGossipedPeer(NodeInfo peer) {
        if (peer == null || peer.nodeId().equals(config.local().nodeId()) || sameAddress(peer, config.local())) {
            return false;
        }
        peerTableLock.lock();
        try {
            if (isDeparted(peer.nodeId())) {
                LOGGER.fine(() -> "Ignoring gossiped " + peer + ": departed (tombstoned) on " + config.local().nodeId());
                return false;
            }
            List<NodeId> displaced = new ArrayList<>();
            for (Map.Entry<NodeId, NodeInfo> entry : knownPeers.entrySet()) {
                if (entry.getKey().equals(peer.nodeId()) || !sameAddress(entry.getValue(), peer)) {
                    continue;
                }
                if (verifiedPeers.contains(entry.getKey())) {
                    LOGGER.fine(() -> "Ignoring gossiped " + peer + ": " + entry.getKey()
                            + " was verified at that address by a direct handshake");
                    return false;
                }
                displaced.add(entry.getKey());
            }
            displaced.forEach(knownPeers::remove);
            NodeInfo existing = knownPeers.get(peer.nodeId());
            if (peer.equals(existing)) {
                return false;
            }
            if (existing != null && verifiedPeers.contains(peer.nodeId()) && isConnected(peer.nodeId())) {
                return false;
            }
            knownPeers.put(peer.nodeId(), peer);
            return true;
        } finally {
            peerTableLock.unlock();
        }
    }

    /** Remaining TTL (ms) of each live tombstone, for the {@code departed} field of PEER_UPDATE. */
    private Map<NodeId, Long> departedSnapshot() {
        long nowMs = System.currentTimeMillis();
        Map<NodeId, Long> snapshot = new HashMap<>();
        departedPeers.forEach((id, expiresAt) -> {
            if (expiresAt > nowMs) {
                snapshot.put(id, expiresAt - nowMs);
            }
        });
        return snapshot;
    }

    /**
     * A peer reported {@code id} as departed. Second-hand, hence admission-only: the tombstone is
     * recorded (capped by this node's own TTL, never extended by re-gossip) and a peer this node merely
     * knows is forgotten — so a node that never reached the leaver cleans up too — but a peer this node
     * holds a handshaked open connection to is left alone (the report may predate its reconnection:
     * first-hand evidence wins), and a leader-eligible peer is never forgotten nor blocked this way.
     */
    private void learnDepartureSecondHand(NodeId id, Long remainingMs, NodeId reporter) {
        if (id == null || remainingMs == null || remainingMs <= 0 || id.equals(config.local().nodeId())) {
            return;
        }
        long ttlMs = Math.min(remainingMs, config.departedPeerTombstoneTtl().toMillis());
        forget(id, ttlMs, "departure reported by " + reporter, true);
    }

    /** Whether an open socket already identified {@code id} through a handshake (published or not). */
    private boolean hasHandshakedOpenConnection(NodeId id) {
        Connection published = connections.get(id);
        if (published != null && published.isOpen() && published.handshaked()) {
            return true;
        }
        for (Connection candidate : liveSockets) {
            if (candidate.isOpen() && candidate.handshaked() && id.equals(candidate.remoteId().orElse(null))) {
                return true;
            }
        }
        return false;
    }

    /** Dials a newly learned peer when this node is the designated initiator for the pair. */
    private void scheduleDialIfInitiator(NodeInfo peer) {
        // Only listening peers are dialed (port 0 marks a discovery client).
        if (running && peer.port() > 0 && !isConnected(peer.nodeId()) && shouldInitiate(peer)) {
            scheduler.schedule(() -> ensureConnectionAsync(peer), 100, TimeUnit.MILLISECONDS);
        }
    }

    private static boolean sameAddress(NodeInfo a, NodeInfo b) {
        return a.port() == b.port() && a.host().equals(b.host());
    }

    private boolean shouldInitiate(NodeInfo peer) {
        return config.local().nodeId().compareTo(peer.nodeId()) < 0;
    }

    private void handleMessage(NodeId senderId, ClusterMessage message) {
        if (message.type() == MessageType.HANDSHAKE) {
            return;
        }
        NodeId source = message.source();
        if (source != null) {
            // Liveness evidence for the slow forget trigger, relayed traffic included (recorded before
            // the tombstone filter below: it must reflect what arrives, not what is accepted).
            lastInboundFromPeer.put(source, System.currentTimeMillis());
        }
        if (source != null && !source.equals(senderId) && isDeparted(source)) {
            // Relayed (second-hand) traffic of a forgotten peer — typically in flight when it left. Its
            // new incarnation is re-admitted by its own direct handshake, not through a relay.
            LOGGER.fine(() -> "Dropping " + message.type() + " relayed by " + senderId + " from departed peer " + source);
            return;
        }
        if (message.type() == MessageType.UNDELIVERABLE && config.local().nodeId().equals(message.destination())) {
            handleUndeliverable(message);
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
            // Forwarding: ONE hop, over an OPEN direct connection to the destination only. A relay must
            // never dial the destination on behalf of the sender nor re-proxy through a third node:
            // when the destination is dead (a killed leader still targeted by every follower's fetches,
            // heartbeats and client requests) each relayed message turned into a TTL-bounded storm of
            // failed dials and re-forwards across the survivors, executed INLINE on the read loop of the
            // connection it arrived on — delaying the sender's own heartbeats past the eviction window,
            // so two live survivors evicted each other and lost the quorum in the middle of a failover.
            Connection direct = connections.get(message.destination());
            if (direct != null && direct.isOpen()) {
                direct.send(message.nextHop());
            } else {
                LOGGER.fine(() -> "Dropping relayed message from " + message.source() + " for "
                        + message.destination() + ": no direct connection to forward it over");
                // Tell the original sender right away, so a request/response caller fails fast instead
                // of waiting out its request timeout on a peer that is gone. Never for a notice itself,
                // never for a lightweight (fire-and-forget: heartbeat/ping) message, only over a direct
                // connection back to the sender (no notice storms), and only to a sender that announced
                // UNDELIVERABLE support in its handshake — an older node would fail to decode it.
                if (message.type() != MessageType.UNDELIVERABLE && message.source() != null
                        && !message.source().equals(localId)
                        && !ClusterMessage.ZERO_UUID.equals(message.messageId())) {
                    Connection back = connections.get(message.source());
                    if (back != null && back.isOpen() && back.peerSupportsUndeliverable()) {
                        back.send(ClusterMessage.lightweight(MessageType.UNDELIVERABLE, "undeliverable", localId,
                                message.source(),
                                new UndeliverablePayload(message.messageId(), message.destination())));
                    }
                }
            }
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

    /**
     * A peer announced it is closing for good. Honored only first-hand: on the connection currently
     * tracked for that peer, identified by a handshake, and only when the announced node is that
     * connection's identity — which rejects a spoofed LEAVE and a delayed LEAVE of an old incarnation
     * arriving on a replaced socket. Never forwarded. An ephemeral leaver (leader-ineligible or without
     * a listen port, in its own announcement and in this node's view alike) is forgotten at once; a
     * leader-eligible one stays a known voter, so the majority is never shrunk without consensus.
     */
    private void handleLeave(Connection connection, ClusterMessage message) {
        LeavePayload payload = message.payload(LeavePayload.class);
        NodeId remoteId = connection.remoteId().orElse(null);
        NodeInfo leaver = payload != null ? payload.node() : null;
        if (leaver == null || remoteId == null || !connection.handshaked()
                || !leaver.nodeId().equals(remoteId) || !remoteId.equals(message.source())
                || connections.get(remoteId) != connection) {
            LOGGER.fine(() -> "Ignoring LEAVE of " + (leaver != null ? leaver.nodeId() : null) + " on "
                    + config.local().nodeId() + ": not first-hand on the connection tracked for it (remote="
                    + remoteId + ")");
            return;
        }
        String reason = payload.reason();
        NodeInfo known = knownPeers.get(remoteId);
        if (isEphemeral(leaver) && (known == null || isEphemeral(known))) {
            LOGGER.info(() -> "Peer " + remoteId + " announced LEAVE (" + reason + ") on " + config.local().nodeId());
            if (forget(remoteId, config.departedPeerTombstoneTtl().toMillis(), "LEAVE: " + reason)) {
                // First receipt: tell the other peers (PEER_UPDATE departed). The LEAVE itself is never
                // forwarded; what peers learn this way is admission-only (see handlePeerUpdate).
                broadcastPeerList();
            }
            return;
        }
        LOGGER.info(() -> "Leader-eligible peer " + remoteId + " announced LEAVE (" + reason + ") on "
                + config.local().nodeId() + "; kept as a known voter");
        // It is closing anyway: stop reading, so it is no longer "connected" when the listeners are told,
        // and let them skip the disconnect grace. The regular disconnect handling follows as the read
        // loop of this connection ends.
        connection.closeQuietly();
        listeners.forEach(listener -> listener.onPeerLeaving(remoteId));
    }

    /**
     * A relay could not forward one of our messages (no direct connection to its destination): fail
     * the matching pending request/response now rather than at the request timeout.
     */
    private void handleUndeliverable(ClusterMessage notice) {
        UndeliverablePayload payload = notice.payload(UndeliverablePayload.class);
        if (payload == null || payload.messageId() == null) {
            return;
        }
        PendingResponse pending = pendingResponses.get(payload.messageId());
        if (pending == null || !pending.destination.equals(payload.destination())) {
            return; // unknown, already completed, or a notice about some other destination
        }
        if (pendingResponses.remove(payload.messageId(), pending)) {
            pending.cancelTimeout();
            pending.future.completeExceptionally(new IOException("Request " + payload.messageId() + " to "
                    + payload.destination() + " undeliverable: relay " + notice.source()
                    + " has no connection to it"));
        }
    }

    /**
     * Publishes, for {@code nodeId}, another open socket whose remote identity is that peer.
     *
     * @return the connection now live for the peer, or {@code null} when there is none
     */
    private Connection adoptOpenConnection(NodeId nodeId, Connection closed) {
        for (Connection candidate : liveSockets) {
            if (candidate == closed || !candidate.isOpen()
                    || !nodeId.equals(candidate.remoteId().orElse(null))) {
                continue;
            }
            Connection live = registerLiveConnection(nodeId, candidate);
            if (live != null && live.isOpen()) {
                return live;
            }
        }
        return null;
    }

    private void handleDisconnect(Connection connection) {
        connection.remoteId().ifPresent(nodeId -> {
            if (isDeparted(nodeId) && !knownPeers.containsKey(nodeId)) {
                // Forgotten for good: forget() already failed its pending responses and reported the
                // departure (onPeerLeft); this socket closing is not news.
                connections.remove(nodeId, connection);
                LOGGER.fine(() -> "Disconnect of departed peer " + nodeId + " on " + config.local().nodeId() + " ignored");
                return;
            }
            LOGGER.info(() -> "Handling disconnect from " + nodeId + " on " + config.local().nodeId() + " (remote=" + connection.remote + ", open=" + connection.isOpen() + ")");
            // Only treat the peer as disconnected when the currently tracked connection goes away.
            // In rare races, nodes can end up with multiple TCP connections; a stale connection closing
            // must NOT fail in-flight request/response calls if there is still an active connection.
            boolean removedActive = connections.remove(nodeId, connection);
            Connection current = connections.get(nodeId);
            if (!removedActive && current != null && current.isOpen()) {
                LOGGER.info(() -> "Ignoring disconnect for " + nodeId + " on " + config.local().nodeId() + " because an active connection remains");
                return;
            }
            if (current != null && current.isOpen()) {
                return;
            }
            // Pending responses are global, not per connection. Before failing them, adopt any other
            // open connection already identified as this peer but not published for it (one in the
            // middle of its own handshake, or a dial registered but not yet published): the peer is
            // still reachable and the responses will arrive through that connection.
            if (adoptOpenConnection(nodeId, connection) != null) {
                LOGGER.info(() -> "Disconnect from " + nodeId + " on " + config.local().nodeId() + " absorbed: another open connection to it took over");
                return;
            }
            LOGGER.info(() -> "Disconnect confirmed for " + nodeId + " on " + config.local().nodeId() + "; failing pending responses");
            // The per-peer connection lock is deliberately kept (see connectionLocks).
            failPendingResponsesTo(nodeId);
            listeners.forEach(listener -> listener.onPeerDisconnected(nodeId));
        });
    }

    private void failPendingResponsesTo(NodeId nodeId) {
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
    }

    @Override
    public boolean isDeparted(NodeId nodeId) {
        Long expiresAt = departedPeers.get(nodeId);
        return expiresAt != null && expiresAt > System.currentTimeMillis();
    }

    /**
     * Forgets a departed peer for good and tombstones its id for
     * {@link TcpTransportConfig#departedPeerTombstoneTtl()}.
     *
     * @see #forget(NodeId, long, String)
     */
    boolean forget(NodeId nodeId) {
        return forget(nodeId, config.departedPeerTombstoneTtl().toMillis(), "departed");
    }

    /**
     * Forgets a departed peer for good: removes it from the known and verified peers, drops its
     * published connection (closed), its per-peer connection lock and everything the router knows
     * about it, fails its pending responses with {@link PeerDisconnectedException} and reports it to
     * the listeners through {@link TransportListener#onPeerLeft(NodeId)}. Its id is tombstoned for
     * {@code tombstoneTtlMs}: second-hand sources (gossip, a third node's handshake peer list, relayed
     * messages, an inbound socket identified only by the source of its first message) cannot re-admit
     * it, while a direct handshake from that id (a new incarnation), an explicit {@link #addPeer} or
     * the expiry lift the tombstone. Without this, a member that left (e.g. a short-lived client)
     * stayed in {@code knownPeers} forever and every heartbeat broadcast dialed it again.
     *
     * @return {@code true} when the peer was known (or connected) and has now been forgotten
     */
    private boolean forget(NodeId nodeId, long tombstoneTtlMs, String reason) {
        return forget(nodeId, tombstoneTtlMs, reason, false);
    }

    /**
     * @param secondHand the departure was reported by another peer: it is ignored for a peer this node
     *                   holds a handshaked open connection to (a new incarnation already came back) and
     *                   for a leader-eligible peer — checked under the same locks as the removal, so a
     *                   handshake in progress is never undone
     */
    private boolean forget(NodeId nodeId, long tombstoneTtlMs, String reason, boolean secondHand) {
        if (nodeId == null || nodeId.equals(config.local().nodeId())) {
            return false;
        }
        long expiresAt = System.currentTimeMillis() + tombstoneTtlMs;
        boolean wasKnown;
        Connection published;
        // lifecycleLock -> peerTableLock: registerLiveConnection checks the tombstone under the
        // lifecycle lock, so a publication cannot slip in between the tombstone and the removal.
        lifecycleLock.lock();
        try {
            peerTableLock.lock();
            try {
                if (secondHand) {
                    NodeInfo known = knownPeers.get(nodeId);
                    if ((known != null && !isEphemeral(known))
                            || (verifiedPeers.contains(nodeId) && hasHandshakedOpenConnection(nodeId))) {
                        LOGGER.fine(() -> "Ignoring departure of " + nodeId + " on " + config.local().nodeId()
                                + " (" + reason + "): connected first-hand or leader-eligible");
                        return false;
                    }
                }
                departedPeers.merge(nodeId, expiresAt, Math::max);
                wasKnown = knownPeers.remove(nodeId) != null;
                verifiedPeers.remove(nodeId);
                published = connections.remove(nodeId);
            } finally {
                peerTableLock.unlock();
            }
        } finally {
            lifecycleLock.unlock();
        }
        if (published != null) {
            published.closeQuietly();
        }
        dropConnectionLock(nodeId);
        router.forget(nodeId);
        failPendingResponsesTo(nodeId);
        if (!wasKnown && published == null) {
            return false;
        }
        LOGGER.info(() -> "Forgot departed peer " + nodeId + " on " + config.local().nodeId() + " (" + reason
                + "); tombstoned for " + tombstoneTtlMs + " ms");
        listeners.forEach(listener -> listener.onPeerLeft(nodeId));
        return true;
    }

    /**
     * Drops the per-peer connection lock of a forgotten peer, unless a dial is holding it right now
     * (that dial re-checks knownPeers under the lock and gives up; the entry is swept later).
     */
    private void dropConnectionLock(NodeId nodeId) {
        ReentrantLock lock = connectionLocks.get(nodeId);
        if (lock != null && lock.tryLock()) {
            try {
                connectionLocks.remove(nodeId, lock);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Whether {@code peer} is an ephemeral member: leader-ineligible (a client) or without a listen port
     * (a discovery client / placeholder). Only ephemeral peers are ever forgotten: a voter is kept even
     * after it left, so the leadership majority is never shrunk without consensus.
     */
    private static boolean isEphemeral(NodeInfo peer) {
        return peer.port() <= 0 || !peer.isLeaderEligible();
    }

    /**
     * Backstop for departures that never announced themselves (kill -9, OOM, network loss): an
     * ephemeral peer without an open connection for longer than
     * {@link TcpTransportConfig#departedPeerForgetAfter()} is forgotten and tombstoned. Before this, such
     * a peer stayed known forever and every heartbeat broadcast dialed it (up to connectTimeout each),
     * logging "No connection available". A peer is only taken for gone when, for that same window,
     * nothing sourced by it arrived either — directly or through a relay: in a partial mesh (firewall,
     * one-sided link) a live client this node cannot dial keeps talking through a relay, and forgetting
     * it would drop all its relayed traffic with no way back (it never handshakes this node directly).
     */
    private void forgetLongDisconnectedEphemeralPeers(long nowMs) {
        long forgetAfterMs = config.departedPeerForgetAfter().toMillis();
        NodeId localId = config.local().nodeId();
        disconnectedSince.keySet().removeIf(id -> !knownPeers.containsKey(id));
        // Evidence older than the window is meaningless; this also drops ids that were never learned.
        lastInboundFromPeer.values().removeIf(at -> nowMs - at >= forgetAfterMs);
        for (NodeInfo peer : List.copyOf(knownPeers.values())) {
            NodeId id = peer.nodeId();
            if (id.equals(localId) || !isEphemeral(peer) || isConnected(id)) {
                disconnectedSince.remove(id);
                continue;
            }
            long since = disconnectedSince.computeIfAbsent(id, k -> nowMs);
            long disconnectedForMs = nowMs - since;
            Long lastInbound = lastInboundFromPeer.get(id);
            boolean silent = lastInbound == null || nowMs - lastInbound >= forgetAfterMs;
            if (disconnectedForMs >= forgetAfterMs && silent) {
                disconnectedSince.remove(id);
                forget(id, config.departedPeerTombstoneTtl().toMillis(),
                        "ephemeral peer without connection nor traffic for " + disconnectedForMs + " ms");
            }
        }
    }

    /** Lazily removes expired tombstones and sweeps the connection locks of forgotten peers. */
    private void purgeExpiredTombstones(long nowMs) {
        departedPeers.entrySet().removeIf(entry -> entry.getValue() <= nowMs);
        for (NodeId id : connectionLocks.keySet()) {
            if (departedPeers.containsKey(id) && !knownPeers.containsKey(id) && !connections.containsKey(id)) {
                dropConnectionLock(id);
            }
        }
    }

    /** Peers minus departed (tombstoned) ids: reachability input from second-hand reports. */
    private Collection<NodeInfo> admissible(Collection<NodeInfo> peers) {
        if (departedPeers.isEmpty()) {
            return peers;
        }
        List<NodeInfo> admissible = new ArrayList<>(peers.size());
        for (NodeInfo peer : peers) {
            if (!isDeparted(peer.nodeId())) {
                admissible.add(peer);
            }
        }
        return admissible;
    }

    private Map<NodeId, Double> admissible(Map<NodeId, Double> latencies) {
        if (departedPeers.isEmpty()) {
            return latencies;
        }
        Map<NodeId, Double> admissible = new HashMap<>(latencies);
        admissible.keySet().removeIf(this::isDeparted);
        return admissible;
    }

    @Override
    public void close() throws IOException {
        List<Connection> leaveTargets = List.of();
        lifecycleLock.lock();
        try {
            if (running && !leaving && config.leaveOnClose()) {
                leaving = true;
                Set<Connection> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
                for (Connection connection : connections.values()) {
                    if (connection.isOpen() && connection.peerSupportsLeave()) {
                        distinct.add(connection);
                    }
                }
                leaveTargets = List.copyOf(distinct);
            }
        } finally {
            lifecycleLock.unlock();
        }
        // Before the connections are dropped: each peer learns first-hand that this node is gone for
        // good (an ephemeral member is then forgotten instead of redialed until a timeout).
        announceLeave(leaveTargets);
        List<Connection> socketsToClose;
        lifecycleLock.lock();
        try {
            running = false;
            socketsToClose = List.copyOf(liveSockets);
            liveSockets.clear();
            connections.clear();
        } finally {
            lifecycleLock.unlock();
        }
        // Closing sockets outside the lifecycle guard avoids nesting disconnect callbacks
        // under it. A dial/accept finishing later is fenced by registerConnection.
        socketsToClose.forEach(Connection::closeQuietly);
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
        connectionLocks.clear();
        pendingResponses.values().forEach(pending -> {
            pending.cancelTimeout();
            pending.future.completeExceptionally(new IOException("Transport closed"));
        });
        pendingResponses.clear();
    }

    /**
     * Sends a LEAVE directly on each connection (not through broadcast(), which is fire-and-forget on
     * the worker pool) and waits, up to {@link TcpTransportConfig#leaveFlushTimeout()}, until each one
     * has been flushed to its socket. A large outbound backlog ahead of the LEAVE may exceed the
     * timeout: the close then proceeds as a plain close and those peers fall back to the
     * disconnection timeout.
     */
    private void announceLeave(List<Connection> targets) {
        if (targets.isEmpty()) {
            return;
        }
        NodeInfo localInfo = config.local();
        LeavePayload payload = new LeavePayload(localInfo, "close");
        List<CompletableFuture<Void>> flushed = new ArrayList<>(targets.size());
        for (Connection connection : targets) {
            flushed.add(connection.sendAndAwaitFlush(ClusterMessage.request(MessageType.LEAVE, "leave",
                    localInfo.nodeId(), connection.remoteId().orElse(null), payload)));
        }
        try {
            CompletableFuture.allOf(flushed.toArray(CompletableFuture[]::new))
                    .get(config.leaveFlushTimeout().toMillis(), TimeUnit.MILLISECONDS);
            LOGGER.fine(() -> "LEAVE flushed to " + targets.size() + " peer(s) by " + localInfo.nodeId());
        } catch (TimeoutException e) {
            long pending = flushed.stream().filter(f -> !f.isDone()).count();
            LOGGER.info(() -> "LEAVE of " + localInfo.nodeId() + " not flushed to " + pending + " peer(s) within "
                    + config.leaveFlushTimeout() + "; closing anyway");
        } catch (ExecutionException e) {
            LOGGER.fine(() -> "LEAVE of " + localInfo.nodeId() + " not delivered to every peer: " + e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final class Connection implements Closeable {
        private final Socket socket;
        private final OutputStream outputStream;
        private final DataInputStream inputStream;
        // Per-connection codec: outbound compression is negotiated per-peer in the handshake, so
        // each connection owns its codec and toggles its compress-output flag once the peer
        // advertises support. Decoding is always capable, independent of this flag.
        private final CompositeMessageCodec codec;
        private final OutboundChannel outbound = new OutboundChannel(config.outboundQueueCapacity());
        private final boolean outboundInitiated;
        private volatile NodeInfo remote;
        private volatile boolean peerSupportsCompression;
        // Negotiated in the handshake (8.3.0): only a peer that announced it receives UNDELIVERABLE.
        private volatile boolean peerSupportsUndeliverable;
        // Negotiated in the handshake (8.7.0): only a peer that announced it receives LEAVE.
        private volatile boolean peerSupportsLeave;
        // Whether the remote identity was set by a handshake on this socket (not inferred).
        private volatile boolean handshaked;
        // Messages whose flush to the socket someone waits for (sendAndAwaitFlush), by messageId.
        private final Map<UUID, CompletableFuture<Void>> flushWaiters = new ConcurrentHashMap<>();
        private volatile boolean open = true;

        private Connection(Socket socket, boolean outboundInitiated) throws IOException {
            this.socket = socket;
            this.outboundInitiated = outboundInitiated;
            this.codec = new CompositeMessageCodec(config.compressionMinSize());
            this.outputStream = socket.getOutputStream();
            this.outputStream.flush();
            this.inputStream = new DataInputStream(socket.getInputStream());
            // Use Virtual Thread for writing (one per connection to ensure order)
            Thread.ofVirtual().name("ngrid-transport-writer").start(this::drainOutbound);
        }

        void setRemote(NodeInfo remote) {
            this.remote = remote;
        }

        /**
         * Records whether the remote peer can decode LZ4-compressed frames (advertised in its
         * handshake) and enables outbound compression on this connection only when both this node
         * has compression enabled and the peer supports it. Until this is called the flag stays
         * disabled, so the handshake itself is never compressed.
         */
        void setPeerSupportsCompression(boolean peerSupports) {
            this.peerSupportsCompression = peerSupports;
            codec.setCompressOutput(config.compressionEnabled() && peerSupports);
        }

        void setPeerSupportsUndeliverable(boolean peerSupports) {
            this.peerSupportsUndeliverable = peerSupports;
        }

        boolean peerSupportsUndeliverable() {
            return peerSupportsUndeliverable;
        }

        void setPeerSupportsLeave(boolean peerSupports) {
            this.peerSupportsLeave = peerSupports;
        }

        boolean peerSupportsLeave() {
            return peerSupportsLeave;
        }

        void markHandshaked() {
            this.handshaked = true;
        }

        boolean handshaked() {
            return handshaked;
        }

        Optional<NodeId> remoteId() {
            return Optional.ofNullable(remote).map(NodeInfo::nodeId);
        }

        boolean isOpen() {
            return open && !socket.isClosed();
        }

        int outboundDepth() {
            return outbound.dataDepth();
        }

        long outboundDropped() {
            return outbound.droppedCount();
        }

        void send(ClusterMessage message) {
            if (!isOpen()) {
                return;
            }
            outbound.enqueue(message);
        }

        /**
         * Enqueues {@code message} (which must carry a unique messageId) and returns a future completed
         * by the writer right after that exact message was flushed to the socket, or exceptionally when
         * the connection closes or the writer fails first.
         */
        CompletableFuture<Void> sendAndAwaitFlush(ClusterMessage message) {
            CompletableFuture<Void> flushed = new CompletableFuture<>();
            flushWaiters.put(message.messageId(), flushed);
            if (isOpen()) {
                outbound.enqueue(message);
            }
            if (!isOpen()) {
                failFlushWaiters(); // closed before (or while) enqueuing: the writer may be gone
            }
            return flushed;
        }

        private void failFlushWaiters() {
            for (UUID id : List.copyOf(flushWaiters.keySet())) {
                CompletableFuture<Void> waiter = flushWaiters.remove(id);
                if (waiter != null) {
                    waiter.completeExceptionally(new IOException("Connection to " + remote + " closed before flush"));
                }
            }
        }

        private void drainOutbound() {
            byte[] lengthPrefix = new byte[Integer.BYTES];
            try {
                while (isOpen()) {
                    ClusterMessage message = outbound.poll(1, TimeUnit.SECONDS);
                    if (message == null) {
                        continue; // timeout — recheck isOpen()
                    }
                    // One writer owns this raw socket stream. DataOutputStream.write(byte[])
                    // itself is synchronized on Java 21 and would pin a slow socket writer.
                    byte[] data = codec.encode(message);
                    lengthPrefix[0] = (byte) (data.length >>> 24);
                    lengthPrefix[1] = (byte) (data.length >>> 16);
                    lengthPrefix[2] = (byte) (data.length >>> 8);
                    lengthPrefix[3] = (byte) data.length;
                    outputStream.write(lengthPrefix);
                    outputStream.write(data);
                    outputStream.flush();
                    if (!flushWaiters.isEmpty()) {
                        CompletableFuture<Void> waiter = flushWaiters.remove(message.messageId());
                        if (waiter != null) {
                            waiter.complete(null);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (open) {
                    LOGGER.log(Level.FINE, "Writer terminating for {0}: {1}", new Object[]{remote, e.getMessage()});
                }
                closeQuietly();
            } finally {
                failFlushWaiters();
            }
        }

        void readLoop() {
            try {
                while (isOpen()) {
                    int length = inputStream.readInt();
                    if (length <= 0 || length > 64 * 1024 * 1024) { // 64 MB sanity limit
                        throw new IOException("Invalid frame length: " + length);
                    }
                    byte[] data = inputStream.readNBytes(length);
                    if (data.length < length) {
                        throw new EOFException("Unexpected end of stream reading frame");
                    }
                    ClusterMessage message;
                    try {
                        message = codec.decode(data);
                    } catch (IOException e) {
                        if (e.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException) {
                            // One malformed/unknown message (e.g. a type introduced by a newer node) is
                            // dropped; only framing and I/O errors close the connection.
                            LOGGER.log(Level.WARNING, "Dropping undecodable message from " + remote + ": "
                                    + e.getCause().getMessage());
                            continue;
                        }
                        throw e;
                    }
                    if (message.type() == null) {
                        LOGGER.log(Level.WARNING, () -> "Dropping message of unknown type from " + remote
                                + " (newer protocol?)");
                        continue;
                    }
                    if (message.type() == MessageType.HANDSHAKE) {
                        handleHandshake(this, message);
                    } else if (message.type() == MessageType.LEAVE) {
                        handleLeave(this, message);
                    } else {
                        if (remote == null && message.source() != null) {
                            NodeInfo inferred = new NodeInfo(
                                    message.source(),
                                    socket.getInetAddress().getHostAddress(),
                                    socket.getPort());
                            remote = inferred;
                            if (registerLiveConnection(inferred.nodeId(), this) != this) { return; }
                        }
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
            liveSockets.remove(this);
            // No need to shutdown writer executor anymore, the flag + socket close will kill it
            socket.close();
        }

        private void closeQuietly() {
            if (open) {
                LOGGER.fine(() -> "Quietly closing connection to " + remote);
            }
            open = false;
            liveSockets.remove(this);
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
