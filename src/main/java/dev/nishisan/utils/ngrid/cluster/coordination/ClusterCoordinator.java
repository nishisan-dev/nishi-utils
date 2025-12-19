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

package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.LeaderElectionListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Maintains the cluster membership and performs a deterministic leader election based on
 * the highest active {@link NodeId}. The coordinator also emits heartbeat messages using
 * the underlying {@link Transport}.
 */
public final class ClusterCoordinator implements TransportListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ClusterCoordinator.class.getName());

    private final Transport transport;
    private final ClusterCoordinatorConfig config;
    private final Map<NodeId, ClusterMember> members = new ConcurrentHashMap<>();
    private final Set<LeadershipListener> leadershipListeners = new CopyOnWriteArraySet<>();
    private final Set<LeaderElectionListener> leaderElectionListeners = new CopyOnWriteArraySet<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<NodeId> leader = new AtomicReference<>();
    private final Object leaderComputationLock = new Object();
    private final AtomicReference<NodeId> preferredLeader = new AtomicReference<>();
    private volatile long preferredLeaderUntilMs;

    private volatile boolean running;

    public ClusterCoordinator(Transport transport, ClusterCoordinatorConfig config, ScheduledExecutorService scheduler) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /**
     * Starts the cluster coordination process, initializing necessary components and tasks.
     *
     * This method transitions the cluster coordination to a running state if it is not already running.
     * It sets the local node as a cluster member, registers itself as a transport listener, and schedules
     * periodic tasks for sending heartbeats and evicting inactive members. Leadership is also recomputed
     * upon startup to establish the current cluster leader.
     *
     * The following actions are performed:
     * - The `running` flag is set to true, marking the cluster as active.
     * - The local node is added to the list of cluster members.
     * - A listener is added to the transport layer to handle incoming messages and events.
     * - Heartbeat messages are scheduled to be broadcast at regular intervals as defined in the configuration.
     * - A task to evict inactive members is scheduled, removing members that miss multiple heartbeats.
     * - The cluster leader is computed and updated, notifying relevant listeners.
     *
     * If the cluster is already running, the method exits without making any changes.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        NodeInfo local = transport.local();
        members.put(local.nodeId(), new ClusterMember(local));
        transport.addListener(this);
        scheduler.scheduleAtFixedRate(this::sendHeartbeat,
                0,
                config.heartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::evictDeadMembers,
                config.heartbeatInterval().toMillis(),
                config.heartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        recomputeLeader();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
    }

    public boolean isLeader() {
        NodeId leaderId = leader.get();
        return leaderId != null && leaderId.equals(transport.local().nodeId());
    }

    public void setPreferredLeader(NodeId leaderId, Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero()) {
            preferredLeader.set(null);
            preferredLeaderUntilMs = 0L;
            recomputeLeader();
            return;
        }
        preferredLeader.set(leaderId);
        preferredLeaderUntilMs = Instant.now().plus(ttl).toEpochMilli();
        recomputeLeader();
    }

    /**
     * Retrieves information about the current leader of the cluster.
     *
     * This method attempts to fetch the {@code NodeInfo} of the leader node as identified by
     * the current leader's {@code NodeId}. If the leader is null or the leader's associated
     * {@code ClusterMember} is not active, an empty {@code Optional} is returned. Otherwise,
     * the method returns the {@code NodeInfo} of the active leader wrapped in an {@code Optional}.
     *
     * @return an {@code Optional} containing the {@code NodeInfo} of the leader if available and active;
     *         otherwise, an empty {@code Optional}.
     */
    public Optional<NodeInfo> leaderInfo() {
        NodeId leaderId = leader.get();
        if (leaderId == null) {
            return Optional.empty();
        }
        ClusterMember member = members.get(leaderId);
        return member != null && member.isActive() ? Optional.of(member.info()) : Optional.empty();
    }

    /**
     * Retrieves a collection of active cluster members.
     *
     * This method filters the cluster members to include only those marked
     * as active and then maps them to their respective {@code NodeInfo} objects.
     *
     * @return a collection of {@code NodeInfo}*/
    public Collection<NodeInfo> activeMembers() {
        return members.values().stream()
                .filter(ClusterMember::isActive)
                .map(ClusterMember::info)
                .toList();
    }

    public void addLeadershipListener(LeadershipListener listener) {
        leadershipListeners.add(listener);
    }

    public void removeLeadershipListener(LeadershipListener listener) {
        leadershipListeners.remove(listener);
    }

    public void addLeaderElectionListener(LeaderElectionListener listener) {
        leaderElectionListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    public void removeLeaderElectionListener(LeaderElectionListener listener) {
        leaderElectionListeners.remove(listener);
    }

    /**
     * Sends a heartbeat message to all nodes in the cluster.
     *
     * This method constructs a heartbeat message containing a timestamp
     * and broadcasts it to all members of the cluster using the transport layer.
     * The heartbeat message is used by the cluster to confirm that this node
     * is still active. The method exits immediately if the cluster is not marked
     * as running.
     *
     * The message includes the following:
     * - Type: HEARTBEAT, to indicate the nature of the message.
     * - Qualifier: "hb", providing additional context about the message.
     * - Source: The local node's identifier.
     * - Payload: A `HeartbeatPayload` containing the current timestamp.
     */
    private void sendHeartbeat() {
        try {
            if (!running) {
                return;
            }
            HeartbeatPayload payload = HeartbeatPayload.now();
            ClusterMessage heartbeat = ClusterMessage.request(MessageType.HEARTBEAT,
                    "hb",
                    transport.local().nodeId(),
                    null,
                    payload);
            transport.broadcast(heartbeat);
        } catch (Throwable t) {
            LOGGER.log(java.util.logging.Level.SEVERE, "Unexpected error in heartbeat task", t);
        }
    }

    /**
     * Scans the cluster members for inactive nodes and evicts them if they have not sent a heartbeat
     * within the configured timeout period.
     *
     * This method checks each cluster member to ensure that it is still active. If a member's last
     * heartbeat timestamp exceeds the configured heartbeat timeout, the member is marked as inactive.
     * The leader is then recomputed to reflect the updated state of the cluster. The local node is
     * excluded from eviction checks.
     *
     * If the cluster is not running, the method exits without performing any actions.
     */
    private void evictDeadMembers() {
        try {
            if (!running) {
                return;
            }
            long now = Instant.now().toEpochMilli();
            for (ClusterMember member : members.values()) {
                if (member.id().equals(transport.local().nodeId())) {
                    continue;
                }
                if (member.isActive() && now - member.lastHeartbeat() > config.heartbeatTimeout().toMillis()) {
                    LOGGER.fine(() -> "Marking member inactive due to missed heartbeat: " + member.info());
                    member.markInactive();
                    recomputeLeader();
                }
            }
        } catch (Throwable t) {
            LOGGER.log(java.util.logging.Level.SEVERE, "Unexpected error in eviction task", t);
        }
    }

    /**
     * Recomputes the leader of the cluster based on currently active members.
     *
     * The method identifies the highest-ranking active node by comparing their unique identifiers,
     * updates the cluster's leader state, and notifies registered listeners if the leadership
     * status has changed.
     *
     * Leadership change is determined by examining the difference between the previous leader
     * and the newly computed leader. If the local node's leadership state changes, the corresponding
     * listeners are notified.
     */
    private void recomputeLeader() {
        synchronized (leaderComputationLock) {
            NodeId preferred = preferredLeader.get();
            if (preferred != null && preferredLeaderUntilMs > Instant.now().toEpochMilli()) {
                ClusterMember preferredMember = members.get(preferred);
                if (preferredMember != null && preferredMember.isActive()) {
                    NodeId previous = leader.getAndSet(preferred);
                    if (!Objects.equals(previous, preferred)) {
                        leadershipListeners.forEach(listener -> listener.onLeaderChanged(preferred));

                        NodeId localNodeId = transport.local().nodeId();
                        boolean wasLeader = previous != null && previous.equals(localNodeId);
                        boolean isLeader = preferred.equals(localNodeId);
                        if (wasLeader != isLeader) {
                            leaderElectionListeners.forEach(listener -> listener.onLeadershipChanged(isLeader, preferred));
                        }
                    }
                    return;
                }
            }
            Optional<NodeId> newLeader = members.values().stream()
                    .filter(ClusterMember::isActive)
                    .map(ClusterMember::id)
                    .max(Comparator.naturalOrder());
            NodeId newLeaderId = newLeader.orElse(null);
            NodeId previous = leader.getAndSet(newLeaderId);
            if (!Objects.equals(previous, newLeaderId)) {
                leadershipListeners.forEach(listener -> listener.onLeaderChanged(newLeaderId));

                // Notify LeaderElectionListener if local node's leadership status changed
                NodeId localNodeId = transport.local().nodeId();
                boolean wasLeader = previous != null && previous.equals(localNodeId);
                boolean isLeader = newLeaderId != null && newLeaderId.equals(localNodeId);
                if (wasLeader != isLeader) {
                    leaderElectionListeners.forEach(listener -> listener.onLeadershipChanged(isLeader, newLeaderId));
                }
            }
        }
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        members.compute(peer.nodeId(), (id, existing) -> {
            // Replace any placeholder member information (e.g. created from a heartbeat before we learned host/port)
            // or update when host/port changes (e.g. peer restarted on a new port).
            if (existing == null || !existing.info().equals(peer)) {
                return new ClusterMember(peer);
            }
            existing.touch();
            return existing;
        });
        recomputeLeader();
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        ClusterMember member = members.get(peerId);
        if (member != null) {
            member.markInactive();
            NodeId preferred = preferredLeader.get();
            if (preferred != null && preferred.equals(peerId)) {
                preferredLeader.set(null);
                preferredLeaderUntilMs = 0L;
            }
            recomputeLeader();
        }
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.HEARTBEAT) {
            message.payload(HeartbeatPayload.class);
            NodeId source = message.source();
            members.computeIfAbsent(source, id -> new ClusterMember(findPeerInfo(source).orElseGet(() -> new NodeInfo(source, "", 0)))).touch();
            recomputeLeader();
        }
    }

    private Optional<NodeInfo> findPeerInfo(NodeId id) {
        return transport.peers().stream().filter(info -> info.nodeId().equals(id)).findFirst();
    }

    @Override
    public void close() throws IOException {
        stop();
        scheduler.shutdownNow();
    }
}
