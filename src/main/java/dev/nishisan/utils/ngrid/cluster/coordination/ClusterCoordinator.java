package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.Closeable;
import java.io.IOException;
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
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<NodeId> leader = new AtomicReference<>();

    private volatile boolean running;

    public ClusterCoordinator(Transport transport, ClusterCoordinatorConfig config, ScheduledExecutorService scheduler) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

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

    public Optional<NodeInfo> leaderInfo() {
        NodeId leaderId = leader.get();
        if (leaderId == null) {
            return Optional.empty();
        }
        ClusterMember member = members.get(leaderId);
        return member != null && member.isActive() ? Optional.of(member.info()) : Optional.empty();
    }

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

    private void sendHeartbeat() {
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
    }

    private void evictDeadMembers() {
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
    }

    private void recomputeLeader() {
        Optional<NodeId> newLeader = members.values().stream()
                .filter(ClusterMember::isActive)
                .map(ClusterMember::id)
                .max(Comparator.naturalOrder());
        NodeId previous = leader.getAndSet(newLeader.orElse(null));
        if (!Objects.equals(previous, newLeader.orElse(null))) {
            leadershipListeners.forEach(listener -> listener.onLeaderChanged(newLeader.orElse(null)));
        }
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        members.compute(peer.nodeId(), (id, existing) -> {
            ClusterMember member = existing == null ? new ClusterMember(peer) : existing;
            member.touch();
            return member;
        });
        recomputeLeader();
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        ClusterMember member = members.get(peerId);
        if (member != null) {
            member.markInactive();
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
