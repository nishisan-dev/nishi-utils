package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration parameters for the {@link TcpTransport} implementation.
 */
public final class TcpTransportConfig {
    private final NodeInfo local;
    private final Set<NodeInfo> initialPeers;
    private final Duration connectTimeout;
    private final Duration reconnectInterval;

    private TcpTransportConfig(Builder builder) {
        this.local = builder.local;
        this.initialPeers = Collections.unmodifiableSet(new HashSet<>(builder.initialPeers));
        this.connectTimeout = builder.connectTimeout;
        this.reconnectInterval = builder.reconnectInterval;
    }

    public NodeInfo local() {
        return local;
    }

    public Set<NodeInfo> initialPeers() {
        return initialPeers;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration reconnectInterval() {
        return reconnectInterval;
    }

    public static Builder builder(NodeInfo local) {
        return new Builder(local);
    }

    public static final class Builder {
        private final NodeInfo local;
        private final Set<NodeInfo> initialPeers = new HashSet<>();
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration reconnectInterval = Duration.ofSeconds(3);

        private Builder(NodeInfo local) {
            this.local = Objects.requireNonNull(local, "local");
        }

        public Builder addPeer(NodeInfo peer) {
            if (!peer.equals(local)) {
                initialPeers.add(peer);
            }
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder reconnectInterval(Duration interval) {
            this.reconnectInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        public TcpTransportConfig build() {
            return new TcpTransportConfig(this);
        }
    }
}
