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
    private final Duration requestTimeout;
    private final int workerThreads;
    private final Duration routeProbeInterval;
    private final int outboundQueueCapacity;
    private final boolean compressionEnabled;
    private final int compressionMinSize;

    private TcpTransportConfig(Builder builder) {
        this.local = builder.local;
        this.initialPeers = Collections.unmodifiableSet(new HashSet<>(builder.initialPeers));
        this.connectTimeout = builder.connectTimeout;
        this.reconnectInterval = builder.reconnectInterval;
        this.requestTimeout = builder.requestTimeout;
        this.workerThreads = builder.workerThreads;
        this.routeProbeInterval = builder.routeProbeInterval;
        this.outboundQueueCapacity = builder.outboundQueueCapacity;
        this.compressionEnabled = builder.compressionEnabled;
        this.compressionMinSize = builder.compressionMinSize;
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

    /**
     * Timeout for request/response style calls (e.g. {@link TcpTransport#sendAndAwait(dev.nishisan.utils.ngrid.common.ClusterMessage)}).
     */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * Worker threads used by the transport to handle accepting connections, reading loops and
     * asynchronous tasks like reconnect attempts.
     */
    public int workerThreads() {
        return workerThreads;
    }

    /**
     * Interval for probing proxy routes to check if a direct connection can be
     * re-established. Defaults to 10 seconds.
     *
     * @since 3.6.0
     */
    public Duration routeProbeInterval() {
        return routeProbeInterval;
    }

    /**
     * Maximum number of pending replication messages buffered per connection
     * before excess replication is dropped (the lagging follower then recovers
     * via the gap/snapshot catch-up). Control traffic is never bounded. A value
     * of {@code 0} means unbounded (default, legacy behaviour).
     *
     * @return the per-connection outbound replication capacity
     * @since 2.2.0
     */
    public int outboundQueueCapacity() {
        return outboundQueueCapacity;
    }

    /**
     * Whether outbound transport frames are eligible for LZ4 compression. Compression is
     * additionally gated per-peer by the handshake capability negotiation, and decoding of
     * compressed frames is always supported regardless of this flag. Defaults to {@code true}.
     *
     * @return {@code true} if outbound compression is enabled
     * @since 4.6.0
     */
    public boolean compressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Minimum serialized JSON size, in bytes, below which a frame is never compressed (small
     * frames would not benefit and could even inflate). Defaults to {@code 512}.
     *
     * @return the minimum payload size eligible for compression
     * @since 4.6.0
     */
    public int compressionMinSize() {
        return compressionMinSize;
    }

    public static Builder builder(NodeInfo local) {
        return new Builder(local);
    }

    public static final class Builder {
        private final NodeInfo local;
        private final Set<NodeInfo> initialPeers = new HashSet<>();
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration reconnectInterval = Duration.ofSeconds(3);
        private Duration requestTimeout = Duration.ofSeconds(20);
        private int workerThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
        private Duration routeProbeInterval = Duration.ofSeconds(10);
        private int outboundQueueCapacity = 0;
        private boolean compressionEnabled = true;
        private int compressionMinSize = 512;

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

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /**
         * Sets the interval for probing proxy routes.
         *
         * @since 3.6.0
         */
        public Builder routeProbeInterval(Duration interval) {
            this.routeProbeInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        public Builder workerThreads(int workerThreads) {
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads must be >= 1");
            }
            this.workerThreads = workerThreads;
            return this;
        }

        /**
         * Sets the per-connection outbound replication capacity ({@code 0} =
         * unbounded, the default).
         *
         * @param capacity the capacity, must be {@code >= 0}
         * @return this builder
         * @since 2.2.0
         */
        public Builder outboundQueueCapacity(int capacity) {
            if (capacity < 0) {
                throw new IllegalArgumentException("outboundQueueCapacity must be >= 0");
            }
            this.outboundQueueCapacity = capacity;
            return this;
        }

        /**
         * Enables or disables LZ4 compression of outbound transport frames (default
         * {@code true}). Compression is still negotiated per-peer in the handshake; decoding
         * of compressed frames is always supported regardless of this flag.
         *
         * @param enabled whether to compress eligible outbound frames
         * @return this builder
         * @since 4.6.0
         */
        public Builder compressionEnabled(boolean enabled) {
            this.compressionEnabled = enabled;
            return this;
        }

        /**
         * Sets the minimum serialized JSON size (bytes) eligible for compression (default
         * {@code 512}). Smaller frames are sent uncompressed.
         *
         * @param minSize the minimum payload size, must be {@code >= 0}
         * @return this builder
         * @since 4.6.0
         */
        public Builder compressionMinSize(int minSize) {
            if (minSize < 0) {
                throw new IllegalArgumentException("compressionMinSize must be >= 0");
            }
            this.compressionMinSize = minSize;
            return this;
        }

        public TcpTransportConfig build() {
            return new TcpTransportConfig(this);
        }
    }
}
