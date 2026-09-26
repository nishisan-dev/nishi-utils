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
    private final Duration departedPeerTombstoneTtl;
    private final Duration departedPeerForgetAfter;
    private final boolean leaveOnClose;
    private final Duration leaveFlushTimeout;

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
        this.departedPeerTombstoneTtl = builder.departedPeerTombstoneTtl;
        this.departedPeerForgetAfter = builder.departedPeerForgetAfter;
        this.leaveOnClose = builder.leaveOnClose;
        this.leaveFlushTimeout = builder.leaveFlushTimeout;
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

    /**
     * How long the id of a peer that announced its departure (LEAVE, first-hand) stays tombstoned; the
     * departure disseminated from it carries the remaining time. A departure only inferred by
     * {@link #departedPeerForgetAfter()} is tombstoned for that window instead. While tombstoned, second-hand
     * sources (gossip, a third node's handshake peer list, relayed messages, an inbound connection
     * without handshake) cannot bring the id back; a direct handshake from that id (a new incarnation)
     * or an explicit {@link TcpTransport#addPeer} clears it at once. Defaults to 10 minutes.
     *
     * @return the tombstone time-to-live
     * @since 8.7.0
     */
    public Duration departedPeerTombstoneTtl() {
        return departedPeerTombstoneTtl;
    }

    /**
     * How long an <b>ephemeral</b> peer (leader-ineligible, or without a listen port) may stay without
     * an open connection, and without any traffic from it (direct or relayed), before the transport
     * forgets it; its id is then tombstoned for this same window only, not for
     * {@link #departedPeerTombstoneTtl()}, since the departure is inferred. It is the backstop for departures that never announced
     * themselves (kill -9, OOM, network loss); a graceful close announces itself with a LEAVE.
     * Leader-eligible peers are never forgotten this way. Defaults to 1 minute; {@code NGridNode} uses
     * {@code max(1 min, 2 x heartbeatTimeout)}.
     *
     * @return the disconnection time after which an ephemeral peer is forgotten
     * @since 8.7.0
     */
    public Duration departedPeerForgetAfter() {
        return departedPeerForgetAfter;
    }

    /**
     * Whether {@link TcpTransport#close()} announces the departure with a {@code LEAVE} on each open
     * connection whose peer supports it, so ephemeral members are forgotten at once instead of after
     * {@link #departedPeerForgetAfter()}. Defaults to {@code true}.
     *
     * @return whether a closing transport sends LEAVE
     * @since 8.7.0
     */
    public boolean leaveOnClose() {
        return leaveOnClose;
    }

    /**
     * Upper bound {@link TcpTransport#close()} waits for the LEAVE messages to be flushed to the sockets
     * before closing them. A large outbound backlog ahead of the LEAVE may exceed it; the close then
     * proceeds as a plain close (the peers fall back to the disconnection timeout). Defaults to 500 ms.
     *
     * @return the LEAVE flush timeout
     * @since 8.7.0
     */
    public Duration leaveFlushTimeout() {
        return leaveFlushTimeout;
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
        private Duration departedPeerTombstoneTtl = Duration.ofMinutes(10);
        private Duration departedPeerForgetAfter = Duration.ofMinutes(1);
        private boolean leaveOnClose = true;
        private Duration leaveFlushTimeout = Duration.ofMillis(500);

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

        /**
         * Sets how long the id of a forgotten peer stays tombstoned (default 10 minutes).
         *
         * @param ttl the tombstone time-to-live, must be positive
         * @return this builder
         * @since 8.7.0
         */
        public Builder departedPeerTombstoneTtl(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            if (ttl.isZero() || ttl.isNegative()) {
                throw new IllegalArgumentException("departedPeerTombstoneTtl must be positive");
            }
            this.departedPeerTombstoneTtl = ttl;
            return this;
        }

        /**
         * Sets how long an ephemeral peer may stay disconnected before it is forgotten (default 1
         * minute).
         *
         * @param after the disconnection time, must be positive
         * @return this builder
         * @since 8.7.0
         */
        public Builder departedPeerForgetAfter(Duration after) {
            Objects.requireNonNull(after, "after");
            if (after.isZero() || after.isNegative()) {
                throw new IllegalArgumentException("departedPeerForgetAfter must be positive");
            }
            this.departedPeerForgetAfter = after;
            return this;
        }

        /**
         * Enables or disables the LEAVE announcement on close (default {@code true}).
         *
         * @param enabled whether a closing transport sends LEAVE
         * @return this builder
         * @since 8.7.0
         */
        public Builder leaveOnClose(boolean enabled) {
            this.leaveOnClose = enabled;
            return this;
        }

        /**
         * Sets how long close() waits for the LEAVE messages to be flushed (default 500 ms).
         *
         * @param timeout the flush timeout, must not be negative
         * @return this builder
         * @since 8.7.0
         */
        public Builder leaveFlushTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative()) {
                throw new IllegalArgumentException("leaveFlushTimeout must not be negative");
            }
            this.leaveFlushTimeout = timeout;
            return this;
        }

        public TcpTransportConfig build() {
            return new TcpTransportConfig(this);
        }
    }
}
