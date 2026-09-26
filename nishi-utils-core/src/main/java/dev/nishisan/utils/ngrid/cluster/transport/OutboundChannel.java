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
import dev.nishisan.utils.ngrid.common.MessageType;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Outbound message buffer for a single transport connection, with bounded backpressure on the
 * recoverable data plane (issue #113; restored in 8.8.0 after the 5.0.0 push-protocol removal left the
 * queue unbounded and the counters at a constant zero).
 *
 * <p>
 * Policy: only <b>data frames</b> — {@link MessageType#RELAY_STREAM_BATCH}, the replication stream a
 * follower pulls and re-fetches on its own when a batch is missing — count towards
 * {@code capacity} and may be dropped. When a data frame arrives while {@code capacity} data frames
 * are already queued, the <b>oldest</b> queued data frame is dropped (the newest carries the most
 * recent state; the follower's next fetch recovers the gap). Every other frame — HEARTBEAT, PING,
 * HANDSHAKE, PEER_UPDATE, LEAVE, UNDELIVERABLE, requests/responses, SYNC_*, RELAY_STREAM_FETCH — is
 * control traffic: never dropped, never counted, so a slow follower can never starve the heartbeat
 * (which would trigger a spurious re-election) nor lose a request.
 * </p>
 *
 * <p>
 * {@code capacity == 0} means unbounded (nothing is ever dropped); the depth counter is still kept
 * for observability. The limit is soft: the check-then-enqueue race lets concurrent producers
 * overshoot by at most their number. The quota is per physical connection: a data frame relayed for
 * another destination is subject to the relay's quota (memory relief on the relay; the destination's
 * catch-up recovers it).
 * </p>
 *
 * <p>
 * This class is not part of the public API and is thread-safe.
 * </p>
 *
 * @since 2.2.0
 */
final class OutboundChannel {

    private final LinkedBlockingQueue<ClusterMessage> queue = new LinkedBlockingQueue<>();
    private final int capacity;
    private final AtomicInteger pendingData = new AtomicInteger();
    private final AtomicLong dropped = new AtomicLong();

    /**
     * Creates an outbound channel.
     *
     * @param capacity maximum number of queued data frames before the oldest is dropped;
     *                 {@code 0} for unbounded
     * @throws IllegalArgumentException if {@code capacity} is negative
     */
    OutboundChannel(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("replicationCapacity must be >= 0");
        }
        this.capacity = capacity;
    }

    /** The one rule deciding which frames are bounded (data plane) — kept in a single place. */
    private static boolean isDataFrame(ClusterMessage message) {
        return message.type() == MessageType.RELAY_STREAM_BATCH;
    }

    /**
     * Enqueues a message. Control frames are always accepted. A data frame is always accepted too,
     * but when the data quota is full the oldest queued data frame is dropped to make room for it.
     *
     * @param message the message to enqueue
     * @return {@code true} when the message was queued (always, for the current policy)
     */
    boolean enqueue(ClusterMessage message) {
        if (!isDataFrame(message)) {
            return queue.offer(message);
        }
        if (capacity > 0 && pendingData.get() >= capacity && dropOldestDataFrame()) {
            dropped.incrementAndGet();
        }
        pendingData.incrementAndGet();
        return queue.offer(message);
    }

    private boolean dropOldestDataFrame() {
        for (Iterator<ClusterMessage> it = queue.iterator(); it.hasNext(); ) {
            if (isDataFrame(it.next())) {
                it.remove();
                pendingData.decrementAndGet();
                return true;
            }
        }
        return false; // the quota was just drained by the writer
    }

    /**
     * Polls the next message, waiting up to the given timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit    the unit of {@code timeout}
     * @return the next message, or {@code null} if the timeout elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    ClusterMessage poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClusterMessage message = queue.poll(timeout, unit);
        if (message != null && isDataFrame(message)) {
            pendingData.decrementAndGet();
        }
        return message;
    }

    /**
     * Removes every queued message, in FIFO order, into {@code sink} (atomically with respect to
     * concurrent enqueues/polls). Used when the connection closes to hand its unsent frames over to
     * the connection that replaced it.
     *
     * @param sink where the drained messages go
     * @return the number of messages drained
     */
    int drainTo(Collection<? super ClusterMessage> sink) {
        java.util.ArrayList<ClusterMessage> drained = new java.util.ArrayList<>();
        int count = queue.drainTo(drained);
        for (ClusterMessage message : drained) {
            if (isDataFrame(message)) {
                pendingData.decrementAndGet();
            }
        }
        sink.addAll(drained);
        return count;
    }

    /**
     * Returns the number of data frames currently queued (bounded by {@link #capacity()} when it is
     * positive, up to the concurrent-producer overshoot).
     *
     * @return the queued data frame count
     */
    int dataDepth() {
        return Math.max(0, pendingData.get());
    }

    /**
     * Returns the cumulative number of data frames dropped by backpressure on this connection.
     *
     * @return the drop count
     */
    long droppedCount() {
        return dropped.get();
    }

    /**
     * Returns the configured data-frame capacity; {@code 0} means unbounded.
     *
     * @return the capacity
     */
    int capacity() {
        return capacity;
    }
}
