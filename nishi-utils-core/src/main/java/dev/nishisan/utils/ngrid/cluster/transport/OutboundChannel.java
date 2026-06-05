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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Outbound message buffer for a single transport connection, with a bounded
 * backpressure policy applied <em>only</em> to replication traffic.
 *
 * <p>
 * Under best-effort writes ({@code quorum=1}), a burst of large
 * {@link MessageType#REPLICATION_REQUEST} messages could grow the per-connection
 * outbound queue without limit, risking leader heap pressure / OOM. This channel
 * caps the number of <strong>pending replication</strong> messages per connection
 * and, when the cap is reached, <strong>drops</strong> the excess replication
 * message instead of buffering it. The lagging follower then recovers through the
 * existing gap/snapshot catch-up mechanism, which is driven periodically by the
 * leader high-watermark carried in heartbeats &mdash; so a dropped replication
 * message is reconciled even if the burst stops.
 * </p>
 *
 * <p>
 * Control and non-replication traffic (heartbeats, pings, sync, acks, client
 * requests/responses, sequence resends, ...) is <strong>never</strong> counted
 * nor dropped, so it always flows: the heartbeat cycle is never throttled and
 * thus the drop policy carries no risk of spurious re-election.
 * </p>
 *
 * <p>
 * The cap is a <em>soft</em> limit: the {@code check-then-increment} on
 * {@link #enqueue(ClusterMessage)} can momentarily overshoot the configured
 * capacity by at most the number of concurrent producers, but it can never grow
 * unbounded. Replication depth is tracked even when no capacity is configured
 * ({@code capacity == 0}, unbounded), so the {@link #dataDepth() occupancy
 * metric} stays meaningful for diagnosing pressure before a bound is set; in that
 * mode replication is measured but never dropped, preserving the legacy
 * behaviour.
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
    private final AtomicInteger pendingReplication = new AtomicInteger();
    private final AtomicLong droppedReplication = new AtomicLong();
    private final int replicationCapacity;

    /**
     * Creates an outbound channel.
     *
     * @param replicationCapacity the maximum number of pending replication
     *                            messages allowed per connection; {@code 0} means
     *                            unbounded (legacy behaviour, never drops)
     * @throws IllegalArgumentException if {@code replicationCapacity} is negative
     */
    OutboundChannel(int replicationCapacity) {
        if (replicationCapacity < 0) {
            throw new IllegalArgumentException("replicationCapacity must be >= 0");
        }
        this.replicationCapacity = replicationCapacity;
    }

    /**
     * Enqueues a message, applying the drop policy to replication traffic only.
     *
     * @param message the message to enqueue
     * @return {@code true} if the message was accepted; {@code false} if it was a
     *         replication message dropped due to backpressure
     */
    boolean enqueue(ClusterMessage message) {
        if (!isCountable(message.type())) {
            // Control / non-replication traffic is never bounded nor dropped.
            return queue.offer(message);
        }
        // Replication (data plane): always measured; dropped only when capped.
        if (replicationCapacity > 0 && pendingReplication.get() >= replicationCapacity) {
            droppedReplication.incrementAndGet();
            return false; // follower recovers via gap/snapshot catch-up
        }
        pendingReplication.incrementAndGet();
        if (!queue.offer(message)) {
            // The backing queue is unbounded, so offer never fails; account
            // defensively as a drop to keep the metric consistent.
            pendingReplication.decrementAndGet();
            droppedReplication.incrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Polls the next message, waiting up to the given timeout. Replication depth
     * is decremented as soon as a replication message leaves the queue (before it
     * is written to the socket), so the metric reflects "pending in queue" and
     * never leaks on a write-error path.
     *
     * @param timeout the maximum time to wait
     * @param unit    the unit of {@code timeout}
     * @return the next message, or {@code null} if the timeout elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    ClusterMessage poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClusterMessage message = queue.poll(timeout, unit);
        if (message != null && isCountable(message.type())) {
            pendingReplication.decrementAndGet();
        }
        return message;
    }

    /**
     * Returns the current number of replication messages pending in the queue.
     *
     * @return the replication queue depth (occupancy metric, RF3)
     */
    int dataDepth() {
        return pendingReplication.get();
    }

    /**
     * Returns the cumulative number of replication messages dropped by
     * backpressure since this channel was created.
     *
     * @return the dropped replication count
     */
    long droppedCount() {
        return droppedReplication.get();
    }

    /**
     * Returns the configured replication capacity ({@code 0} = unbounded).
     *
     * @return the replication capacity
     */
    int capacity() {
        return replicationCapacity;
    }

    private static boolean isCountable(MessageType type) {
        return type == MessageType.REPLICATION_REQUEST;
    }
}
