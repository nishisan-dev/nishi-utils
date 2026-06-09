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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Outbound message buffer for a single transport connection.
 *
 * <p>
 * Since 5.0.0 (RELAY_STREAM, the definitive replication model) the leader no longer pushes
 * replication traffic: followers PULL the durable op-log as a sequential stream and the leader
 * commits asynchronously on its own binlog. There is therefore no unbounded best-effort data plane
 * to bound, and the legacy per-connection replication drop policy (which dropped excess
 * {@code REPLICATION_REQUEST} bursts and let the follower recover via gap/snapshot catch-up) was
 * removed along with the push protocol. This channel is now a plain unbounded outbound queue.
 * </p>
 *
 * <p>
 * The depth/dropped/capacity accessors are retained as stable {@code 0} values so the transport's
 * observability surface (occupancy and drop metrics) keeps compiling and reporting a meaningful
 * "no drops" view.
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

    /**
     * Creates an outbound channel. The {@code replicationCapacity} argument is accepted for source
     * compatibility but no longer has any effect (the replication drop policy was removed with the
     * push protocol in 5.0.0).
     *
     * @param replicationCapacity ignored; must be {@code >= 0}
     * @throws IllegalArgumentException if {@code replicationCapacity} is negative
     */
    OutboundChannel(int replicationCapacity) {
        if (replicationCapacity < 0) {
            throw new IllegalArgumentException("replicationCapacity must be >= 0");
        }
    }

    /**
     * Enqueues a message. The backing queue is unbounded, so this always accepts.
     *
     * @param message the message to enqueue
     * @return {@code true} (always)
     */
    boolean enqueue(ClusterMessage message) {
        return queue.offer(message);
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
        return queue.poll(timeout, unit);
    }

    /**
     * Returns the replication queue depth. Always {@code 0} since the push data plane was removed.
     *
     * @return {@code 0}
     */
    int dataDepth() {
        return 0;
    }

    /**
     * Returns the cumulative number of replication messages dropped by backpressure. Always
     * {@code 0} since the drop policy was removed.
     *
     * @return {@code 0}
     */
    long droppedCount() {
        return 0L;
    }

    /**
     * Returns the configured replication capacity. Always {@code 0} (unbounded) since the drop
     * policy was removed.
     *
     * @return {@code 0}
     */
    int capacity() {
        return 0;
    }
}
