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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Application-facing hooks for the orchestrated automatic affinity handback (issue tems#9, D11).
 * The embedding application (e.g. Cardinal, whose authoritative state is fed by an external Kafka
 * firehose) implements this to participate in the stop-the-world choreography that the replication
 * library drives: the interim leader stops consuming and flushes its in-flight pipe so the frozen
 * snapshot is consistent, and the returning leader resumes consuming only after it has cut over.
 *
 * <p>All methods have a no-op default so backends that do not gate an external input source are
 * unaffected. Callbacks are invoked on library worker/scheduler threads — keep them prompt;
 * {@link #onHandoverPrepare()} may block (bounded) while it drains, as that blocking IS the
 * "production frozen, snapshot ready" signal back to the library.</p>
 */
public interface HandoverListener {

    /**
     * Invoked on the INTERIM LEADER when a handback request has been accepted and production has
     * been frozen at the handover watermark. The application must stop consuming its external input
     * and drain any in-flight work so that every already-ingested item has produced its op-log delta
     * (≤ the frozen watermark). Returning from this method is the synchronous "flushed and ready"
     * signal: the library then serves the full snapshot. Throwing aborts the handback.
     */
    default void onHandoverPrepare() {
    }

    /**
     * Invoked on the (former) INTERIM LEADER once leadership has been handed to {@code newLeader}.
     * This node is now a follower; the application should ensure its external consumer stays
     * stopped (it is no longer the leader).
     *
     * @param newLeader the node that took over leadership via the handback
     */
    default void onDemotedAfterHandover(NodeId newLeader) {
    }

    /**
     * Invoked on the CANDIDATE once it has installed the snapshot, cut over, and asserted leadership.
     * The normal leadership-activation path (drain gate → resume) also runs; this is an explicit
     * completion signal for observability/metrics.
     */
    default void onPromotionComplete() {
    }

    /**
     * Invoked when an in-flight handback is aborted (timeout, candidate left, flush failure). On the
     * interim leader the application should resume consuming — leadership was retained.
     *
     * @param reason a short human-readable abort reason
     */
    default void onHandoverAborted(String reason) {
    }
}
