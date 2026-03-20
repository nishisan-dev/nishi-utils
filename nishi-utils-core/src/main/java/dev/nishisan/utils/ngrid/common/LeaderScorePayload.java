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

package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Payload carrying leader score metrics for reelection evaluation.
 */
public final class LeaderScorePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final NodeId nodeId;
    private final double writeRate;
    private final long epochMilli;

    /**
     * Creates a leader score payload.
     *
     * @param nodeId     the node ID
     * @param writeRate  the write rate
     * @param epochMilli the timestamp
     */
    public LeaderScorePayload(NodeId nodeId, double writeRate, long epochMilli) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.writeRate = writeRate;
        this.epochMilli = epochMilli;
    }

    /**
     * Returns the node ID.
     *
     * @return the node ID
     */
    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * Returns the write rate.
     *
     * @return the write rate
     */
    public double writeRate() {
        return writeRate;
    }

    /**
     * Returns the timestamp.
     *
     * @return the epoch millis
     */
    public long epochMilli() {
        return epochMilli;
    }
}
