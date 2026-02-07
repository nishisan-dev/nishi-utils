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
 * Payload suggesting a leader change based on observed metrics.
 */
public final class LeaderSuggestionPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final NodeId leaderId;
    private final double writeRate;
    private final long epochMilli;

    /**
     * Creates a leader suggestion payload.
     *
     * @param leaderId   the suggested leader ID
     * @param writeRate  the write rate
     * @param epochMilli the timestamp
     */
    public LeaderSuggestionPayload(NodeId leaderId, double writeRate, long epochMilli) {
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        this.writeRate = writeRate;
        this.epochMilli = epochMilli;
    }

    /**
     * Returns the suggested leader ID.
     *
     * @return the leader ID
     */
    public NodeId leaderId() {
        return leaderId;
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
