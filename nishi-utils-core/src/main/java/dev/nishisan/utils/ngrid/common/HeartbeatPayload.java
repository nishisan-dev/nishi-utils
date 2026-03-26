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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Simple heartbeat payload carrying a timestamp from the sender.
 */
public final class HeartbeatPayload {

    private final long epochMilli;
    private final long leaderHighWatermark;
    private final long leaderEpoch;

    @JsonCreator
    public HeartbeatPayload(
            @JsonProperty("epochMilli") long epochMilli,
            @JsonProperty("leaderHighWatermark") long leaderHighWatermark,
            @JsonProperty("leaderEpoch") long leaderEpoch) {
        this.epochMilli = epochMilli;
        this.leaderHighWatermark = leaderHighWatermark;
        this.leaderEpoch = leaderEpoch;
    }

    public static HeartbeatPayload now(long leaderHighWatermark, long leaderEpoch) {
        return new HeartbeatPayload(Instant.now().toEpochMilli(), leaderHighWatermark, leaderEpoch);
    }

    public static HeartbeatPayload now() {
        return new HeartbeatPayload(Instant.now().toEpochMilli(), -1L, 0L);
    }

    public long epochMilli() {
        return epochMilli;
    }

    public long leaderHighWatermark() {
        return leaderHighWatermark;
    }

    public long leaderEpoch() {
        return leaderEpoch;
    }
}
