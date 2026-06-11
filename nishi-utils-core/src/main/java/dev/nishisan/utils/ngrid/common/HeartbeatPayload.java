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
 * <p>
 * Carries the sender's advertised watermark, its leader-epoch view and — issue tems#9, D10c —
 * whether the SENDER currently asserts leadership. The flag is what makes a dual-leader
 * OBSERVABLE: two nodes asserting it to each other trigger the deterministic affinity resolution.
 * Absent on the wire (older peers), it decodes as {@code false} on both the binary and the JSON
 * paths.
 */
public final class HeartbeatPayload {

    private final long epochMilli;
    private final long leaderHighWatermark;
    private final long leaderEpoch;
    private final boolean leader;

    @JsonCreator
    public HeartbeatPayload(
            @JsonProperty("epochMilli") long epochMilli,
            @JsonProperty("leaderHighWatermark") long leaderHighWatermark,
            @JsonProperty("leaderEpoch") long leaderEpoch,
            @JsonProperty("leader") boolean leader) {
        this.epochMilli = epochMilli;
        this.leaderHighWatermark = leaderHighWatermark;
        this.leaderEpoch = leaderEpoch;
        this.leader = leader;
    }

    public HeartbeatPayload(long epochMilli, long leaderHighWatermark, long leaderEpoch) {
        this(epochMilli, leaderHighWatermark, leaderEpoch, false);
    }

    public static HeartbeatPayload now(long leaderHighWatermark, long leaderEpoch, boolean leader) {
        return new HeartbeatPayload(Instant.now().toEpochMilli(), leaderHighWatermark, leaderEpoch, leader);
    }

    public static HeartbeatPayload now(long leaderHighWatermark, long leaderEpoch) {
        return now(leaderHighWatermark, leaderEpoch, false);
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

    /** True when the SENDER asserted leadership at send time (issue tems#9, D10c). */
    public boolean leader() {
        return leader;
    }
}
