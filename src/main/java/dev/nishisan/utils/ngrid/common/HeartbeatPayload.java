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
import java.time.Instant;

/**
 * Simple heartbeat payload carrying a timestamp from the sender.
 */
public final class HeartbeatPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 3L;

    private final long epochMilli;
    private final long leaderHighWatermark;
    private final long leaderEpoch;

    public HeartbeatPayload(long epochMilli, long leaderHighWatermark, long leaderEpoch) {
        this.epochMilli = epochMilli;
        this.leaderHighWatermark = leaderHighWatermark;
        this.leaderEpoch = leaderEpoch;
    }

    public static HeartbeatPayload now(long leaderHighWatermark, long leaderEpoch) {
        return new HeartbeatPayload(Instant.now().toEpochMilli(), leaderHighWatermark, leaderEpoch);
    }

    /**
     * Legacy factory for backward compatibility or non-leader heartbeats.
     */
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
