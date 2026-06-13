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

import java.util.Objects;

/**
 * Interim leader → candidate grant of an orchestrated affinity handback (issue tems#9, D11).
 * Sent once the incumbent has entered PREP: production is frozen at {@code frozenWatermark},
 * the application has flushed its in-flight pipe, and the candidate may now pull a full snapshot
 * (via the existing {@code SYNC_REQUEST} path) and cut over.
 *
 * <p>{@code leaderEpoch} is the incumbent's current leader epoch — the candidate asserts
 * leadership at an epoch strictly above it so the demotion fences cleanly with no epoch ladder.
 * {@code topic} is the representative topic the leader serves; the candidate bootstraps every
 * topic it follows.</p>
 */
public final class HandbackGrantPayload {

    private final String topic;
    private final long frozenWatermark;
    private final long leaderEpoch;

    @JsonCreator
    public HandbackGrantPayload(
            @JsonProperty("topic") String topic,
            @JsonProperty("frozenWatermark") long frozenWatermark,
            @JsonProperty("leaderEpoch") long leaderEpoch) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.frozenWatermark = frozenWatermark;
        this.leaderEpoch = leaderEpoch;
    }

    public String topic() {
        return topic;
    }

    public long frozenWatermark() {
        return frozenWatermark;
    }

    public long leaderEpoch() {
        return leaderEpoch;
    }
}
