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
 * Request for state synchronization.
 */
public final class SyncRequestPayload {

    private final String topic;
    private final int chunkIndex;
    private final boolean allowFollowerResponse;

    public SyncRequestPayload(String topic) {
        this(topic, 0, false);
    }

    public SyncRequestPayload(String topic, int chunkIndex) {
        this(topic, chunkIndex, false);
    }

    @JsonCreator
    public SyncRequestPayload(
            @JsonProperty("topic") String topic,
            @JsonProperty("chunkIndex") int chunkIndex,
            @JsonProperty("allowFollowerResponse") boolean allowFollowerResponse) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.chunkIndex = chunkIndex;
        this.allowFollowerResponse = allowFollowerResponse;
    }

    public String topic() {
        return topic;
    }

    public int chunkIndex() {
        return chunkIndex;
    }

    public boolean allowFollowerResponse() {
        return allowFollowerResponse;
    }
}
