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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

/**
 * Response carrying a state snapshot for synchronization.
 * The sequence refers to the last applied sequence for the specific topic.
 */
public final class SyncResponsePayload {

    private final String topic;
    private final long sequence;
    private final int chunkIndex;
    private final boolean hasMore;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object data;

    public SyncResponsePayload(String topic, long sequence, Object data) {
        this(topic, sequence, 0, false, data);
    }

    @JsonCreator
    public SyncResponsePayload(
            @JsonProperty("topic") String topic,
            @JsonProperty("sequence") long sequence,
            @JsonProperty("chunkIndex") int chunkIndex,
            @JsonProperty("hasMore") boolean hasMore,
            @JsonProperty("data") Object data) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.sequence = sequence;
        this.chunkIndex = chunkIndex;
        this.hasMore = hasMore;
        this.data = data;
    }

    public String topic() {
        return topic;
    }

    public long sequence() {
        return sequence;
    }

    public int chunkIndex() {
        return chunkIndex;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public Object data() {
        return data;
    }
}
