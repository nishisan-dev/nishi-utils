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
import java.util.UUID;

/**
 * Encapsulates an operation that must be replicated across cluster members.
 */
public final class ReplicationPayload {

    private final UUID operationId;
    private final long sequence;
    private final long epoch;
    private final String topic;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object data;

    @JsonCreator
    public ReplicationPayload(
            @JsonProperty("operationId") UUID operationId,
            @JsonProperty("sequence") long sequence,
            @JsonProperty("epoch") long epoch,
            @JsonProperty("topic") String topic,
            @JsonProperty("data") Object data) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.sequence = sequence;
        this.epoch = epoch;
        this.topic = Objects.requireNonNull(topic, "topic");
        this.data = Objects.requireNonNull(data, "data");
    }

    public UUID operationId() {
        return operationId;
    }

    public long sequence() {
        return sequence;
    }

    public long epoch() {
        return epoch;
    }

    public String topic() {
        return topic;
    }

    public Object data() {
        return data;
    }
}
