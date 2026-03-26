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

package dev.nishisan.utils.ngrid.structures;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.nishisan.utils.queue.NQueueHeaders;

import java.util.Objects;

/**
 * Envelope that carries the routing key, headers, and value
 * when a follower forwards an {@code offer} to the cluster leader.
 * <p>
 * Without this wrapper the follower path in {@link DistributedQueue#offer}
 * would lose the key and headers, causing metadata to be silently dropped.
 *
 * @param <T> the queue element type
 */
public final class OfferPayload<T> {

    private final byte[] key;
    private final NQueueHeaders headers;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final T value;

    @JsonCreator
    public OfferPayload(
            @JsonProperty("key") byte[] key,
            @JsonProperty("headers") NQueueHeaders headers,
            @JsonProperty("value") T value) {
        this.key = key;
        this.headers = Objects.requireNonNull(headers, "headers");
        this.value = Objects.requireNonNull(value, "value");
    }

    /** Returns the routing/partitioning key, or {@code null} if absent. */
    public byte[] key() {
        return key;
    }

    /** Returns the record headers; never {@code null}. */
    public NQueueHeaders headers() {
        return headers;
    }

    /** Returns the element value; never {@code null}. */
    public T value() {
        return value;
    }
}
