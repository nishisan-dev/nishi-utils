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
 * Response payload for RPC style commands executed by the leader.
 */
public final class ClientResponsePayload {

    private final UUID requestId;
    private final boolean success;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object body;
    private final String error;

    @JsonCreator
    public ClientResponsePayload(
            @JsonProperty("requestId") UUID requestId,
            @JsonProperty("success") boolean success,
            @JsonProperty("body") Object body,
            @JsonProperty("error") String error) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.success = success;
        this.body = body;
        this.error = error;
    }

    public UUID requestId() {
        return requestId;
    }

    public boolean success() {
        return success;
    }

    public Object body() {
        return body;
    }

    public String error() {
        return error;
    }
}
