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
 * Simple RPC style payload for client requests routed to the cluster leader.
 */
public final class ClientRequestPayload {

    private final UUID requestId;
    private final String command;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object body;

    @JsonCreator
    public ClientRequestPayload(
            @JsonProperty("requestId") UUID requestId,
            @JsonProperty("command") String command,
            @JsonProperty("body") Object body) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.command = Objects.requireNonNull(command, "command");
        this.body = body;
    }

    public UUID requestId() {
        return requestId;
    }

    public String command() {
        return command;
    }

    public Object body() {
        return body;
    }
}
