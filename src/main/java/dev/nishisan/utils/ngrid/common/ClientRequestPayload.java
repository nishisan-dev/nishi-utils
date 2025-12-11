/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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
import java.util.Objects;
import java.util.UUID;

/**
 * Simple RPC style payload for client requests routed to the cluster leader.
 */
public final class ClientRequestPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID requestId;
    private final String command;
    private final Serializable body;

    public ClientRequestPayload(UUID requestId, String command, Serializable body) {
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

    public Serializable body() {
        return body;
    }
}
