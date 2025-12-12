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
import java.util.Objects;
import java.util.UUID;

/**
 * Response payload for RPC style commands executed by the leader.
 */
public final class ClientResponsePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID requestId;
    private final boolean success;
    private final Serializable body;
    private final String error;

    public ClientResponsePayload(UUID requestId, boolean success, Serializable body, String error) {
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

    public Serializable body() {
        return body;
    }

    public String error() {
        return error;
    }
}
