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

/**
 * Either side → other cancellation of an in-flight orchestrated affinity handback
 * (issue tems#9, D11). On the interim leader it un-freezes production and retains leadership
 * (no demotion happened → no dual-leader). On the candidate it clears the request and the node
 * stays a follower. Idempotent.
 */
public final class HandbackAbortPayload {

    private final String reason;

    @JsonCreator
    public HandbackAbortPayload(@JsonProperty("reason") String reason) {
        this.reason = reason == null ? "" : reason;
    }

    public String reason() {
        return reason;
    }
}
