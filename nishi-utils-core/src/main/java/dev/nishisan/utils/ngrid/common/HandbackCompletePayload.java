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
 * Candidate → interim leader confirmation that the orchestrated affinity handback completed
 * (issue tems#9, D11): the candidate installed the full snapshot, cut over its counters to
 * {@code cutoverWatermark} (SET — lineage offset zeroed), and asserted leadership at
 * {@code newEpoch} (strictly above the granted epoch). On receipt the interim leader demotes
 * cleanly to follower. The authoritative demotion also happens via epoch convergence from the
 * candidate's heartbeats; this message is the prompt, low-latency signal.
 */
public final class HandbackCompletePayload {

    private final long cutoverWatermark;
    private final long newEpoch;

    @JsonCreator
    public HandbackCompletePayload(
            @JsonProperty("cutoverWatermark") long cutoverWatermark,
            @JsonProperty("newEpoch") long newEpoch) {
        this.cutoverWatermark = cutoverWatermark;
        this.newEpoch = newEpoch;
    }

    public long cutoverWatermark() {
        return cutoverWatermark;
    }

    public long newEpoch() {
        return newEpoch;
    }
}
