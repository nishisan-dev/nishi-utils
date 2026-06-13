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
 * Candidate → interim leader request to begin an orchestrated affinity handback
 * (issue tems#9, D11). The candidate is the highest-affinity node returning to a
 * cluster that already has a healthy leader; instead of reclaiming via the fragile
 * watermark gates (which a lineage-blind counter offset can defeat), it asks the
 * incumbent to freeze production, serve a full snapshot, and hand leadership back.
 *
 * <p>The {@code candidateApplied} / {@code observedLeaderWatermark} fields are
 * advisory proof that the candidate is current/stable enough to take over; the
 * authoritative re-anchor happens via the snapshot cutover, not these counters.</p>
 */
public final class HandbackRequestPayload {

    private final NodeId candidate;
    private final long candidateApplied;
    private final long observedLeaderWatermark;

    @JsonCreator
    public HandbackRequestPayload(
            @JsonProperty("candidate") NodeId candidate,
            @JsonProperty("candidateApplied") long candidateApplied,
            @JsonProperty("observedLeaderWatermark") long observedLeaderWatermark) {
        this.candidate = Objects.requireNonNull(candidate, "candidate");
        this.candidateApplied = candidateApplied;
        this.observedLeaderWatermark = observedLeaderWatermark;
    }

    public NodeId candidate() {
        return candidate;
    }

    public long candidateApplied() {
        return candidateApplied;
    }

    public long observedLeaderWatermark() {
        return observedLeaderWatermark;
    }
}
