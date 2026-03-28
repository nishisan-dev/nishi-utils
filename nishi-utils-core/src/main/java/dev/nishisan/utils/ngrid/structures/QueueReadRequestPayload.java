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

import java.util.Objects;

/**
 * RPC payload used for cursor-based queue reads.
 */
public final class QueueReadRequestPayload {
    private final String consumerKey;
    private final Long hintOffset;

    @JsonCreator
    public QueueReadRequestPayload(
            @JsonProperty("consumerKey") String consumerKey,
            @JsonProperty("hintOffset") Long hintOffset) {
        this.consumerKey = Objects.requireNonNull(consumerKey, "consumerKey");
        this.hintOffset = hintOffset;
    }

    public String consumerKey() {
        return consumerKey;
    }

    public Long hintOffset() {
        return hintOffset;
    }
}
