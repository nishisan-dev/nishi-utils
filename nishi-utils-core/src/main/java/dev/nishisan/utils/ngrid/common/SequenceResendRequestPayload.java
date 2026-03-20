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

import java.io.Serializable;

/**
 * Payload for requesting missing replication sequences from the leader.
 *
 * @param topic        the replication topic
 * @param fromSequence the start of the missing range (inclusive)
 * @param toSequence   the end of the missing range (inclusive)
 */
public record SequenceResendRequestPayload(
                String topic,
                long fromSequence,
                long toSequence) implements Serializable {
}
