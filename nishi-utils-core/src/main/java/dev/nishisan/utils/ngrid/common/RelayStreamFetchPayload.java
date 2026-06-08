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

/**
 * Follower → leader request to pull the next contiguous run of the op-log in RELAY_STREAM mode.
 *
 * <p>The follower drives replication by its own durable cursor: {@code fromSequence} is the next
 * sequence it has not yet persisted to its relay, and the leader answers with a strictly contiguous
 * run starting there (see {@link RelayStreamBatchPayload}). Re-sending the same {@code fromSequence}
 * is idempotent because the cursor only advances after a durable append.
 *
 * @param topic        the replication topic
 * @param fromSequence the next sequence the follower wants (inclusive)
 * @param maxBatch     the maximum number of entries the follower is willing to receive in one batch
 */
public record RelayStreamFetchPayload(
                String topic,
                long fromSequence,
                int maxBatch) {
}
