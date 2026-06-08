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

import java.util.List;

/**
 * Leader → follower response to a {@link RelayStreamFetchPayload}: a strictly contiguous run of the
 * op-log starting at {@code fromSequence}, in RELAY_STREAM mode.
 *
 * <p>{@code frames} are {@code RelayEntryCodec}-encoded entries (the same compact frame the relay
 * stores), ascending and contiguous from {@code fromSequence}; the leader truncates the run at the
 * first hole so the follower can persist them in order with no gap. The list is empty when the
 * follower is already caught up ({@code fromSequence > leaderHighWatermark}) or when a snapshot is
 * required.
 *
 * <p>{@code needSnapshot} is {@code true} when {@code fromSequence} is below the leader's retained
 * window ({@code fromSequence < oldestSequence}): the follower is too far behind to stream and must
 * bootstrap from a snapshot before resuming. {@code leaderHighWatermark} and {@code oldestSequence}
 * are advisory, for follower lag/floor metrics.
 *
 * @param topic               the replication topic
 * @param fromSequence        the first sequence of the run (equals the request's fromSequence)
 * @param frames              contiguous, ascending {@code RelayEntryCodec} frames; may be empty
 * @param leaderHighWatermark the highest committed sequence the leader holds for the topic
 * @param oldestSequence      the lowest sequence still retained in the leader op-log ({@code -1} if empty)
 * @param needSnapshot        {@code true} if the follower must snapshot before it can stream
 */
public record RelayStreamBatchPayload(
                String topic,
                long fromSequence,
                List<byte[]> frames,
                long leaderHighWatermark,
                long oldestSequence,
                boolean needSnapshot) {
}
