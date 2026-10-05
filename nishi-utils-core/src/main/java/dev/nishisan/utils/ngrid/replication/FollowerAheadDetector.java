/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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

package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.common.NodeId;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Detects, per topic, a follower whose stream cursor stays persistently ABOVE the leader's high
 * watermark (8.10.1).
 *
 * <p>Within one lineage a follower never holds a sequence the leader does not have. When the leader's
 * {@code RELAY_STREAM_BATCH} advertises a {@code leaderHighWatermark} below the local cursor, the two
 * numberings have diverged (e.g. a snapshot labeled with a stale counter lowered the leader's
 * frontier). The follower then fetches from {@code cursor+1}, receives empty batches without
 * {@code needSnapshot} and, once the leader produces again, discards every operation with a sequence up
 * to the cursor as a duplicate — silently and with lag {@code 0}.
 *
 * <p>To not mistake that state for a transient lag, the trigger requires hysteresis: at least
 * {@code confirmations} consecutive responses from the SAME leader and term showing the condition, and
 * at least {@code minDurationMs} between the first and the last of them. Any response without the
 * condition, or from another leader or term, restarts the streak. After a trigger, the topic stays in
 * cooldown for {@code cooldownMs}.
 *
 * <p>Holds in-memory state only and does no I/O; safe to call from several threads (per-topic state is
 * updated atomically with {@link ConcurrentHashMap#compute}). The clock is passed on every call so
 * tests can control it.
 */
final class FollowerAheadDetector {

    /** Ongoing streak of a topic: leader, term, start and number of observations. */
    private record Streak(NodeId leader, long epoch, long firstSeenMs, int count) {
    }

    private final IntSupplier confirmations;
    private final LongSupplier minDurationMs;
    private final LongSupplier cooldownMs;
    private final Map<String, Streak> streaks = new ConcurrentHashMap<>();
    private final Map<String, Long> cooldownUntilMs = new ConcurrentHashMap<>();

    /**
     * @param confirmations minimum number of consecutive responses showing the condition (K)
     * @param minDurationMs minimum time the condition must persist (T), in milliseconds
     * @param cooldownMs    minimum spacing between two triggers for the same topic, in milliseconds
     */
    FollowerAheadDetector(IntSupplier confirmations, LongSupplier minDurationMs, LongSupplier cooldownMs) {
        this.confirmations = Objects.requireNonNull(confirmations, "confirmations");
        this.minDurationMs = Objects.requireNonNull(minDurationMs, "minDurationMs");
        this.cooldownMs = Objects.requireNonNull(cooldownMs, "cooldownMs");
    }

    /**
     * Records one stream response from the leader and tells whether the bootstrap must be armed now.
     *
     * @param topic     topic of the response
     * @param leader    leader that answered (its identity was already validated by the caller)
     * @param epoch     leader term as observed by the follower
     * @param cursor    local stream cursor (highest sequence persisted to the relay)
     * @param leaderHwm high watermark advertised by the leader; negative means unknown (e.g. the leader
     *                  is still draining its relay after promotion) and ends the streak
     * @param nowMs     wall clock, in milliseconds
     * @return {@code true} when the condition persisted long enough and the topic is not in cooldown
     */
    boolean observe(String topic, NodeId leader, long epoch, long cursor, long leaderHwm, long nowMs) {
        if (topic == null || leader == null || leaderHwm < 0L || cursor <= leaderHwm) {
            if (topic != null) {
                streaks.remove(topic);
            }
            return false;
        }
        Streak streak = streaks.compute(topic, (t, current) -> {
            if (current == null || !current.leader().equals(leader) || current.epoch() != epoch) {
                return new Streak(leader, epoch, nowMs, 1);
            }
            return new Streak(current.leader(), current.epoch(), current.firstSeenMs(), current.count() + 1);
        });
        if (streak.count() < Math.max(1, confirmations.getAsInt())
                || nowMs - streak.firstSeenMs() < Math.max(0L, minDurationMs.getAsLong())) {
            return false;
        }
        Long until = cooldownUntilMs.get(topic);
        if (until != null && nowMs < until) {
            return false;
        }
        cooldownUntilMs.put(topic, nowMs + Math.max(0L, cooldownMs.getAsLong()));
        streaks.remove(topic);
        return true;
    }

    /**
     * Ends the topic's streak (e.g. a batch carried data, or the topic entered a bootstrap through
     * another path). Leaves the cooldown untouched.
     *
     * @param topic topic
     */
    void reset(String topic) {
        if (topic != null) {
            streaks.remove(topic);
        }
    }

    /**
     * Number of observations in the topic's ongoing streak ({@code 0} without one). For tests and
     * diagnostics.
     *
     * @param topic topic
     * @return the current streak length
     */
    int streakLength(String topic) {
        Streak streak = streaks.get(topic);
        return streak == null ? 0 : streak.count();
    }
}
