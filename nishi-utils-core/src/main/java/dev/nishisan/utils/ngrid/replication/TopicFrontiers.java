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

package dev.nishisan.utils.ngrid.replication;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable vector of applied frontiers, one per replication topic (issue #178).
 *
 * <p>Each entry is the highest sequence of that topic this node has durably applied (or, on the
 * leader, produced): {@code nextExpected - 1}. Two nodes are compared topic by topic, never through
 * a single aggregated counter — an aggregate cannot tell "missing the last catalog op" from "one
 * extra status op", which is exactly what let a follower behind on {@code map:ngrrd.catalog} be
 * elected ahead of a peer that held that op.
 *
 * <p>{@link #total()} (the sum of all frontiers) is the ONLY scalar derived from the vector: it is
 * the same number for a leader and for a fully caught-up follower and is used for observability
 * and for peers that predate the vector (rolling upgrade). It is never used to rank two nodes that
 * both advertise a vector unless their vectors are {@link Comparison#INCOMPARABLE incomparable}.
 *
 * <p>Synthetic sequence-state keys ({@code _global}, {@code _topic:*}) are never part of a vector.
 *
 * @param byTopic frontier per topic; topics absent from the map have frontier {@code 0}
 */
public record TopicFrontiers(Map<String, Long> byTopic) {

    /** The empty vector: no topic known, total {@code 0}. Also what a peer without vector support advertises. */
    public static final TopicFrontiers EMPTY = new TopicFrontiers(Map.of());

    /** Outcome of comparing this vector against another one. */
    public enum Comparison {
        /** Strictly ahead on at least one topic and not behind on any. */
        AHEAD,
        /** Strictly behind on at least one topic and not ahead on any. */
        BEHIND,
        /** Within the tolerance on every topic. */
        EQUAL,
        /** Ahead on some topic and behind on another: no lineage dominates the other. */
        INCOMPARABLE
    }

    public TopicFrontiers {
        Objects.requireNonNull(byTopic, "byTopic");
        TreeMap<String, Long> copy = new TreeMap<>();
        byTopic.forEach((topic, frontier) -> {
            if (topic == null || frontier == null || isSyntheticKey(topic) || frontier <= 0L) {
                return;
            }
            copy.put(topic, frontier);
        });
        byTopic = Collections.unmodifiableMap(copy);
    }

    /** Builds a vector from a map (copied; synthetic keys and non-positive frontiers dropped). */
    public static TopicFrontiers of(Map<String, Long> byTopic) {
        return byTopic == null || byTopic.isEmpty() ? EMPTY : new TopicFrontiers(byTopic);
    }

    /** True for the internal sequence-state keys that are not topics. */
    public static boolean isSyntheticKey(String key) {
        return "_global".equals(key) || key.startsWith("_topic:");
    }

    /** Frontier for a topic, {@code 0} when the topic is unknown to this vector. */
    public long frontier(String topic) {
        Long value = byTopic.get(topic);
        return value == null ? 0L : value;
    }

    /** True when no topic has a positive frontier. */
    public boolean isEmpty() {
        return byTopic.isEmpty();
    }

    /** Sum of all frontiers: the total number of applied operations across topics. */
    public long total() {
        long sum = 0L;
        for (long value : byTopic.values()) {
            sum += value;
        }
        return sum;
    }

    /**
     * Compares this vector with {@code other} topic by topic over the union of their topics. A topic
     * counts as "ahead" when this frontier exceeds the other's by more than {@code threshold}, as
     * "behind" in the symmetric case, and as equal otherwise.
     *
     * @param other     the other vector (never null)
     * @param threshold non-negative tolerance applied per topic (the join/reclaim lag threshold)
     * @return the comparison outcome
     */
    public Comparison compare(TopicFrontiers other, long threshold) {
        Objects.requireNonNull(other, "other");
        long tolerance = Math.max(0L, threshold);
        boolean ahead = false;
        boolean behind = false;
        TreeMap<String, Boolean> topics = new TreeMap<>();
        byTopic.keySet().forEach(t -> topics.put(t, Boolean.TRUE));
        other.byTopic.keySet().forEach(t -> topics.put(t, Boolean.TRUE));
        for (String topic : topics.keySet()) {
            long mine = frontier(topic);
            long theirs = other.frontier(topic);
            if (mine > theirs + tolerance) {
                ahead = true;
            } else if (theirs > mine + tolerance) {
                behind = true;
            }
        }
        if (ahead && behind) {
            return Comparison.INCOMPARABLE;
        }
        if (ahead) {
            return Comparison.AHEAD;
        }
        if (behind) {
            return Comparison.BEHIND;
        }
        return Comparison.EQUAL;
    }

    /**
     * True when {@code other} holds state this node lacks: it dominates this vector, or the vectors
     * are incomparable and {@code other} has the larger total (the deterministic tie-break both sides
     * compute identically, so exactly one of the two nodes yields).
     */
    public boolean isBehind(TopicFrontiers other, long threshold) {
        Comparison c = compare(other, threshold);
        return c == Comparison.BEHIND
                || (c == Comparison.INCOMPARABLE && total() < other.total());
    }

    /** Mirror of {@link #isBehind}: true when this vector holds state {@code other} lacks. */
    public boolean isAhead(TopicFrontiers other, long threshold) {
        Comparison c = compare(other, threshold);
        return c == Comparison.AHEAD
                || (c == Comparison.INCOMPARABLE && total() > other.total());
    }

    /**
     * Human-readable list of the topics on which the two vectors differ beyond the threshold, e.g.
     * {@code map:ngrrd.catalog=9<10}. Empty when equal.
     */
    public String describeDivergence(TopicFrontiers other, long threshold) {
        Objects.requireNonNull(other, "other");
        long tolerance = Math.max(0L, threshold);
        TreeMap<String, Boolean> topics = new TreeMap<>();
        byTopic.keySet().forEach(t -> topics.put(t, Boolean.TRUE));
        other.byTopic.keySet().forEach(t -> topics.put(t, Boolean.TRUE));
        StringBuilder sb = new StringBuilder();
        for (String topic : topics.keySet()) {
            long mine = frontier(topic);
            long theirs = other.frontier(topic);
            if (Math.abs(mine - theirs) > tolerance) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(topic).append('=').append(mine).append(mine < theirs ? '<' : '>').append(theirs);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return byTopic.toString();
    }
}
