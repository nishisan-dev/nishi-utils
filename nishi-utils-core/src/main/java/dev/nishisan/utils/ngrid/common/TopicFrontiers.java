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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * <p>Two incomparable vectors (each ahead on some topic) are ordered deterministically so that every
 * node ranks the same pair identically: by {@link #total()} first, then topic by topic in
 * <em>priority order</em> — the caller's priority list first (e.g. the ngrrd catalog before its
 * status topic), the remaining topics by name. The first topic on which they differ decides. This
 * is a convention for choosing which lineage survives when both hold an unreplicated op; it never
 * overrides dominance.
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
     * are incomparable and {@code other} wins the deterministic tie-break (larger total, then the
     * first differing topic in priority order — both sides compute it identically, so exactly one
     * of the two nodes yields).
     */
    public boolean isBehind(TopicFrontiers other, long threshold) {
        return isBehind(other, threshold, List.of());
    }

    /** {@link #isBehind(TopicFrontiers, long)} with an explicit topic priority list for the tie-break. */
    public boolean isBehind(TopicFrontiers other, long threshold, List<String> priorityTopics) {
        Comparison c = compare(other, threshold);
        return c == Comparison.BEHIND
                || (c == Comparison.INCOMPARABLE && tieBreak(other, threshold, priorityTopics) < 0);
    }

    /** Mirror of {@link #isBehind}: true when this vector holds state {@code other} lacks. */
    public boolean isAhead(TopicFrontiers other, long threshold) {
        return isAhead(other, threshold, List.of());
    }

    /** {@link #isAhead(TopicFrontiers, long)} with an explicit topic priority list for the tie-break. */
    public boolean isAhead(TopicFrontiers other, long threshold, List<String> priorityTopics) {
        Comparison c = compare(other, threshold);
        return c == Comparison.AHEAD
                || (c == Comparison.INCOMPARABLE && tieBreak(other, threshold, priorityTopics) > 0);
    }

    /**
     * Deterministic order between two INCOMPARABLE vectors: larger total wins; on equal totals the
     * first topic (priority list first, then the rest by name) on which they differ beyond the
     * threshold wins for the side holding the higher frontier. {@code 0} only when nothing differs.
     */
    private int tieBreak(TopicFrontiers other, long threshold, List<String> priorityTopics) {
        int byTotal = Long.compare(total(), other.total());
        if (byTotal != 0) {
            return byTotal;
        }
        long tolerance = Math.max(0L, threshold);
        Set<String> ordered = new LinkedHashSet<>();
        if (priorityTopics != null) {
            ordered.addAll(priorityTopics);
        }
        ordered.addAll(byTopic.keySet());
        ordered.addAll(other.byTopic.keySet());
        for (String topic : ordered) {
            long mine = frontier(topic);
            long theirs = other.frontier(topic);
            if (mine > theirs + tolerance) {
                return 1;
            }
            if (theirs > mine + tolerance) {
                return -1;
            }
        }
        return 0;
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
