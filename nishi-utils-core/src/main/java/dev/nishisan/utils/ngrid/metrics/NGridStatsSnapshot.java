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

package dev.nishisan.utils.ngrid.metrics;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of NGrid runtime metrics.
 */
public final class NGridStatsSnapshot {
    private final Instant capturedAt;
    private final Map<String, Long> writesByNode;
    private final Map<String, Long> ingressWritesByNode;
    private final Map<String, Long> queueOffersByNode;
    private final Map<String, Long> queuePollsByNode;
    private final Map<String, Map<String, Long>> mapPutsByName;
    private final Map<String, Map<String, Long>> mapRemovesByName;
    private final Map<String, Double> rttMsByNode;
    private final Map<String, Long> rttFailuresByNode;

    /**
     * Creates a new stats snapshot.
     *
     * @param capturedAt          capture timestamp
     * @param writesByNode        writes per node
     * @param ingressWritesByNode ingress writes per node
     * @param queueOffersByNode   queue offers per node
     * @param queuePollsByNode    queue polls per node
     * @param mapPutsByName       map puts by map name and node
     * @param mapRemovesByName    map removes by map name and node
     * @param rttMsByNode         RTT in ms per node
     * @param rttFailuresByNode   RTT failures per node
     */
    public NGridStatsSnapshot(Instant capturedAt,
            Map<String, Long> writesByNode,
            Map<String, Long> ingressWritesByNode,
            Map<String, Long> queueOffersByNode,
            Map<String, Long> queuePollsByNode,
            Map<String, Map<String, Long>> mapPutsByName,
            Map<String, Map<String, Long>> mapRemovesByName,
            Map<String, Double> rttMsByNode,
            Map<String, Long> rttFailuresByNode) {
        this.capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
        this.writesByNode = Map.copyOf(Objects.requireNonNull(writesByNode, "writesByNode"));
        this.ingressWritesByNode = Map.copyOf(Objects.requireNonNull(ingressWritesByNode, "ingressWritesByNode"));
        this.queueOffersByNode = Map.copyOf(Objects.requireNonNull(queueOffersByNode, "queueOffersByNode"));
        this.queuePollsByNode = Map.copyOf(Objects.requireNonNull(queuePollsByNode, "queuePollsByNode"));
        this.mapPutsByName = copyNested(mapPutsByName, "mapPutsByName");
        this.mapRemovesByName = copyNested(mapRemovesByName, "mapRemovesByName");
        this.rttMsByNode = Map.copyOf(Objects.requireNonNull(rttMsByNode, "rttMsByNode"));
        this.rttFailuresByNode = Map.copyOf(Objects.requireNonNull(rttFailuresByNode, "rttFailuresByNode"));
    }

    /**
     * Returns the capture timestamp.
     * 
     * @return the timestamp
     */
    public Instant capturedAt() {
        return capturedAt;
    }

    /**
     * Returns writes per node.
     * 
     * @return the map
     */
    public Map<String, Long> writesByNode() {
        return writesByNode;
    }

    /**
     * Returns ingress writes per node.
     * 
     * @return the map
     */
    public Map<String, Long> ingressWritesByNode() {
        return ingressWritesByNode;
    }

    /**
     * Returns queue offers per node.
     * 
     * @return the map
     */
    public Map<String, Long> queueOffersByNode() {
        return queueOffersByNode;
    }

    /**
     * Returns queue polls per node.
     * 
     * @return the map
     */
    public Map<String, Long> queuePollsByNode() {
        return queuePollsByNode;
    }

    /**
     * Returns map puts by map name.
     * 
     * @return the nested map
     */
    public Map<String, Map<String, Long>> mapPutsByName() {
        return mapPutsByName;
    }

    /**
     * Returns map removes by map name.
     * 
     * @return the nested map
     */
    public Map<String, Map<String, Long>> mapRemovesByName() {
        return mapRemovesByName;
    }

    /**
     * Returns RTT in ms per node.
     * 
     * @return the map
     */
    public Map<String, Double> rttMsByNode() {
        return rttMsByNode;
    }

    /**
     * Returns RTT failures per node.
     * 
     * @return the map
     */
    public Map<String, Long> rttFailuresByNode() {
        return rttFailuresByNode;
    }

    private static Map<String, Map<String, Long>> copyNested(Map<String, Map<String, Long>> map, String name) {
        Objects.requireNonNull(map, name);
        return map.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> Map.copyOf(entry.getValue())));
    }
}
