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

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Metric keys used by NGrid components.
 */
public final class NGridMetrics {
    private static final String WRITE_NODE_PREFIX = "ngrid.write.node.";
    private static final String INGRESS_WRITE_PREFIX = "ngrid.ingress.write.node.";
    private static final String QUEUE_OFFER_PREFIX = "ngrid.queue.offer.node.";
    private static final String QUEUE_POLL_PREFIX = "ngrid.queue.poll.node.";
    private static final String MAP_PUT_PREFIX = "ngrid.map.put.";
    private static final String MAP_REMOVE_PREFIX = "ngrid.map.remove.";
    private static final String RTT_PREFIX = "ngrid.rtt.ms.node.";
    private static final String RTT_FAILURE_PREFIX = "ngrid.rtt.fail.node.";

    private NGridMetrics() {
    }

    /**
     * Returns the write metric key for a node.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String writeNode(NodeId nodeId) {
        return WRITE_NODE_PREFIX + nodeId.value();
    }

    /**
     * Returns the ingress write metric key.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String ingressWrite(NodeId nodeId) {
        return INGRESS_WRITE_PREFIX + nodeId.value();
    }

    /**
     * Returns the queue offer metric key.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String queueOffer(NodeId nodeId) {
        return QUEUE_OFFER_PREFIX + nodeId.value();
    }

    /**
     * Returns the queue poll metric key.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String queuePoll(NodeId nodeId) {
        return QUEUE_POLL_PREFIX + nodeId.value();
    }

    /**
     * Returns the map put metric key.
     * 
     * @param mapName the map
     * @param nodeId  the node
     * @return the metric key
     */
    public static String mapPut(String mapName, NodeId nodeId) {
        return MAP_PUT_PREFIX + mapName + ".node." + nodeId.value();
    }

    /**
     * Returns the map remove metric key.
     * 
     * @param mapName the map
     * @param nodeId  the node
     * @return the metric key
     */
    public static String mapRemove(String mapName, NodeId nodeId) {
        return MAP_REMOVE_PREFIX + mapName + ".node." + nodeId.value();
    }

    /**
     * Returns the RTT metric key.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String rttMs(NodeId nodeId) {
        return RTT_PREFIX + nodeId.value();
    }

    /**
     * Returns the RTT failure metric key.
     * 
     * @param nodeId the node
     * @return the metric key
     */
    public static String rttFailure(NodeId nodeId) {
        return RTT_FAILURE_PREFIX + nodeId.value();
    }
}
