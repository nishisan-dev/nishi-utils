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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility methods for testing cluster behavior and stability.
 * <p>
 * This class provides helper methods to wait for cluster consensus across
 * multiple nodes, making integration tests more reliable and readable.
 */
public final class ClusterTestUtils {

    private ClusterTestUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Waits for cluster consensus across multiple nodes with a default timeout of
     * 20 seconds.
     * <p>
     * Validates that all nodes agree on:
     * - The cluster leader
     * - The number of active members (should equal number of nodes)
     * - Transport-level connectivity between all nodes
     *
     * @param nodes the nodes to check for consensus
     * @throws InterruptedException     if the thread is interrupted while waiting
     * @throws IllegalStateException    if consensus is not reached within the
     *                                  timeout
     * @throws IllegalArgumentException if no nodes are provided
     */
    public static void awaitClusterConsensus(NGridNode... nodes) throws InterruptedException {
        awaitClusterConsensus(Duration.ofSeconds(20), nodes);
    }

    /**
     * Waits for cluster consensus across multiple nodes with a specified timeout.
     * <p>
     * Validates that all nodes agree on:
     * - The cluster leader
     * - The number of active members (should equal number of nodes)
     * - Transport-level connectivity between all nodes
     *
     * @param timeout the maximum time to wait for consensus
     * @param nodes   the nodes to check for consensus
     * @throws InterruptedException     if the thread is interrupted while waiting
     * @throws IllegalStateException    if consensus is not reached within the
     *                                  timeout
     * @throws IllegalArgumentException if timeout is invalid or no nodes are
     *                                  provided
     */
    public static void awaitClusterConsensus(Duration timeout, NGridNode... nodes) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(nodes, "nodes");

        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Timeout must be positive");
        }

        if (nodes.length == 0) {
            throw new IllegalArgumentException("At least one node must be provided");
        }

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int expectedMembers = nodes.length;

        while (System.currentTimeMillis() < deadline) {
            if (hasConsensus(nodes, expectedMembers)) {
                return;
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException(buildConsensusErrorMessage(nodes, expectedMembers));
    }

    /**
     * Checks if all nodes have reached consensus.
     */
    private static boolean hasConsensus(NGridNode[] nodes, int expectedMembers) {
        // 1. Check all nodes agree on leader
        Optional<NodeInfo> firstLeader = nodes[0].coordinator().leaderInfo();
        for (NGridNode node : nodes) {
            if (!firstLeader.equals(node.coordinator().leaderInfo())) {
                return false;
            }
        }

        // 2. Check all nodes see the correct number of members
        for (NGridNode node : nodes) {
            if (node.coordinator().activeMembers().size() != expectedMembers) {
                return false;
            }
        }

        // 3. Check transport connectivity (all nodes connected to each other)
        for (int i = 0; i < nodes.length; i++) {
            for (int j = 0; j < nodes.length; j++) {
                if (i == j)
                    continue;
                NodeId otherNodeId = nodes[j].transport().local().nodeId();
                if (!nodes[i].transport().isConnected(otherNodeId)) {
                    return false;
                }
            }
        }

        // 4. Leader must be present
        return firstLeader.isPresent();
    }

    /**
     * Builds a detailed error message when consensus fails.
     */
    private static String buildConsensusErrorMessage(NGridNode[] nodes, int expectedMembers) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cluster did not reach consensus. Expected %d members.\n", expectedMembers));

        for (int i = 0; i < nodes.length; i++) {
            NGridNode node = nodes[i];
            NodeId nodeId = node.transport().local().nodeId();
            Optional<NodeInfo> leader = node.coordinator().leaderInfo();
            int activeCount = node.coordinator().activeMembers().size();

            sb.append(String.format("  Node[%d] %s: leader=%s, activeMembers=%d\n",
                    i, nodeId,
                    leader.map(NodeInfo::nodeId).orElse(null),
                    activeCount));
        }

        return sb.toString();
    }
}
