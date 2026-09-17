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

package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.util.Objects;

/**
 * Single source of truth for the cluster's leadership affinity order (election order): who
 * outranks whom when deciding a leader, a reclaim candidate, or a handback candidate.
 *
 * <p>The rule, in order: a leader-ineligible candidate (role {@link NodeInfo#ROLE_LEADER_INELIGIBLE})
 * never outranks anyone; an ineligible reference is always outranked by an eligible candidate;
 * between two eligible nodes, higher {@code priority} wins, and {@code NodeId} breaks ties (strict
 * total order). Used by {@link ClusterCoordinator} (leader election, dual-leader resolution) and by
 * {@code ReplicationManager} (reclaim-quiesce engagement, affinity handback) so the rule is defined
 * exactly once.
 */
public final class LeadershipAffinity {

    private LeadershipAffinity() {
        // utility class
    }

    /**
     * Returns {@code true} if {@code candidate} outranks {@code reference} in the cluster's
     * leadership affinity order.
     *
     * @param candidate the node being evaluated as a potential leader/reclaimer
     * @param reference the node it is being compared against
     * @return {@code true} if {@code candidate} outranks {@code reference}
     */
    public static boolean outranks(NodeInfo candidate, NodeInfo reference) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(reference, "reference");
        if (!candidate.isLeaderEligible()) {
            return false;
        }
        if (!reference.isLeaderEligible()) {
            return true;
        }
        if (candidate.priority() != reference.priority()) {
            return candidate.priority() > reference.priority();
        }
        return candidate.nodeId().compareTo(reference.nodeId()) > 0;
    }
}
