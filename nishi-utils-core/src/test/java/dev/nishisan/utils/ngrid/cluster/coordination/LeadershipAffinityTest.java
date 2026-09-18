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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B2 — {@link LeadershipAffinity} is the single source of truth for the leadership affinity rule,
 * used by both {@link ClusterCoordinator} and {@code ReplicationManager}. Covers the four cases:
 * two eligible nodes (priority and NodeId tie-break), an ineligible candidate, and an ineligible
 * reference.
 */
class LeadershipAffinityTest {

    private static NodeInfo node(String id, int priority, boolean ineligible) {
        Set<String> roles = ineligible ? Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE) : Set.of();
        return new NodeInfo(NodeId.of(id), "127.0.0.1", 5000, roles, priority);
    }

    @Test
    void betweenTwoEligibleNodesHigherPriorityOutranks() {
        NodeInfo candidate = node("a", 100, false);
        NodeInfo reference = node("b", 10, false);
        assertTrue(LeadershipAffinity.outranks(candidate, reference));
        assertFalse(LeadershipAffinity.outranks(reference, candidate));
    }

    @Test
    void betweenTwoEligibleNodesWithEqualPriorityNodeIdBreaksTheTie() {
        NodeInfo lower = node("node-1", 10, false);
        NodeInfo higher = node("node-9", 10, false);
        assertTrue(LeadershipAffinity.outranks(higher, lower));
        assertFalse(LeadershipAffinity.outranks(lower, higher));
    }

    @Test
    void ineligibleCandidateNeverOutranksEvenWithHigherPriority() {
        NodeInfo ineligibleCandidate = node("a", 100, true);
        NodeInfo eligibleReference = node("b", 10, false);
        assertFalse(LeadershipAffinity.outranks(ineligibleCandidate, eligibleReference),
                "an ineligible candidate should never outrank, even with a higher priority");
    }

    @Test
    void ineligibleReferenceIsAlwaysOutrankedByAnEligibleCandidate() {
        NodeInfo eligibleCandidate = node("a", 0, false);
        NodeInfo ineligibleReference = node("b", 100, true);
        assertTrue(LeadershipAffinity.outranks(eligibleCandidate, ineligibleReference),
                "an ineligible reference should always be outranked by an eligible candidate,"
                        + " even with a lower priority");
    }

    @Test
    void twoIneligibleNodesNeverOutrankEachOther() {
        NodeInfo a = node("a", 100, true);
        NodeInfo b = node("b", 10, true);
        assertFalse(LeadershipAffinity.outranks(a, b));
        assertFalse(LeadershipAffinity.outranks(b, a));
    }
}
