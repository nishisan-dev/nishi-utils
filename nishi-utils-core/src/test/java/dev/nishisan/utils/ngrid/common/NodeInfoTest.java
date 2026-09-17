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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M0 — {@link NodeInfo#isLeaderEligible()} determines whether a member may be chosen leader,
 * based on the presence of {@link NodeInfo#ROLE_LEADER_INELIGIBLE} in {@link NodeInfo#roles()}.
 */
class NodeInfoTest {

    @Test
    void noRolesIsEligible() {
        // The 3-arg constructor stores Collections.emptySet() internally (never null).
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        assertTrue(node.isLeaderEligible(), "a node without roles should be eligible");
    }

    @Test
    void emptyRolesIsEligible() {
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000, Set.of());
        assertTrue(node.isLeaderEligible(), "empty roles should be eligible");
    }

    @Test
    void otherRoleWithoutLeaderIneligibleIsEligible() {
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000, Set.of("storage", "client"));
        assertTrue(node.isLeaderEligible(), "roles without leader-ineligible should be eligible");
    }

    @Test
    void withLeaderIneligibleRoleIsNotEligible() {
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000,
                Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE));
        assertFalse(node.isLeaderEligible(), "the leader-ineligible role should make the node ineligible");
    }

    @Test
    void leaderIneligibleRoleAmongOtherRolesIsNotEligible() {
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000,
                Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE));
        assertFalse(node.isLeaderEligible(),
                "the leader-ineligible role combined with other roles should still make the node ineligible");
    }
}
