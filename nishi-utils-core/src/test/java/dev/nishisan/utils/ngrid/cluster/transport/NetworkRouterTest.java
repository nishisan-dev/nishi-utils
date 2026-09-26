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

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proxy selection of {@link NetworkRouter}: a relay must be a peer we currently hold a connection
 * to. The reachability map is gossip and keeps naming departed clients and dead nodes as "knowing"
 * the target; routing through one of them swallowed every message — heartbeats included, which
 * evicted live members and collapsed the leadership quorum during a failover.
 */
class NetworkRouterTest {

    private static final NodeId TARGET = NodeId.of("storage-1");
    private static final NodeId DEAD_CLIENT = NodeId.of("client-dead");
    private static final NodeId LIVE_PEER = NodeId.of("storage-0");

    private final Set<NodeId> connected = new CopyOnWriteArraySet<>();
    private final NetworkRouter router = new NetworkRouter(Map::of, connected::contains);

    private static NodeInfo info(NodeId id) {
        return new NodeInfo(id, "127.0.0.1", 1);
    }

    @Test
    void aPeerWeAreNotConnectedToIsNeverChosenAsRelay() {
        // Both the dead client and the live peer "know" the target per gossip; only the live one is
        // connected.
        router.updateReachability(DEAD_CLIENT, List.of(info(TARGET)), Map.of());
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of());
        connected.add(LIVE_PEER);

        router.markDirectFailure(TARGET);

        assertEquals(Optional.of(LIVE_PEER), router.nextHop(TARGET),
                "the relay must be a peer we hold a connection to");
    }

    @Test
    void withNoConnectedRelayTheRouteStaysDirect() {
        router.updateReachability(DEAD_CLIENT, List.of(info(TARGET)), Map.of());

        router.markDirectFailure(TARGET);

        assertEquals(Optional.of(TARGET), router.nextHop(TARGET),
                "with no usable relay the route must fall back to dialing the target directly");
        assertTrue(!router.isProxy(TARGET));
    }

    @Test
    void forgettingARelayDropsItsReportsAndReroutesThroughAnotherRelay() {
        router.updateReachability(DEAD_CLIENT, List.of(info(TARGET)), Map.of(TARGET, 1.0));
        connected.add(DEAD_CLIENT);
        router.markDirectFailure(TARGET);
        assertEquals(Optional.of(DEAD_CLIENT), router.nextHop(TARGET), "precondition: proxy via the client");

        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of());
        connected.add(LIVE_PEER);
        router.forget(DEAD_CLIENT);

        assertEquals(Optional.of(LIVE_PEER), router.nextHop(TARGET),
                "a route through a forgotten peer must be re-evaluated");
        router.promoteToDirect(TARGET);
        connected.remove(LIVE_PEER);
        router.markDirectFailure(TARGET);
        assertEquals(Optional.of(TARGET), router.nextHop(TARGET),
                "the forgotten peer must no longer be a relay candidate, even if a socket to it were open");
    }

    @Test
    void forgettingATargetDropsItsRouteAndEveryMentionOfIt() {
        router.updateReachability(LIVE_PEER, List.of(info(DEAD_CLIENT)), Map.of(DEAD_CLIENT, 3.0));
        connected.add(LIVE_PEER);
        router.markDirectFailure(DEAD_CLIENT);
        assertTrue(router.isProxy(DEAD_CLIENT), "precondition: proxy route to the client");

        router.forget(DEAD_CLIENT);

        assertTrue(!router.isProxy(DEAD_CLIENT), "the forgotten peer's own route must be gone");
        assertTrue(!router.routesSnapshot().containsKey(DEAD_CLIENT));
        router.markDirectFailure(DEAD_CLIENT);
        assertEquals(Optional.of(DEAD_CLIENT), router.nextHop(DEAD_CLIENT),
                "no peer may still be recorded as knowing the forgotten id");
    }
}
