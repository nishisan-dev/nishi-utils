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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    // ---- connected-peer reports (B1): a relay is a candidate only with a live link to the target ----

    /**
     * Um peer que apenas LISTA o alvo no gossip, mas declara não ter conexão com ele, não pode ser
     * relay: todo nó continua listando um líder morto por muito tempo.
     */
    @Test
    void aPeerThatReportsTheTargetAsNotConnectedIsNeverChosenAsRelay() {
        connected.add(LIVE_PEER);
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of());

        router.markDirectFailure(TARGET);

        assertEquals(Optional.of(TARGET), router.nextHop(TARGET),
                "um peer sem link vivo ao alvo não pode ser relay");
        assertFalse(router.isProxy(TARGET));
    }

    @Test
    void aSenderWithoutTheConnectedFieldKeepsTheLegacyBehaviour() {
        connected.add(LIVE_PEER);
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), null);

        router.markDirectFailure(TARGET);

        assertEquals(Optional.of(LIVE_PEER), router.nextHop(TARGET),
                "sem o campo (8.7.0) o gossip ainda vale como alcançabilidade");
        assertTrue(router.isProxy(TARGET), "sem o campo, isProxy segue como antes");
    }

    /** O relay perde o link ao alvo (o alvo morreu): a rota volta a DIRECT e isProxy fica falso. */
    @Test
    void relayThatLosesItsLinkToTheTargetReturnsTheRouteToDirect() {
        connected.add(LIVE_PEER);
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        router.markDirectFailure(TARGET);
        assertEquals(Optional.of(LIVE_PEER), router.nextHop(TARGET), "precondição: proxy via o peer vivo");
        assertTrue(router.isProxy(TARGET));

        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of());

        assertEquals(Optional.of(TARGET), router.nextHop(TARGET), "sem relay candidato a rota volta a DIRECT");
        assertFalse(router.isProxy(TARGET));
    }

    /** UNDELIVERABLE vindo do relay R para o alvo T retira R dos candidatos de T. */
    @Test
    void undeliverableFromTheRelayDropsItAsCandidate() {
        connected.add(LIVE_PEER);
        connected.add(DEAD_CLIENT);
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        router.markDirectFailure(TARGET);
        assertEquals(Optional.of(LIVE_PEER), router.nextHop(TARGET), "precondição: proxy via o peer vivo");

        router.relayFailed(LIVE_PEER, TARGET);

        assertEquals(Optional.of(TARGET), router.nextHop(TARGET), "sem outro candidato a rota volta a DIRECT");
        assertFalse(router.isProxy(TARGET));

        // With a second candidate the route moves to it instead.
        router.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        router.updateReachability(DEAD_CLIENT, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        router.markDirectFailure(TARGET);
        NodeId first = router.nextHop(TARGET).orElseThrow();
        router.relayFailed(first, TARGET);
        NodeId second = router.nextHop(TARGET).orElseThrow();
        assertTrue(!second.equals(first) && !second.equals(TARGET),
                "com outro candidato a rota deveria migrar para ele, mas foi para " + second);
    }

    /** isProxy só vale enquanto o relatório de conexão do relay está fresco. */
    @Test
    void isProxyRequiresAFreshConnectedReport() throws InterruptedException {
        NetworkRouter shortLived = new NetworkRouter(Map::of, connected::contains, Duration.ofMillis(100));
        connected.add(LIVE_PEER);
        shortLived.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        shortLived.markDirectFailure(TARGET);
        assertTrue(shortLived.isProxy(TARGET), "relatório fresco: rota via proxy conta");

        Thread.sleep(250);

        assertFalse(shortLived.isProxy(TARGET), "relatório vencido: só o gossip não sustenta isProxy");
        shortLived.updateReachability(LIVE_PEER, List.of(info(TARGET)), Map.of(), Set.of(TARGET));
        assertTrue(shortLived.isProxy(TARGET), "relatório renovado volta a contar");
    }
}
