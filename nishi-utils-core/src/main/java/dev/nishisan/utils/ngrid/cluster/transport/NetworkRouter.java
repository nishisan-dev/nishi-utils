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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Manages routing decisions, maintaining direct vs proxy routes (Sticky Fallback).
 * Now supports RTT-based cost estimation for Phase 2.
 */
/*
 * Relay policy (8.3.0): a route "via" a proxy is ONE hop over that proxy's direct, open connection to
 * the destination. The relay never dials the destination on the sender's behalf nor re-proxies
 * through a third node, so the gossip-derived two-hop routes this router could previously compute
 * (A → B → C → D) are deliberately no longer attempted: they turned every message to a dead node into
 * a TTL-bounded storm of failed dials across the survivors. A relay that cannot forward answers
 * UNDELIVERABLE (negotiated in the handshake) so request/response callers fail fast.
 */
final class NetworkRouter {
    private static final Logger LOGGER = Logger.getLogger(NetworkRouter.class.getName());
    private static final double SWITCH_THRESHOLD = 0.15; // 15% improvement required to switch

    enum RouteType { DIRECT, PROXY }

    record Route(RouteType type, NodeId via, Instant lastDirectFailure) {
        static Route direct() {
            return new Route(RouteType.DIRECT, null, null);
        }

        static Route proxy(NodeId via) {
            return new Route(RouteType.PROXY, via, Instant.now());
        }
    }

    private final Map<NodeId, Route> routes = new ConcurrentHashMap<>();
    // Tracks "Who knows Whom". Key: Target NodeId, Value: Set of Peers that know Target.
    private final Map<NodeId, Set<NodeId>> reachabilityMap = new ConcurrentHashMap<>();
    // Key: SourcePeer, Value: Map of (TargetNode -> Latency reported by SourcePeer)
    private final Map<NodeId, Map<NodeId, Double>> reportedLatencies = new ConcurrentHashMap<>();
    private final Supplier<Map<NodeId, Double>> localLatenciesSupplier;
    /**
     * Whether a peer can currently relay for us — i.e. we hold an open connection to it. The
     * reachability map is gossip: it keeps saying that a departed client or a dead node "knows" the
     * target long after it is gone, and a proxy we cannot even reach silently swallows every message
     * routed through it (heartbeats included, which evicts live members).
     */
    private final Predicate<NodeId> relayCandidate;

    NetworkRouter(Supplier<Map<NodeId, Double>> localLatenciesSupplier) {
        this(localLatenciesSupplier, id -> true);
    }

    NetworkRouter(Supplier<Map<NodeId, Double>> localLatenciesSupplier, Predicate<NodeId> relayCandidate) {
        this.localLatenciesSupplier = localLatenciesSupplier;
        this.relayCandidate = relayCandidate;
    }

    /**
     * Updates the reachability information of nodes and re-evaluates routes if necessary.
     *
     * @param sourcePeer The {@link NodeId} of the peer reporting the reachability data.
     * @param knownByPeer A collection of {@link NodeInfo} objects representing nodes known by the reporting peer.
     * @param latenciesFromSource A map of {@link NodeId} to latency values, indicating the latencies
     *                             reported by the peer for each target node.
     */
    void updateReachability(NodeId sourcePeer, Collection<NodeInfo> knownByPeer, Map<NodeId, Double> latenciesFromSource) {
        reportedLatencies.put(sourcePeer, new HashMap<>(latenciesFromSource));
        for (NodeInfo target : knownByPeer) {
            reachabilityMap.computeIfAbsent(target.nodeId(), k -> ConcurrentHashMap.newKeySet()).add(sourcePeer);
        }
        // Re-evaluate routes for these targets if we are in proxy mode
        for (NodeInfo target : knownByPeer) {
            NodeId targetId = target.nodeId();
            routes.computeIfPresent(targetId, (id, current) -> {
                if (current.type() == RouteType.PROXY) {
                    return findBestProxy(targetId, null).map(Route::proxy).orElse(current);
                }
                return current;
            });
        }
    }

    /**
     * Updates the routing information by marking a target node as unreachable for direct communication.
     * Attempts to find the best proxy node for routing if a direct route fails.
     *
     * @param target The {@link NodeId} of the target node for which the direct communication has failed.
     */
    void markDirectFailure(NodeId target) {
        routes.compute(target, (id, current) -> {
            if (current != null && current.type() == RouteType.PROXY) {
                return current;
            }
            return findBestProxy(target, null).map(Route::proxy).orElse(Route.direct());
        });
    }

    /**
     * Drops everything known about a departed peer: its own route, the reachability and latencies it
     * reported, and every mention of it as a target or relay in other peers' reports. A proxy route
     * that went through it is re-evaluated (another relay, or back to direct), so nothing keeps
     * pointing at an id the transport no longer knows.
     *
     * @param id the forgotten peer
     */
    void forget(NodeId id) {
        routes.remove(id);
        reachabilityMap.remove(id);
        reportedLatencies.remove(id);
        reachabilityMap.values().forEach(reporters -> reporters.remove(id));
        // Inner latency maps are replaced, never mutated: findBestProxy reads them concurrently.
        reportedLatencies.replaceAll((source, latencies) -> {
            if (!latencies.containsKey(id)) {
                return latencies;
            }
            Map<NodeId, Double> copy = new HashMap<>(latencies);
            copy.remove(id);
            return copy;
        });
        routes.replaceAll((target, route) -> route.type() == RouteType.PROXY && id.equals(route.via())
                ? findBestProxy(target, id).map(Route::proxy).orElse(Route.direct())
                : route);
    }

    void promoteToDirect(NodeId target) {
        routes.put(target, Route.direct());
    }

    Optional<NodeId> nextHop(NodeId target) {
        return nextHop(target, null);
    }

    Optional<NodeId> nextHop(NodeId target, NodeId exclude) {
        Route route = routes.getOrDefault(target, Route.direct());

        // Phase 2: Even if we are DIRECT, check if there is a proxy path that is MUCH better
        if (route.type() == RouteType.DIRECT) {
            if (target.equals(exclude)) {
                return findBestProxy(target, exclude);
            }

            Optional<NodeId> betterProxy = findSignificantlyBetterProxy(target, exclude);
            if (betterProxy.isPresent()) {
                LOGGER.info("RTT Optimization: Routing to " + target + " via proxy " + betterProxy.get());
                return betterProxy;
            }

            return Optional.of(target);
        }

        NodeId via = route.via();
        if (via != null && via.equals(exclude)) {
            return findBestProxy(target, exclude);
        }

        return Optional.ofNullable(via);
    }

    /**
     * Attempts to find a proxy node for routing to the target node that provides significantly
     * better performance, as compared to the direct route latency.
     *
     * This method evaluates the round-trip time (RTT) to the target node through direct communication.
     * If a candidate proxy with a substantially lower cost-to-reach is available while meeting the defined
     * performance threshold, this proxy is selected and returned.
     *
     * @param target The {@link NodeId} of the target node for which a better proxy is being searched.
     * @param exclude The {@link NodeId} of a node to exclude from proxy consideration, typically used
     *                for avoiding routing through a specified node.
     * @return An {@link Optional} containing the {@link NodeId} of the significantly better proxy
     *         node if one is found; otherwise, an empty {@link Optional}.
     */
    private Optional<NodeId> findSignificantlyBetterProxy(NodeId target, NodeId exclude) {
        Map<NodeId, Double> local = localLatenciesSupplier.get();
        Double directRtt = local.get(target);
        if (directRtt == null) return Optional.empty();

        Optional<PathCost> bestProxy = findBestProxyWithCost(target, exclude);
        if (bestProxy.isPresent() && bestProxy.get().cost < directRtt * (1.0 - SWITCH_THRESHOLD)) {
            return Optional.of(bestProxy.get().nodeId);
        }
        return Optional.empty();
    }

    boolean isProxy(NodeId target) {
        Route route = routes.get(target);
        return route != null && route.type() == RouteType.PROXY;
    }

    private record PathCost(NodeId nodeId, double cost) {}

    private Optional<NodeId> findBestProxy(NodeId target, NodeId exclude) {
        return findBestProxyWithCost(target, exclude).map(PathCost::nodeId);
    }

    private Optional<PathCost> findBestProxyWithCost(NodeId target, NodeId exclude) {
        Set<NodeId> candidates = reachabilityMap.getOrDefault(target, Collections.emptySet());
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        Map<NodeId, Double> local = localLatenciesSupplier.get();

        List<PathCost> scoredCandidates = candidates.stream()
                .filter(id -> !id.equals(exclude) && relayCandidate.test(id))
                .map(proxyId -> {
                    Double rttToProxy = local.get(proxyId);
                    Map<NodeId, Double> proxyReported = reportedLatencies.get(proxyId);
                    Double rttFromProxyToTarget = proxyReported != null ? proxyReported.get(target) : null;

                    if (rttToProxy != null && rttFromProxyToTarget != null) {
                        return new PathCost(proxyId, rttToProxy + rttFromProxyToTarget);
                    }
                    // If no RTT info, use a very high but not MAX_VALUE cost to allow it as fallback
                    return new PathCost(proxyId, 1000000.0);
                })
                .toList();

        return scoredCandidates.stream()
                .min(Comparator.comparingDouble(PathCost::cost));
    }

    Map<NodeId, Route> routesSnapshot() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(routes));
    }
}
