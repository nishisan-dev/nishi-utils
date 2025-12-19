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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Manages routing decisions, maintaining direct vs proxy routes (Sticky Fallback).
 */
final class NetworkRouter {
    private static final Logger LOGGER = Logger.getLogger(NetworkRouter.class.getName());

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

    void updateReachability(NodeId sourcePeer, Collection<NodeInfo> knownByPeer) {
        for (NodeInfo target : knownByPeer) {
            reachabilityMap.computeIfAbsent(target.nodeId(), k -> ConcurrentHashMap.newKeySet()).add(sourcePeer);
        }
    }

    void markDirectFailure(NodeId target) {
        routes.compute(target, (id, current) -> {
            if (current != null && current.type() == RouteType.PROXY) {
                // Already proxy, update timestamp? Or keep sticky.
                return current;
            }
            // Switch to Proxy
            return findBestProxy(target, null).map(Route::proxy).orElse(Route.direct());
        });
    }
    
    void promoteToDirect(NodeId target) {
        routes.put(target, Route.direct());
    }

    Optional<NodeId> nextHop(NodeId target) {
        return nextHop(target, null);
    }

    Optional<NodeId> nextHop(NodeId target, NodeId exclude) {
        Route route = routes.getOrDefault(target, Route.direct());
        if (route.type() == RouteType.DIRECT) {
            if (target.equals(exclude)) {
                // Cannot use direct route if it's the excluded node
                return findBestProxy(target, exclude).map(NodeId.class::cast);
            }
            return Optional.of(target);
        }
        
        NodeId via = route.via();
        if (via != null && via.equals(exclude)) {
            // Current proxy is the excluded node, try to find another one
            return findBestProxy(target, exclude);
        }
        
        return Optional.ofNullable(via);
    }

    boolean isProxy(NodeId target) {
        Route route = routes.get(target);
        return route != null && route.type() == RouteType.PROXY;
    }

    private Optional<NodeId> findBestProxy(NodeId target, NodeId exclude) {
        Set<NodeId> candidates = reachabilityMap.getOrDefault(target, Collections.emptySet());
        if (candidates.isEmpty()) {
            LOGGER.warning("No proxy candidates found for " + target);
            return Optional.empty();
        }
        
        return candidates.stream()
                .filter(id -> !id.equals(exclude))
                .findAny();
    }
    
    Map<NodeId, Route> routesSnapshot() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(routes));
    }
}
