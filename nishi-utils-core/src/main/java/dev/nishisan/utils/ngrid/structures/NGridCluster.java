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

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Container that manages multiple {@link NGridNode} instances forming a local
 * cluster. Designed for dev/test usage via {@link NGrid#local(int)}.
 * <p>
 * Provides convenience methods to access distributed structures from any node
 * in the cluster. The {@link #close()} method shuts down all managed nodes.
 *
 * @since 3.3.0
 */
public final class NGridCluster implements Closeable {

    private final List<NGridNode> nodes;

    NGridCluster(List<NGridNode> nodes) {
        this.nodes = Collections.unmodifiableList(Objects.requireNonNull(nodes, "nodes"));
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster must have at least one node");
        }
    }

    /**
     * Returns a distributed queue from the first node in the cluster.
     *
     * @param name the queue name
     * @param type the element type
     * @param <T>  element type
     * @return the distributed queue
     */
    public <T> DistributedQueue<T> queue(String name, Class<T> type) {
        return nodes.get(0).getQueue(name, type);
    }

    /**
     * Returns a distributed queue from a specific node in the cluster.
     *
     * @param nodeIndex the node index (0-based)
     * @param name      the queue name
     * @param type      the element type
     * @param <T>       element type
     * @return the distributed queue
     */
    public <T> DistributedQueue<T> queue(int nodeIndex, String name, Class<T> type) {
        return nodes.get(nodeIndex).getQueue(name, type);
    }

    /**
     * Returns a distributed map from the first node in the cluster.
     *
     * @param name      the map name
     * @param keyType   the key type
     * @param valueType the value type
     * @param <K>       key type
     * @param <V>       value type
     * @return the distributed map
     */
    public <K, V> DistributedMap<K, V> map(
            String name, Class<K> keyType, Class<V> valueType) {
        return nodes.get(0).getMap(name, keyType, valueType);
    }

    /**
     * Returns a distributed map from a specific node in the cluster.
     *
     * @param nodeIndex the node index (0-based)
     * @param name      the map name
     * @param keyType   the key type
     * @param valueType the value type
     * @param <K>       key type
     * @param <V>       value type
     * @return the distributed map
     */
    public <K, V> DistributedMap<K, V> map(
            int nodeIndex, String name, Class<K> keyType, Class<V> valueType) {
        return nodes.get(nodeIndex).getMap(name, keyType, valueType);
    }

    /**
     * Returns the node at the given index.
     *
     * @param index 0-based node index
     * @return the node
     */
    public NGridNode node(int index) {
        return nodes.get(index);
    }

    /**
     * Returns an unmodifiable list of all nodes in the cluster.
     */
    public List<NGridNode> nodes() {
        return nodes;
    }

    /**
     * Returns the number of nodes in the cluster.
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Returns the current leader node, if one has been elected.
     *
     * @return the leader node, or empty if no leader is available
     */
    public Optional<NGridNode> leader() {
        Optional<NodeInfo> leaderInfo = nodes.get(0).coordinator().leaderInfo();
        if (leaderInfo.isEmpty()) {
            return Optional.empty();
        }
        var leaderId = leaderInfo.get().nodeId();
        return nodes.stream()
                .filter(n -> n.config().local().nodeId().equals(leaderId))
                .findFirst();
    }

    /**
     * Closes all managed nodes. Exceptions from individual nodes are
     * accumulated and the first one is thrown after all nodes are closed.
     */
    @Override
    public void close() throws IOException {
        IOException first = null;
        for (NGridNode node : nodes) {
            try {
                node.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
