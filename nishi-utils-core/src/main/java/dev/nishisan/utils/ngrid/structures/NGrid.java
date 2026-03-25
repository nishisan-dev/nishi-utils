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

/**
 * Simplified facade for creating NGrid clusters and nodes.
 * <p>
 * This class provides a concise, fluent API that wraps the lower-level
 * {@link NGridConfig.Builder} and {@link NGridNode} APIs. The existing
 * API remains fully available and is used internally by this facade.
 *
 * <h3>Usage Examples</h3>
 *
 * <b>Local cluster (dev/tests):</b>
 * <pre>{@code
 * try (NGridCluster cluster = NGrid.local(2)
 *         .queue("orders")
 *         .map("users")
 *         .start()) {
 *
 *     cluster.queue("orders", String.class).offer("pedido-1");
 *     cluster.map("users", String.class, String.class).put("u1", "Alice");
 * }
 * }</pre>
 *
 * <b>Production node with seed:</b>
 * <pre>{@code
 * try (NGridNode node = NGrid.node("192.168.1.10", 9011)
 *         .seed("192.168.1.11:9011")
 *         .queue("orders")
 *         .start()) {
 *
 *     node.getQueue("orders", String.class).offer("pedido-1");
 * }
 * }</pre>
 *
 * @since 3.3.0
 */
public final class NGrid {

    private NGrid() {
        // Static facade — no instantiation
    }

    /**
     * Creates a local cluster builder for dev/test usage.
     * <p>
     * All nodes will bind to {@code 127.0.0.1} with ephemeral ports,
     * use temporary data directories, and form a full-mesh cluster
     * automatically.
     *
     * @param nodeCount number of nodes in the local cluster (must be ≥ 1)
     * @return a builder for configuring the local cluster
     * @throws IllegalArgumentException if nodeCount is less than 1
     */
    public static NGridLocalBuilder local(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount must be >= 1, got: " + nodeCount);
        }
        return new NGridLocalBuilder(nodeCount);
    }

    /**
     * Creates a single-node builder for production usage, with automatic port
     * allocation.
     * <p>
     * The node will bind to the specified host. The port will be allocated
     * automatically (ephemeral). Use {@link #node(String, int)} to specify
     * an explicit port.
     *
     * @param host the host/IP to bind to
     * @return a builder for configuring the node
     */
    public static NGridNodeBuilder node(String host) {
        return new NGridNodeBuilder(host, 0);
    }

    /**
     * Creates a single-node builder for production usage with an explicit port.
     *
     * @param host the host/IP to bind to
     * @param port the TCP port to bind to
     * @return a builder for configuring the node
     */
    public static NGridNodeBuilder node(String host, int port) {
        return new NGridNodeBuilder(host, port);
    }
}
