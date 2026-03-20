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
 * Defines the consistency guarantees for read operations.
 */
public enum ConsistencyLevel {
    /**
     * Strong consistency (Linearizable). Operations are routed to the leader.
     * This guarantees that the read returns the most recent write.
     */
    STRONG,

    /**
     * Eventual consistency. Operations are executed locally on the node.
     * The data may be stale compared to the leader. Provides lowest latency.
     */
    EVENTUAL,

    /**
     * Bounded staleness. Operations are executed locally if the local node's
     * replication lag is within the specified limit. Otherwise, they may
     * fall back to the leader or fail depending on implementation (currently routes to leader).
     */
    BOUNDED
}
