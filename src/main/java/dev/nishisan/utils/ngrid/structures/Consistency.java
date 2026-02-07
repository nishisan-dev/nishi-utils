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

import java.util.Objects;

/**
 * Configuration for read consistency.
 */
public final class Consistency {
    private final ConsistencyLevel level;
    private final long maxLag;

    private Consistency(ConsistencyLevel level, long maxLag) {
        this.level = Objects.requireNonNull(level, "level");
        this.maxLag = maxLag;
    }

    /**
     * Strong consistency. Routes to leader.
     */
    public static final Consistency STRONG = new Consistency(ConsistencyLevel.STRONG, 0);

    /**
     * Eventual consistency. Reads from local state regardless of lag.
     */
    public static final Consistency EVENTUAL = new Consistency(ConsistencyLevel.EVENTUAL, Long.MAX_VALUE);

    /**
     * Bounded consistency. Reads from local state if the replication lag (Leader
     * Sequence - Local Sequence)
     * is less than or equal to {@code maxLag}.
     *
     * @param maxLag Maximum allowed sequence lag. Must be >= 0.
     * @return A Consistency configuration.
     */
    public static Consistency bounded(long maxLag) {
        if (maxLag < 0) {
            throw new IllegalArgumentException("maxLag must be >= 0");
        }
        return new Consistency(ConsistencyLevel.BOUNDED, maxLag);
    }

    /**
     * Returns the consistency level.
     *
     * @return the level
     */
    public ConsistencyLevel level() {
        return level;
    }

    /**
     * Returns the maximum allowed replication lag.
     *
     * @return the max lag
     */
    public long maxLag() {
        return maxLag;
    }
}
