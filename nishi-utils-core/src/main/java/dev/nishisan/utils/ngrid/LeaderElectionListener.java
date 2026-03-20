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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Listener that is notified when the local node gains or loses leadership, and
 * also whenever
 * the observed leader changes.
 */
@FunctionalInterface
public interface LeaderElectionListener {

    /**
     * Called when the local node gains or loses leadership.
     *
     * @param isLeader      whether the local node is the leader after the change
     * @param currentLeader the current leader id (may be {@code null} if unknown)
     */
    void onLeadershipChanged(boolean isLeader, NodeId currentLeader);
}
