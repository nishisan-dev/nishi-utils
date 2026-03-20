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

package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.common.NodeId;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Raised when a replication operation cannot reach the required quorum due to membership/connection loss.
 */
public final class QuorumUnreachableException extends RuntimeException {
    private final UUID operationId;
    private final int requiredQuorum;
    private final int reachableMembers;
    private final Set<NodeId> acknowledgements;

    public QuorumUnreachableException(UUID operationId,
                                     int requiredQuorum,
                                     int reachableMembers,
                                     Set<NodeId> acknowledgements) {
        super("Quorum unreachable for operationId=" + operationId
              + " requiredQuorum=" + requiredQuorum
              + " reachableMembers=" + reachableMembers
              + " acknowledgements=" + acknowledgements.size());
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.requiredQuorum = requiredQuorum;
        this.reachableMembers = reachableMembers;
        this.acknowledgements = Set.copyOf(Objects.requireNonNull(acknowledgements, "acknowledgements"));
    }

    public UUID operationId() {
        return operationId;
    }

    public int requiredQuorum() {
        return requiredQuorum;
    }

    public int reachableMembers() {
        return reachableMembers;
    }

    public Set<NodeId> acknowledgements() {
        return acknowledgements;
    }
}


