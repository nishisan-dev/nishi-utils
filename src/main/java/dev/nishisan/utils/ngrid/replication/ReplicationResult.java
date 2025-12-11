/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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

import dev.nishisan.utils.ngrid.common.OperationStatus;

import java.io.Serializable;
import java.util.UUID;

/**
 * Result of a replication request initiated by the leader.
 */
public final class ReplicationResult implements Serializable {
    private final UUID operationId;
    private final OperationStatus status;

    public ReplicationResult(UUID operationId, OperationStatus status) {
        this.operationId = operationId;
        this.status = status;
    }

    public UUID operationId() {
        return operationId;
    }

    public OperationStatus status() {
        return status;
    }
}
