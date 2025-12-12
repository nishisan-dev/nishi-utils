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

import dev.nishisan.utils.ngrid.common.OperationStatus;

import java.io.Serializable;
import java.util.UUID;

/**
 * Maintains the replicated history locally so that a node can recover operations after
 * a restart or a temporary disconnection.
 */
public final class ReplicatedRecord implements Serializable {
    private final UUID operationId;
    private final String topic;
    private final Serializable payload;
    private volatile OperationStatus status;

    public ReplicatedRecord(UUID operationId, String topic, Serializable payload, OperationStatus status) {
        this.operationId = operationId;
        this.topic = topic;
        this.payload = payload;
        this.status = status;
    }

    public UUID operationId() {
        return operationId;
    }

    public String topic() {
        return topic;
    }

    public Serializable payload() {
        return payload;
    }

    public OperationStatus status() {
        return status;
    }

    public void status(OperationStatus status) {
        this.status = status;
    }
}
