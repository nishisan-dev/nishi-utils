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

package dev.nishisan.utils.ngrid.queue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable command replicated across the cluster for queue operations.
 */
public final class QueueReplicationCommand implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final QueueReplicationCommandType type;
    private final Serializable value;

    private QueueReplicationCommand(QueueReplicationCommandType type, Serializable value) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
    }

    public static QueueReplicationCommand offer(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value);
    }

    public static QueueReplicationCommand poll(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.POLL, value);
    }

    public QueueReplicationCommandType type() {
        return type;
    }

    public Serializable value() {
        return value;
    }
}
