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

/**
 * Thrown when a write is rejected because the node, although the elected leader, is still
 * synchronizing its state from a peer (leader sync / catch-up in progress). Accepting writes during
 * this window would let a freshly promoted leader advance from stale state and overwrite progress
 * made by the previous leader — the exact divergence this gate prevents.
 *
 * <p>Extends {@link IllegalStateException} on purpose: the leader-readiness pre-check and the
 * not-leader rejection already surface as {@code IllegalStateException}, so existing callers that
 * catch it keep working while this subtype lets callers distinguish the syncing case specifically.</p>
 */
public class LeaderSyncingException extends IllegalStateException {

    /**
     * Creates a leader-syncing exception with the given message.
     *
     * @param message the detail message
     */
    public LeaderSyncingException(String message) {
        super(message);
    }

    /**
     * Creates a leader-syncing exception with a message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public LeaderSyncingException(String message, Throwable cause) {
        super(message, cause);
    }
}
