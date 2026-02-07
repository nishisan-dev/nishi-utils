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

package dev.nishisan.utils.ngrid.cluster.coordination;

/**
 * Thrown when a write operation is rejected because the leader's lease has
 * expired.
 * This indicates the leader has been isolated from the cluster and should not
 * accept
 * new writes to prevent data divergence.
 */
public class LeaseExpiredException extends RuntimeException {

    /**
     * Creates a lease expired exception with the given message.
     *
     * @param message the detail message
     */
    public LeaseExpiredException(String message) {
        super(message);
    }

    /**
     * Creates a lease expired exception with a message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public LeaseExpiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
