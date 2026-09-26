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

package dev.nishisan.utils.ngrid.common;

/**
 * Payload of {@link MessageType#LEAVE}: a node announces, on each of its open connections, that it is
 * closing for good. The receiver honors it only first-hand — on the connection currently tracked for
 * that peer and only when {@code node} is the identity that connection handshaked with — and never
 * forwards it. An ephemeral node (leader-ineligible or without a listen port) is then forgotten at
 * once; a leader-eligible one stays a known voter.
 *
 * @param node   the leaving node, as it describes itself
 * @param reason free-form reason, for logs only
 * @since 8.7.0
 */
public record LeavePayload(NodeInfo node, String reason) {
}
