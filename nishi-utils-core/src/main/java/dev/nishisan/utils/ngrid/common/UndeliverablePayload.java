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

import java.util.UUID;

/**
 * Payload of {@link MessageType#UNDELIVERABLE}: a relay reports back to the original sender that the
 * message {@code messageId} addressed to {@code destination} could not be forwarded because the relay
 * holds no direct connection to that destination. The sender fails the matching pending
 * request/response immediately instead of waiting out its request timeout — typically the case of a
 * request still routed to a leader that just died while the gossip-based proxy route to it remains.
 *
 * @param messageId   the id of the message that could not be forwarded
 * @param destination the destination the relay could not reach
 */
public record UndeliverablePayload(UUID messageId, NodeId destination) {
}
