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

import dev.nishisan.utils.ngrid.queue.QueueConsumerCursor;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Cursor-oriented consumer view over a distributed queue.
 *
 * @param <T> the element type
 */
public final class DistributedQueueConsumer<T> {
    private final DistributedQueue<T> queue;
    private final QueueConsumerCursor cursor;

    DistributedQueueConsumer(DistributedQueue<T> queue, QueueConsumerCursor cursor) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.cursor = Objects.requireNonNull(cursor, "cursor");
    }

    public String groupId() {
        return cursor.groupId();
    }

    public String consumerId() {
        return cursor.consumerId();
    }

    public Optional<T> poll() {
        return queue.poll(cursor);
    }

    public Optional<T> pollWhenAvailable(Duration timeout) {
        return queue.pollWhenAvailable(cursor, timeout);
    }

    public Optional<T> peek() {
        return queue.peek(cursor);
    }

    public long position() {
        return queue.position(cursor);
    }

    public void seek(long offset) {
        queue.seek(cursor, offset);
    }
}
