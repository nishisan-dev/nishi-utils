/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 8.10.1: {@link NMapPersistence#close()} reports a writer that did not finish draining the WAL queue
 * within the join bound, so a caller deciding on a clean-shutdown marker does not trust an incomplete WAL.
 */
class NMapPersistenceCloseTest {

    @TempDir
    Path tempDir;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeFailsWhenTheWriterDoesNotTerminateAndSucceedsOnceItDrains() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_NO_FSYNC)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(5))
                .build();
        NMapPersistence<String, Serializable> persistence =
                new NMapPersistence<>(cfg, new ConcurrentHashMap<>(), tempDir, "close-map");
        persistence.start();
        BlockingValue value = new BlockingValue();
        try {
            persistence.appendAsync(NMapOperationType.PUT, "k", value);
            assertTrue(value.entered.await(10, TimeUnit.SECONDS), "the writer must start serializing the entry");
            persistence.closeJoinTimeoutMillis(200);

            IOException failure = assertThrows(IOException.class, persistence::close,
                    "a writer still busy after the join bound must be reported");
            assertTrue(failure.getMessage().contains("did not terminate"), failure.getMessage());
            assertTrue(persistence.walOpen(), "a failed close leaves the WAL channel to the busy writer");
        } finally {
            value.release.countDown();
        }
        // The writer that outlived the join closes its own channel once its final flush is done.
        long deadline = System.currentTimeMillis() + 10_000;
        while (persistence.walOpen() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(persistence.walOpen(), "the late writer must close the WAL channel when it terminates");
        persistence.closeJoinTimeoutMillis(10_000);
        assertDoesNotThrow(persistence::close, "once the writer drained, close succeeds");
    }

    /** A value whose Java serialization (on the writer thread) blocks until released. */
    private static final class BlockingValue implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final transient CountDownLatch entered = new CountDownLatch(1);
        private final transient CountDownLatch release = new CountDownLatch(1);

        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            out.defaultWriteObject();
        }
    }
}
