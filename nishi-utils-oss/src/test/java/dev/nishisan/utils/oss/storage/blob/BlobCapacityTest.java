package dev.nishisan.utils.oss.storage.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class BlobCapacityTest {
    @TempDir Path directory;

    private BlobStorage storage() {
        return BlobStorage.create(directory, 1, 1 << 20, 1 << 20);
    }

    @Test void reservationAndOpenShareOneBudget() {
        try (var storage = storage()) {
            storage.configureCapacity(9000, () -> Long.MAX_VALUE);
            storage.reserve("move", "a", 8192);
            storage.reserve("move", "a", 8192);
            assertEquals(8192, storage.reservedBytes());
            assertThrows(BlobCapacityException.class, () -> storage.put("b", new byte[4096]));
            assertThrows(BlobCapacityException.class, () -> storage.put("a", new byte[8192]));
            storage.atomicReplaceReserved("a", new byte[8192], "move");
            assertEquals(0, storage.reservedBytes());
            assertThrows(BlobCapacityException.class, () -> storage.put("b", new byte[4096]));
            storage.delete("a");
            storage.put("b", new byte[4096]);
        }
    }

    @Test void failedGrowthPreservesExistingImageAndReleaseRestoresBudget() {
        try (var storage = storage()) {
            storage.configureCapacity(9000, () -> Long.MAX_VALUE);
            byte[] original = {1, 2, 3};
            storage.put("a", original);
            storage.reserve("move", "b", 4096);
            assertThrows(BlobCapacityException.class, () -> storage.put("a", new byte[8192]));
            assertArrayEquals(original, storage.get("a").orElseThrow());
            storage.releaseReservation("move");
            storage.releaseReservation("move");
            storage.put("a", new byte[8192]);
        }
    }

    @Test void simultaneousReservationsCannotOversubscribe() throws Exception {
        try (var storage = storage(); var pool = Executors.newFixedThreadPool(2)) {
            storage.configureCapacity(9000, () -> Long.MAX_VALUE);
            var start = new CountDownLatch(1);
            Callable<Boolean> first = () -> reserveAfter(start, storage, "one");
            Callable<Boolean> second = () -> reserveAfter(start, storage, "two");
            var a = pool.submit(first);
            var b = pool.submit(second);
            start.countDown();
            assertNotEquals(a.get(5, TimeUnit.SECONDS), b.get(5, TimeUnit.SECONDS));
            assertEquals(8192, storage.reservedBytes());
        }
    }

    private boolean reserveAfter(CountDownLatch start, BlobStorage storage, String id) throws InterruptedException {
        start.await();
        try { storage.reserve(id, id, 8192); return true; }
        catch (BlobCapacityException expected) { return false; }
    }

    @Test void filesystemCheckAndRestartAccounting() {
        try (var storage = storage()) {
            storage.put("a", new byte[4096]);
            storage.configureCapacity(0, () -> 0);
            assertThrows(BlobCapacityException.class, () -> storage.reserve("move", "b", 4096));
            storage.put("a", new byte[4096]); // no extra allocation
        }
        try (var storage = BlobStorage.open(directory)) {
            storage.configureCapacity(5000, () -> Long.MAX_VALUE);
            assertThrows(BlobCapacityException.class, () -> storage.put("b", new byte[4096]));
            assertEquals(0, storage.reservedBytes());
        }
    }

    @Test void exactBoundaryAndOverflow() {
        assertEquals(95, CapacityBudget.limit(100));
        assertTrue(CapacityBudget.fits(100, 90, 0, 5));
        assertFalse(CapacityBudget.fits(100, 95, 0, 0));
        assertFalse(CapacityBudget.fits(100, 90, 4, 2));
        assertFalse(CapacityBudget.fits(0, Long.MAX_VALUE - 2, 2, 1));
        assertEquals(4096, BlobStorage.alignedRegionBytes(1));
        assertThrows(IllegalArgumentException.class, () -> BlobStorage.alignedRegionBytes(Long.MAX_VALUE));
    }

    @Test void fullDestinationRejectsMigrationEvenWhenReplacingAnExistingRegion() {
        try (var storage = storage()) {
            storage.put("a", new byte[4096]);
            storage.configureCapacity(4096, () -> Long.MAX_VALUE);
            assertThrows(BlobCapacityException.class, () -> storage.reserve("move", "a", 4096));
            storage.put("a", new byte[4096]); // regular writes still update the existing series
            storage.configureCapacity(9000, () -> Long.MAX_VALUE);
            storage.reserve("move", "a", 4096);
            storage.configureCapacity(4096, () -> Long.MAX_VALUE);
            assertThrows(BlobCapacityException.class,
                    () -> storage.atomicReplaceReserved("a", new byte[4096], "move"));
        }
    }
}
