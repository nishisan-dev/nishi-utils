package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.queue.NQueue;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class QueueConfigTest {

    @Test
    void shouldBuildQueueConfigWithDefaults() {
        QueueConfig config = QueueConfig.builder("test-queue").build();

        assertEquals("test-queue", config.name());
        assertNotNull(config.retention());
        assertEquals(QueueConfig.RetentionPolicy.Type.TIME_BASED, config.retention().type());
        assertEquals(Duration.ofDays(7), config.retention().duration());
        assertFalse(config.compressionEnabled());
        assertNull(config.maxSizeBytes());
        assertNull(config.maxItems());
    }

    @Test
    void shouldBuildQueueConfigWithCustomRetention() {
        QueueConfig config = QueueConfig.builder("orders")
                .retention(QueueConfig.RetentionPolicy.timeBased(Duration.ofHours(24)))
                .build();

        assertEquals("orders", config.name());
        assertEquals(QueueConfig.RetentionPolicy.Type.TIME_BASED, config.retention().type());
        assertEquals(Duration.ofHours(24), config.retention().duration());
    }

    @Test
    void shouldBuildQueueConfigWithSizeBasedRetention() {
        QueueConfig config = QueueConfig.builder("events")
                .retention(QueueConfig.RetentionPolicy.sizeBased(10_000_000))
                .build();

        assertEquals(QueueConfig.RetentionPolicy.Type.SIZE_BASED, config.retention().type());
        assertEquals(10_000_000L, config.retention().maxSize());
    }

    @Test
    void shouldBuildQueueConfigWithCountBasedRetention() {
        QueueConfig config = QueueConfig.builder("logs")
                .retention(QueueConfig.RetentionPolicy.countBased(100_000))
                .build();

        assertEquals(QueueConfig.RetentionPolicy.Type.COUNT_BASED, config.retention().type());
        assertEquals(100_000, config.retention().maxCount());
    }

    @Test
    void shouldBuildQueueConfigWithAllOptions() {
        NQueue.Options nqOptions = NQueue.Options.defaults()
                .withFsync(true)
                .withMemoryBuffer(true)
                .withMemoryBufferSize(1000);

        QueueConfig config = QueueConfig.builder("complete-queue")
                .retention(QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(1)))
                .maxSizeBytes(50_000_000)
                .maxItems(1_000_000)
                .compressionEnabled(true)
                .nqueueOptions(nqOptions)
                .build();

        assertEquals("complete-queue", config.name());
        assertEquals(50_000_000L, config.maxSizeBytes());
        assertEquals(1_000_000, config.maxItems());
        assertTrue(config.compressionEnabled());
        assertNotNull(config.nqueueOptions());
    }

    @Test
    void shouldRejectNullQueueName() {
        assertThrows(NullPointerException.class, () -> QueueConfig.builder(null));
    }

    @Test
    void shouldRejectEmptyQueueName() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder(""));
    }

    @Test
    void shouldRejectBlankQueueName() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder("   "));
    }

    @Test
    void shouldRejectNullRetention() {
        assertThrows(NullPointerException.class, () -> QueueConfig.builder("test").retention(null));
    }

    @Test
    void shouldRejectNegativeMaxSizeBytes() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder("test").maxSizeBytes(-1));
    }

    @Test
    void shouldRejectZeroMaxSizeBytes() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder("test").maxSizeBytes(0));
    }

    @Test
    void shouldRejectNegativeMaxItems() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder("test").maxItems(-1));
    }

    @Test
    void shouldRejectZeroMaxItems() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.builder("test").maxItems(0));
    }

    @Test
    void shouldRejectNullNQueueOptions() {
        assertThrows(NullPointerException.class, () -> QueueConfig.builder("test").nqueueOptions(null));
    }

    // RetentionPolicy tests
    @Test
    void shouldCreateTimeBasedRetentionPolicy() {
        QueueConfig.RetentionPolicy policy = QueueConfig.RetentionPolicy.timeBased(Duration.ofHours(12));

        assertEquals(QueueConfig.RetentionPolicy.Type.TIME_BASED, policy.type());
        assertEquals(Duration.ofHours(12), policy.duration());
        assertNull(policy.maxSize());
        assertNull(policy.maxCount());
    }

    @Test
    void shouldCreateSizeBasedRetentionPolicy() {
        QueueConfig.RetentionPolicy policy = QueueConfig.RetentionPolicy.sizeBased(5_000_000);

        assertEquals(QueueConfig.RetentionPolicy.Type.SIZE_BASED, policy.type());
        assertEquals(5_000_000L, policy.maxSize());
        assertNull(policy.duration());
        assertNull(policy.maxCount());
    }

    @Test
    void shouldCreateCountBasedRetentionPolicy() {
        QueueConfig.RetentionPolicy policy = QueueConfig.RetentionPolicy.countBased(50_000);

        assertEquals(QueueConfig.RetentionPolicy.Type.COUNT_BASED, policy.type());
        assertEquals(50_000, policy.maxCount());
        assertNull(policy.duration());
        assertNull(policy.maxSize());
    }

    @Test
    void shouldRejectNullDurationInTimeBasedPolicy() {
        assertThrows(NullPointerException.class, () -> QueueConfig.RetentionPolicy.timeBased(null));
    }

    @Test
    void shouldRejectNegativeDurationInTimeBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.timeBased(Duration.ofHours(-1)));
    }

    @Test
    void shouldRejectZeroDurationInTimeBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.timeBased(Duration.ZERO));
    }

    @Test
    void shouldRejectNegativeSizeInSizeBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.sizeBased(-1));
    }

    @Test
    void shouldRejectZeroSizeInSizeBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.sizeBased(0));
    }

    @Test
    void shouldRejectNegativeCountInCountBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.countBased(-1));
    }

    @Test
    void shouldRejectZeroCountInCountBasedPolicy() {
        assertThrows(IllegalArgumentException.class, () -> QueueConfig.RetentionPolicy.countBased(0));
    }

    @Test
    void shouldHaveReadableToString() {
        QueueConfig config = QueueConfig.builder("test-queue")
                .retention(QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(1)))
                .build();

        String toString = config.toString();
        assertTrue(toString.contains("test-queue"));
        assertTrue(toString.contains("retention"));
    }

    @Test
    void retentionPolicyShouldHaveReadableToString() {
        QueueConfig.RetentionPolicy timeBased = QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(1));
        assertTrue(timeBased.toString().contains("TimeBased"));

        QueueConfig.RetentionPolicy sizeBased = QueueConfig.RetentionPolicy.sizeBased(1000);
        assertTrue(sizeBased.toString().contains("SizeBased"));

        QueueConfig.RetentionPolicy countBased = QueueConfig.RetentionPolicy.countBased(1000);
        assertTrue(countBased.toString().contains("CountBased"));
    }
}
