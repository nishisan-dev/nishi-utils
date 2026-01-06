package dev.nishisan.utils.stats;

import dev.nishisan.utils.stats.dto.HitCounterDTO;
import dev.nishisan.utils.stats.dto.SimpleValueDTO;
import dev.nishisan.utils.stats.list.FixedSizeList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class StatsUtilsTest {

    private StatsUtils statsUtils;

    @BeforeEach
    void setUp() {
        statsUtils = new StatsUtils();
    }

    @AfterEach
    void tearDown() {
        statsUtils.shutdown();
    }

    @Test
    void testNotifyHitCounter() {
        String counterName = "testCounter";
        statsUtils.notifyHitCounter(counterName);
        assertEquals(1L, statsUtils.getCounterValue(counterName));

        statsUtils.notifyHitCounter(counterName);
        assertEquals(2L, statsUtils.getCounterValue(counterName));
    }

    @Test
    void testGetCounterValueNotFound() {
        assertEquals(-1L, statsUtils.getCounterValue("nonExistent"));
    }

    @Test
    void testGetCounterValueOrNull() {
        assertNull(statsUtils.getCounterValueOrNull("nonExistent"));
        statsUtils.notifyHitCounter("test");
        assertEquals(1L, statsUtils.getCounterValueOrNull("test"));
    }

    @Test
    void testNotifyCurrentValue() {
        String valueName = "testValue";
        statsUtils.notifyCurrentValue(valueName, 100L);
        // StatsUtils doesn't have a direct getter for CurrentValue in the public API exposed for reading?
        // Checking the code: public void notifyCurrentValue... but no getValue?
        // Wait, looking at StatsUtils.java, there is no getValue(String name).
        // It only logs them in calcStats.
        // But let's check if we can verify via listener.
    }

    @Test
    void testNotifyAverageCounter() {
        String avgName = "testAvg";
        statsUtils.notifyAverageCounter(avgName, 10L);
        statsUtils.notifyAverageCounter(avgName, 20L);

        assertEquals(15.0, statsUtils.getAverage(avgName));
    }

    @Test
    void testGetAverageNotFound() {
        assertEquals(-1.0, statsUtils.getAverage("nonExistent"));
    }

    @Test
    void testGetAverageOrNull() {
        assertNull(statsUtils.getAverageOrNull("nonExistent"));
        statsUtils.notifyAverageCounter("test", 10L);
        assertEquals(10.0, statsUtils.getAverageOrNull("test"));
    }

    @Test
    void testHumanSize() {
        assertEquals("500 B", statsUtils.humanSize(500));
        assertEquals("1.0 kB", statsUtils.humanSize(1000));
        assertEquals("1.5 kB", statsUtils.humanSize(1500));
        assertEquals("1.0 MB", statsUtils.humanSize(1_000_000));
    }

    @Test
    void testGetDateFromMsec() {
        // 1 day, 1 hour, 1 minute, 1 second = 86400 + 3600 + 60 + 1 = 90061 seconds = 90061000 ms
        long msec = 90061000L;
        String result = statsUtils.getDateFromMsec(msec);
        assertEquals("[1] - 01:01:01", result);

        // Test 0
         assertEquals("[0] - 00:00:00", statsUtils.getDateFromMsec(0));
    }

    @Test
    void testListener() {
        AtomicBoolean hitCreated = new AtomicBoolean(false);
        AtomicBoolean hitIncremented = new AtomicBoolean(false);
        AtomicBoolean avgCreated = new AtomicBoolean(false);
        AtomicBoolean avgAdded = new AtomicBoolean(false);
        AtomicBoolean valCreated = new AtomicBoolean(false);
        AtomicBoolean valUpdated = new AtomicBoolean(false);

        statsUtils.registerListener(new IStatsListener() {
            @Override
            public void onAverageCounterCreated(FixedSizeList list) {
                avgCreated.set(true);
            }

            @Override
            public void onAverageCounterValueAdded(FixedSizeList list) {
                avgAdded.set(true);
            }

            @Override
            public void onCurrentValueCounterCreated(SimpleValueDTO value) {
                valCreated.set(true);
            }

            @Override
            public void onCurrentValueCounterUpdated(SimpleValueDTO value) {
                valUpdated.set(true);
            }

            @Override
            public void onHitCounterCreated(HitCounterDTO metric) {
                hitCreated.set(true);
            }

            @Override
            public void onHitCounterIncremented(HitCounterDTO metric) {
                hitIncremented.set(true);
            }

            @Override
            public void onHitCounterRemoved(HitCounterDTO metric) {

            }
        });

        statsUtils.notifyHitCounter("hit");
        assertTrue(hitCreated.get());
        statsUtils.notifyHitCounter("hit");
        assertTrue(hitIncremented.get());

        statsUtils.notifyAverageCounter("avg", 10L);
        assertTrue(avgCreated.get());
        statsUtils.notifyAverageCounter("avg", 20L);
        assertTrue(avgAdded.get());

        statsUtils.notifyCurrentValue("val", 100L);
        assertTrue(valCreated.get());
        statsUtils.notifyCurrentValue("val", 200L);
        assertTrue(valUpdated.get());
    }
}
