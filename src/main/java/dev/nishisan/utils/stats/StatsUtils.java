package dev.nishisan.utils.stats;

import dev.nishisan.utils.stats.dto.HitCounterDTO;
import dev.nishisan.utils.stats.dto.SimpleValueDTO;
import dev.nishisan.utils.stats.list.FixedSizeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.CharacterIterator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Utility class for monitoring statistics and metrics, such as counters, averages, and memory usage.
 * Provides functionality to update, retrieve, and calculate various statistical data points.
 * It also supports notifying listeners about updates and changes to these statistics.
 */
public class StatsUtils {

    private static final Logger logger = LoggerFactory.getLogger(StatsUtils.class);
    private final ConcurrentMap<String, HitCounterDTO> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SimpleValueDTO> values = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FixedSizeList<Long>> averages = new ConcurrentHashMap<>();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    private Long usedMemory = 0L;
    private List<IStatsListener> listeners = new ArrayList<>();

    public StatsUtils() {
        this.startStatsThread();
    }

    /**
     * Notifies the system about an average counter update or creation.
     * If the specified counter name does not exist in the averages map,
     * a new counter is created with a FixedSizeList of capacity 10,
     * listeners are notified, and the provided value is added to the list.
     * If the counter already exists, the specified value is added to the existing list.
     *
     * @param name  the name of the average counter
     * @param value the value to be added to the average counter
     */
    public void notifyAverageCounter(String name, Long value) {
        if (!this.averages.containsKey(name)) {
            FixedSizeList<Long> list = new FixedSizeList<Long>(name, 10);
            this.averages.put(name, list);
            this.listeners.forEach(l -> l.onAverageCounterCreated(list));
            list.add(value);
        } else {
            this.averages.get(name).add(value);
        }
    }

    /**
     * Notifies the system about an average counter update or creation.
     * If the specified counter name does not exist in the averages map,
     * a new counter is created with a FixedSizeList of capacity 10,
     * listeners are notified, and the provided value is added to the list.
     * If the counter already exists, the specified value is added to the existing list.
     *
     * @param name  the name of the average counter
     * @param value the value to be added to the average counter
     */
    public void notifyAverageCounter(String name, Integer value) {
        this.notifyAverageCounter(name, value.longValue());
    }

    /**
     * Notifies the system of a current value update or creation. If the specified name
     * is not already present in the values map, a new SimpleValueDTO will be created
     * and added to the map. Additionally, all registered listeners will be notified
     * of the new counter creation. If the name already exists in the map, the existing
     * SimpleValueDTO value will be updated.
     *
     * @param name  the unique identifier for the value to be notified or updated
     * @param value the value to be associated with the specified name
     */
    public void notifyCurrentValue(String name, Long value) {
        SimpleValueDTO simplesValueDTO = new SimpleValueDTO(name, value);
        if (!this.values.containsKey(name)) {
            this.values.put(name, simplesValueDTO);
            this.listeners.forEach(l -> l.onCurrentValueCounterCreated(simplesValueDTO));
        } else {
            this.values.get(name).setValue(value);
        }

    }

    /**
     * Notifies the system of a current value update or creation. This method acts
     * as a wrapper, converting the provided Integer value to a Long before delegating
     * the task to the overloaded method.
     *
     * @param name the unique identifier for the value to be notified or updated
     * @param l    the Integer value to be associated with the specified name,
     *             which will be converted to Long
     */
    public void notifyCurrentValue(String name, Integer l) {
        this.notifyCurrentValue(name, l.longValue());
    }


    /**
     * Retrieves the current value of a specific hit counter based on its name.
     * If the counter with the specified name exists, its value is returned.
     * If the counter is not found, a warning is logged, and -1 is returned.
     *
     * @param counterName the name of the counter whose value is to be retrieved
     * @return the current value of the counter if it exists, or -1 if the counter is not found
     */
    public Long getCounterValue(String counterName) {
        if (this.counters.containsKey(counterName)) {
            HitCounterDTO metric = this.counters.get(counterName);
            return metric.getValue();
        } else {
            logger.warn(String.format("Counter:[%s] Not Found", counterName));
        }
        return -1L;
    }


    /**
     * Retrieves the rate of a specified hit counter by its name.
     * If the counter exists, its current rate is returned.
     * If the counter does not exist, a warning is logged, and -1.0 is returned.
     *
     * @param counterName the name of the hit counter whose rate is to be retrieved
     * @return the current rate of the counter if it exists, or -1.0 if the counter is not found
     */
    public Double getCounterRate(String counterName) {
        if (this.counters.containsKey(counterName)) {
            HitCounterDTO metric = this.counters.get(counterName);
            return metric.getRate();
        } else {
            logger.warn(String.format("Counter:[%s] Not Found", counterName));
            return -1D;
        }
    }

    /**
     * Retrieves the average value of a specified average counter.
     * If the average counter with the given name exists, the method calculates
     * and returns the average of the values stored in the associated FixedSizeList.
     * If the counter does not exist, a warning is logged, and -1.0 is returned.
     *
     * @param name the name of the average counter whose average value is to be retrieved
     * @return the calculated average value as a Double if the counter exists,
     * or -1.0 if the counter is not found
     */
    public Double getAverage(String name) {
        if (this.averages.containsKey(name)) {
            FixedSizeList<Long> reads = this.averages.get(name);
            return reads.stream().mapToDouble(a -> a).average().orElse(0.0);
        } else {
            logger.warn(String.format("Average:[%s] Not Found", name));
            return -1D;
        }
    }

    /**
     * Notifies the system of a hit counter update or creation. If the specified counter
     * name already exists in the counters map, it increments the counter's value.
     * Otherwise, a new hit counter is created and added to the counters map, and all
     * registered listeners are notified of the new hit counter creation.
     *
     * @param counter the name of the hit counter to be updated or created
     */
    public void notifyHitCounter(String counter) {
        HitCounterDTO metric;
        if (this.counters.containsKey(counter)) {
            metric = this.counters.get(counter);
            metric.increment();
            this.listeners.forEach(l -> l.onHitCounterIncremented(metric));
            this.counters.replace(counter, metric);
        } else {
            metric = new HitCounterDTO(counter);
            this.counters.put(counter, metric);
            this.listeners.forEach(l -> l.onHitCounterCreated(metric));
        }
    }

    /**
     * Creates a thread to call calcStats() every 10 seconds.
     */
    public void startStatsThread() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    caclcStats();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    /**
     * Wrapper method for calculating and logging system statistics.
     * This method invokes the main {@code calcStats} method with the default parameter,
     * setting {@code print} to {@code false}. It is used for internal statistics
     * calculation without logging the results.
     */
    public void caclcStats() {
        calcStats(false);
    }

    /**
     * Calculates and logs system statistics including memory usage, counters, averages, and values.
     * It also performs maintenance tasks such as removing expired counters.
     * This method operates dynamically and logs information based on the provided input parameter.
     *
     * @param print a boolean indicating whether the calculated statistics should be logged.
     *              If true, detailed statistics are printed using the logger; otherwise, statistics are calculated but not logged.
     */
    public void calcStats(boolean print) {

        // Get the Java runtime
        Runtime runtime = Runtime.getRuntime();
        usedMemory = runtime.totalMemory() - runtime.freeMemory();

        if (!this.counters.isEmpty()) {
            if (print)
                logger.debug(" ---------------------------------------------------------------------------------------------");

            this.counters.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach((entry) -> {
                String k = entry.getKey();
                HitCounterDTO v = entry.getValue();
                v.calc();
                if (print)
                    logger.debug(String.format("  Stats:  [%-35s]:=[%10.3f]/s Current Value:(%11d)", k, round(v.getRate(), 3), v.getValue()));
            });
        } else {
            if (print)
                logger.warn("Empty Stats Received");
        }
        if (!this.values.isEmpty()) {
            if (print)
                logger.debug(" ---------------------------------------------------------------------------------------------");
        }
        this.values.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach((entry) -> {
            if (print)
                logger.debug(String.format("  Value:  [%-35s]:=[%10d]", entry.getKey(), entry.getValue()));
        });
        if (!this.averages.isEmpty()) {
            if (print)
                logger.debug(" ---------------------------------------------------------------------------------------------");
        }
        this.averages.forEach((k, v) -> {
            try {
                Double avg = v.stream().mapToDouble(a -> a).average().orElse(0.0);
                if (print)
                    logger.debug(String.format("  Average: [%-34s]:=[%10.3f]", k, round(avg, 3)));
            } catch (Exception ex) {
                //
                // É seguro ignorar esse erro já que é só stats :)
                //
            }
        });
        if (print) {
            logger.debug(" ----------------------------------------------------------------------------------------");
            logger.debug(String.format("  Memory:  [%-34s]:=[%14s]", "Heap", humanSize(usedMemory)));
        }
        /**
         * Faz a manutençao dos contadores expirados
         */
        LocalDateTime expiredRef = LocalDateTime.now().minusHours(3);
        List<String> expiredCounter = new ArrayList<>();
        this.counters.forEach((k, v) -> {
            if (v.getLastUpdated().isBefore(expiredRef)) {
                expiredCounter.add(k);
            }
        });
        if (!expiredCounter.isEmpty()) {
            logger.debug(" ----------------------------------------------------------------------------------------");
            expiredCounter.forEach(k -> {
                HitCounterDTO removed = this.counters.remove(k);
                this.listeners.forEach(l -> l.onHitCounterRemoved(removed));
                if (print)
                    logger.debug("Counter:[{}] Is Expired...removing", k);
            });
        }

    }


    /**
     * Converts a given byte size into a human-readable format using SI prefixes,
     * such as kB, MB, GB, etc. The method scales down the size in increments of
     * 1000 until it fits within the range [-999.950, 999.950).
     *
     * @param bytes the size in bytes to be converted
     * @return a human-readable string representation of the input size with
     * an appropriate SI prefix
     */
    public String humanSize(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }

    /**
     * Converts a given time difference in milliseconds into a formatted string representation
     * showing days, hours, minutes, and seconds.
     *
     * @param diffMSec the time difference in milliseconds to be converted
     * @return a formatted string representing the time in the format "[days] - HH:mm:ss"
     */
    public String getDateFromMsec(long diffMSec) {
        int left = 0;
        int ss = 0;
        int mm = 0;
        int hh = 0;
        int dd = 0;
        left = (int) (diffMSec / 1000);
        ss = left % 60;
        left = (int) left / 60;
        if (left > 0) {
            mm = left % 60;
            left = (int) left / 60;
            if (left > 0) {
                hh = left % 24;
                left = (int) left / 24;
                if (left > 0) {
                    dd = left;
                }
            }
        }
        String formated = String.format("[%d] - %02d:%02d:%02d", dd, hh, mm, ss);

        return formated;
    }

    /**
     * Rounds a given decimal value to the specified number of places.
     * The rounding mode used is {@code BigDecimal.ROUND_HALF_UP}.
     *
     * @param value  the decimal value to be rounded
     * @param places the number of decimal places to round to
     * @return the rounded value as a double
     */
    private double round(double value, int places) {
        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }


    public void registerListener(IStatsListener listener) {
        if (!this.listeners.contains(listener)) {
            this.listeners.add(listener);
        }

    }
}
