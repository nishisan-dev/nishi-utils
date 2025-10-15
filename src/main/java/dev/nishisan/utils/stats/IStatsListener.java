package dev.nishisan.utils.stats;

import dev.nishisan.utils.stats.dto.HitCounterDTO;
import dev.nishisan.utils.stats.dto.SimpleValueDTO;
import dev.nishisan.utils.stats.list.FixedSizeList;

/**
 * An interface for listening to various events related to statistics counters.
 * This listener provides callback methods invoked during the lifecycle of
 * different statistics counters such as average counters, current value counters,
 * and hit counters. Implementations can use these callbacks to perform specific
 * actions when the respective events occur.
 */
public interface IStatsListener {
    /**
     * Callback method triggered when an average counter is created.
     *
     * @param list the FixedSizeList instance representing the container for the counter's data.
     *             This list has a fixed capacity and ensures older elements are removed as new ones are added when at capacity.
     */
    public void onAverageCounterCreated(FixedSizeList list);

    /**
     * Callback method triggered when a current value counter is created.
     *
     * @param value the SimpleValueDTO instance representing the created counter,
     *              containing information such as the counter's name and its initial value.
     */
    public void onCurrentValueCounterCreated(SimpleValueDTO value);

    /**
     * Callback method triggered when a hit counter is created.
     *
     * @param metric the MetricDTO instance that represents the hit counter.
     *               This object contains details such as the counter's name, current value,
     *               last calculation time, current rate, and the last update timestamp.
     */
    public void onHitCounterCreated(HitCounterDTO metric);


    /**
     * Callback method triggered when the value of a hit counter is incremented.
     *
     * @param metric the HitCounterDTO instance representing the hit counter
     *               that has been incremented. This object contains details such as the counter's name,
     *               current value, last calculation time, current rate, last value, and the timestamp
     *               of the last update.
     */
    public void onHitCounterIncremented(HitCounterDTO metric);

    /**
     * Callback method triggered when a hit counter is removed.
     *
     * @param metric the HitCounterDTO instance representing the hit counter
     *               that was removed. This object contains information such as the counter's name,
     *               current value, last calculation time, current rate, last value,
     *               and the timestamp of the last update.
     */
    public void onHitCounterRemoved(HitCounterDTO metric);
}
