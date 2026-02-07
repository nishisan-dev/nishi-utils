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

package dev.nishisan.utils.stats.dto;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DTO that tracks a counter and its calculated hit rate over time.
 */
public class HitCounterDTO {
    private String name;
    private AtomicLong currentValue = new AtomicLong(0L);
    private Long lastCalc = 0L;
    private Double currentRate = 0D;
    private Long lastValue = 0L;
    private LocalDateTime lastUpdated = LocalDateTime.now();

    /**
     * Creates a new hit counter with the given name.
     *
     * @param name the counter name
     */
    public HitCounterDTO(String name) {
        this.name = name;
        this.lastCalc = System.currentTimeMillis();
    }

    /**
     * Increments the counter by one.
     */
    public void increment() {
        this.currentValue.incrementAndGet();
        this.lastUpdated = LocalDateTime.now();
    }

    /**
     * Increments the counter by the given value.
     *
     * @param value the value to add
     */
    public void increment(Long value) {
        this.currentValue.addAndGet(value);
        this.lastUpdated = LocalDateTime.now();
    }

    /**
     * Calculates the current hit rate based on elapsed time.
     *
     * @return the current rate (hits per second)
     */
    public Double calc() {
        Long now = System.currentTimeMillis();
        Long deltaT = now - this.lastCalc;
        this.lastCalc = now;
        Double secondsElapsed = deltaT.doubleValue() / 1000;
        Long currentValue = this.currentValue.get();
        Long deltaV = currentValue - this.lastValue;

        this.currentRate = deltaV / secondsElapsed;

        this.lastValue = currentValue;
        this.lastCalc = System.currentTimeMillis();
        return this.currentRate;
    }

    /**
     * Returns the current rate.
     *
     * @return the rate
     */
    public Double getRate() {
        return this.currentRate;
    }

    /**
     * Returns the total counter value.
     *
     * @return the total value
     */
    public Long getValue() {
        return this.currentValue.get();
    }

    /**
     * Returns the timestamp of the last update.
     *
     * @return the last updated timestamp
     */
    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    /**
     * Returns the counter name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the current atomic value.
     *
     * @return the current value
     */
    public AtomicLong getCurrentValue() {
        return currentValue;
    }

    /**
     * Returns the last calculation timestamp.
     *
     * @return the last calculation time in millis
     */
    public Long getLastCalc() {
        return lastCalc;
    }

    /**
     * Returns the current calculated rate.
     *
     * @return the current rate
     */
    public Double getCurrentRate() {
        return currentRate;
    }

    /**
     * Returns the last recorded value.
     *
     * @return the last value
     */
    public Long getLastValue() {
        return lastValue;
    }
}
