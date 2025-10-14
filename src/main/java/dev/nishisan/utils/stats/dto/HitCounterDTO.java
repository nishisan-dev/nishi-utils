package dev.nishisan.utils.stats.dto;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class HitCounterDTO {
    private String name;
    private AtomicLong currentValue = new AtomicLong(0L);
    private Long lastCalc = 0L;
    private Double currentRate = 0D;
    private Long lastValue = 0L;
    private LocalDateTime lastUpdated = LocalDateTime.now();

    public HitCounterDTO(String name) {
        this.name = name;
        this.lastCalc = System.currentTimeMillis();
    }

    public void increment() {
        this.currentValue.incrementAndGet();
        this.lastUpdated = LocalDateTime.now();
    }

    public void increment(Long value) {
        this.currentValue.addAndGet(value);
        this.lastUpdated = LocalDateTime.now();
    }

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

    public Double getRate() {
        return this.currentRate;
    }

    public Long getValue() {
        return this.currentValue.get();
    }

    /**
     * @return the lastUpdated
     */
    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

}
