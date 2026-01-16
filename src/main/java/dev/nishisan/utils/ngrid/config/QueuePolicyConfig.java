package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueuePolicyConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    
    private String name; // queue-name in storage section from example, moved here for clarity or keep in root?
                         // The example had storage -> queue-name. Let's assume it's part of policy for now or global.
                         // Actually, queue name is usually global for the grid node.
    
    private RetentionConfig retention;
    private PerformanceConfig performance;
    private CompactionConfig compaction;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RetentionConfig getRetention() {
        return retention;
    }

    public void setRetention(RetentionConfig retention) {
        this.retention = retention;
    }

    public PerformanceConfig getPerformance() {
        return performance;
    }

    public void setPerformance(PerformanceConfig performance) {
        this.performance = performance;
    }

    public CompactionConfig getCompaction() {
        return compaction;
    }

    public void setCompaction(CompactionConfig compaction) {
        this.compaction = compaction;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetentionConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private String policy; // TIME_BASED, DELETE_ON_CONSUME
        private String duration; // "24h"

        public String getPolicy() {
            return policy;
        }

        public void setPolicy(String policy) {
            this.policy = policy;
        }

        public String getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            this.duration = duration;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PerformanceConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private boolean fsync;
        @JsonProperty("memory-buffer")
        private MemoryBufferConfig memoryBuffer;
        @JsonProperty("short-circuit")
        private boolean shortCircuit = true;

        public boolean isFsync() {
            return fsync;
        }

        public void setFsync(boolean fsync) {
            this.fsync = fsync;
        }

        public MemoryBufferConfig getMemoryBuffer() {
            return memoryBuffer;
        }

        public void setMemoryBuffer(MemoryBufferConfig memoryBuffer) {
            this.memoryBuffer = memoryBuffer;
        }

        public boolean isShortCircuit() {
            return shortCircuit;
        }

        public void setShortCircuit(boolean shortCircuit) {
            this.shortCircuit = shortCircuit;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MemoryBufferConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private boolean enabled;
        private int size;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CompactionConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private double threshold;
        private String interval;

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }

        public String getInterval() {
            return interval;
        }

        public void setInterval(String interval) {
            this.interval = interval;
        }
    }
}
