package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serial;

import java.util.List;

/**
 * Configuration for the cluster policy, deserialized from YAML/JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterPolicyConfig  {
    @Serial

    private String name;
    private ReplicationConfig replication;
    private TransportConfig transport;
    private List<String> seeds; // Fallback explicit seeds if autodiscover is false
    private List<SeedNodeConfig> seedNodes;

    /** Creates a default cluster policy config. */
    public ClusterPolicyConfig() {
    }

    /**
     * Returns the cluster name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the cluster name.
     *
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the replication configuration.
     *
     * @return the replication config
     */
    public ReplicationConfig getReplication() {
        return replication;
    }

    /**
     * Sets the replication configuration.
     *
     * @param replication the replication config
     */
    public void setReplication(ReplicationConfig replication) {
        this.replication = replication;
    }

    /**
     * Returns the transport configuration.
     *
     * @return the transport config
     */
    public TransportConfig getTransport() {
        return transport;
    }

    /**
     * Sets the transport configuration.
     *
     * @param transport the transport config
     */
    public void setTransport(TransportConfig transport) {
        this.transport = transport;
    }

    /**
     * Returns the fallback seed addresses.
     *
     * @return the seeds
     */
    public List<String> getSeeds() {
        return seeds;
    }

    /**
     * Sets the fallback seed addresses.
     *
     * @param seeds the seeds
     */
    public void setSeeds(List<String> seeds) {
        this.seeds = seeds;
    }

    /**
     * Returns the seed node configurations.
     *
     * @return the seed nodes
     */
    public List<SeedNodeConfig> getSeedNodes() {
        return seedNodes;
    }

    /**
     * Sets the seed node configurations.
     *
     * @param seedNodes the seed nodes
     */
    public void setSeedNodes(List<SeedNodeConfig> seedNodes) {
        this.seedNodes = seedNodes;
    }

    /**
     * Configuration for a seed node.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SeedNodeConfig  {
        @Serial

        private String id;
        private String host;
        private int port;
        private int priority;

        /** Creates a default seed node config. */
        public SeedNodeConfig() {
        }

        /**
         * Returns the seed node leadership priority (affinity). Higher = preferred leader; ties
         * broken by node id. Defaults to {@code 0}.
         *
         * @return the leadership priority
         */
        public int getPriority() {
            return priority;
        }

        /**
         * Sets the seed node leadership priority (affinity).
         *
         * @param priority the leadership priority (higher = preferred)
         */
        public void setPriority(int priority) {
            this.priority = priority;
        }

        /**
         * Returns the seed node ID.
         *
         * @return the ID
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the seed node ID.
         *
         * @param id the ID
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * Returns the seed node host.
         *
         * @return the host
         */
        public String getHost() {
            return host;
        }

        /**
         * Sets the seed node host.
         *
         * @param host the host
         */
        public void setHost(String host) {
            this.host = host;
        }

        /**
         * Returns the seed node port.
         *
         * @return the port
         */
        public int getPort() {
            return port;
        }

        /**
         * Sets the seed node port.
         *
         * @param port the port
         */
        public void setPort(int port) {
            this.port = port;
        }
    }

    /**
     * Replication configuration.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReplicationConfig  {
        @Serial
        private int factor = 1;
        private boolean strict = false;
        private String followerIngestMode;
        private long resendLogSegmentMaxBytes = 0L;
        private int resendLogMaxSegments = 0;
        private String relayExpireAfterWrite;

        /** Creates a default replication config. */
        public ReplicationConfig() {
        }

        /**
         * Returns the replication factor.
         *
         * @return the factor
         */
        public int getFactor() {
            return factor;
        }

        /**
         * Sets the replication factor.
         *
         * @param factor the factor
         */
        public void setFactor(int factor) {
            this.factor = factor;
        }

        /**
         * Returns whether strict replication is enabled.
         *
         * @return {@code true} if strict
         */
        public boolean isStrict() {
            return strict;
        }

        /**
         * Sets whether strict replication is enabled.
         *
         * @param strict {@code true} to enable strict mode
         */
        public void setStrict(boolean strict) {
            this.strict = strict;
        }

        /**
         * Returns the follower ingest mode name ({@code RELAY_STREAM}, the only mode since 5.0.0),
         * or {@code null} when unset (defaults to {@code RELAY_STREAM}).
         *
         * @return the configured follower ingest mode name, or {@code null}
         */
        public String getFollowerIngestMode() {
            return followerIngestMode;
        }

        /**
         * Sets the follower ingest mode name. Case-insensitive; invalid or unset values fall back to
         * {@code RELAY_STREAM} at load time (the single, definitive mode).
         *
         * @param followerIngestMode the mode name ({@code RELAY_STREAM})
         */
        public void setFollowerIngestMode(String followerIngestMode) {
            this.followerIngestMode = followerIngestMode;
        }

        /**
         * Returns the per-segment byte cap for the leader-side binlog (the MySQL
         * {@code max_binlog_size} analog), in bytes. {@code 0} (default) disables byte-based rolling.
         *
         * @return the per-segment byte cap in bytes
         */
        public long getResendLogSegmentMaxBytes() {
            return resendLogSegmentMaxBytes;
        }

        /**
         * Sets the per-segment byte cap for the leader-side binlog, in bytes.
         *
         * @param resendLogSegmentMaxBytes bytes per segment ({@code 0} disables)
         */
        public void setResendLogSegmentMaxBytes(long resendLogSegmentMaxBytes) {
            this.resendLogSegmentMaxBytes = resendLogSegmentMaxBytes;
        }

        /**
         * Returns the cap on the number of binlog segment files retained per topic — the MySQL
         * "keep N binlog files" model. {@code 0} (default) disables the cap.
         *
         * @return the retained segment-count cap
         */
        public int getResendLogMaxSegments() {
            return resendLogMaxSegments;
        }

        /**
         * Sets the cap on the number of binlog segment files retained per topic.
         *
         * @param resendLogMaxSegments the retained segment-count cap ({@code 0} disables)
         */
        public void setResendLogMaxSegments(int resendLogMaxSegments) {
            this.resendLogMaxSegments = resendLogMaxSegments;
        }

        /**
         * Returns the follower relay-log write-time TTL (the MySQL relay-log expiry analog) as a
         * duration string (ISO-8601 like {@code PT2H} or simple forms like {@code 2h}/{@code 30m}),
         * or {@code null} when unset (TTL disabled).
         *
         * @return the relay TTL duration string, or {@code null}
         */
        public String getRelayExpireAfterWrite() {
            return relayExpireAfterWrite;
        }

        /**
         * Sets the follower relay-log write-time TTL as a duration string.
         *
         * @param relayExpireAfterWrite the relay TTL duration string ({@code null} disables)
         */
        public void setRelayExpireAfterWrite(String relayExpireAfterWrite) {
            this.relayExpireAfterWrite = relayExpireAfterWrite;
        }
    }

    /**
     * Transport configuration.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransportConfig  {
        @Serial
        private int workers = 2;
        private int outboundQueueCapacity = 0;
        private CompressionConfig compression;

        /** Creates a default transport config. */
        public TransportConfig() {
        }

        /**
         * Returns the number of transport workers.
         *
         * @return the worker count
         */
        public int getWorkers() {
            return workers;
        }

        /**
         * Sets the number of transport workers.
         *
         * @param workers the worker count
         */
        public void setWorkers(int workers) {
            this.workers = workers;
        }

        /**
         * Returns the per-connection outbound replication capacity
         * ({@code 0} = unbounded, the default).
         *
         * @return the outbound replication capacity
         */
        public int getOutboundQueueCapacity() {
            return outboundQueueCapacity;
        }

        /**
         * Sets the per-connection outbound replication capacity. When reached,
         * excess replication is dropped and the lagging follower recovers via the
         * gap/snapshot catch-up. Control traffic is never bounded.
         *
         * @param outboundQueueCapacity the capacity, {@code 0} = unbounded
         */
        public void setOutboundQueueCapacity(int outboundQueueCapacity) {
            this.outboundQueueCapacity = outboundQueueCapacity;
        }

        /**
         * Returns the transport compression configuration, or {@code null} when the
         * {@code compression} section is absent (defaults then apply).
         *
         * @return the compression config, or {@code null}
         */
        public CompressionConfig getCompression() {
            return compression;
        }

        /**
         * Sets the transport compression configuration.
         *
         * @param compression the compression config
         */
        public void setCompression(CompressionConfig compression) {
            this.compression = compression;
        }
    }

    /**
     * Transport-layer LZ4 compression configuration (YAML section
     * {@code cluster.transport.compression}).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CompressionConfig {
        private boolean enabled = true;
        private int minSize = 512;

        /** Creates a default compression config (enabled, 512-byte threshold). */
        public CompressionConfig() {
        }

        /**
         * Whether outbound transport frames are eligible for LZ4 compression (default
         * {@code true}).
         *
         * @return {@code true} if compression is enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Enables or disables outbound transport compression.
         *
         * @param enabled whether to compress eligible frames
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Minimum serialized JSON size (bytes) eligible for compression (default {@code 512}).
         *
         * @return the minimum payload size
         */
        public int getMinSize() {
            return minSize;
        }

        /**
         * Sets the minimum serialized JSON size (bytes) eligible for compression.
         *
         * @param minSize the minimum payload size
         */
        public void setMinSize(int minSize) {
            this.minSize = minSize;
        }
    }
}
