package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Configuration for the cluster policy, deserialized from YAML/JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterPolicyConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

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
    public static class SeedNodeConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private String id;
        private String host;
        private int port;

        /** Creates a default seed node config. */
        public SeedNodeConfig() {
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
    public static class ReplicationConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int factor = 1;
        private boolean strict = false;

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
    }

    /**
     * Transport configuration.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransportConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int workers = 2;

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
    }
}
