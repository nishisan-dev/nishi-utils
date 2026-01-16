package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterPolicyConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String name;
    private ReplicationConfig replication;
    private TransportConfig transport;
    private List<String> seeds; // Fallback explicit seeds if autodiscover is false
    private List<SeedNodeConfig> seedNodes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ReplicationConfig getReplication() {
        return replication;
    }

    public void setReplication(ReplicationConfig replication) {
        this.replication = replication;
    }

    public TransportConfig getTransport() {
        return transport;
    }

    public void setTransport(TransportConfig transport) {
        this.transport = transport;
    }

    public List<String> getSeeds() {
        return seeds;
    }

    public void setSeeds(List<String> seeds) {
        this.seeds = seeds;
    }

    public List<SeedNodeConfig> getSeedNodes() {
        return seedNodes;
    }

    public void setSeedNodes(List<SeedNodeConfig> seedNodes) {
        this.seedNodes = seedNodes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SeedNodeConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private String id;
        private String host;
        private int port;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReplicationConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int factor = 1;
        private boolean strict = false;

        public int getFactor() {
            return factor;
        }

        public void setFactor(int factor) {
            this.factor = factor;
        }

        public boolean isStrict() {
            return strict;
        }

        public void setStrict(boolean strict) {
            this.strict = strict;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransportConfig implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int workers = 2;

        public int getWorkers() {
            return workers;
        }

        public void setWorkers(int workers) {
            this.workers = workers;
        }
    }
}
