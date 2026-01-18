package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NGridYamlConfig {

    @JsonProperty("node")
    private NodeIdentityConfig node;

    @JsonProperty("autodiscover")
    private AutodiscoverConfig autodiscover;

    @JsonProperty("cluster")
    private ClusterPolicyConfig cluster;

    @JsonProperty("queue")
    private QueuePolicyConfig queue;

    @JsonProperty("queues")
    private List<QueuePolicyConfig> queues = Collections.emptyList();

    @JsonProperty("maps")
    private List<MapPolicyConfig> maps = Collections.emptyList();

    public NodeIdentityConfig getNode() {
        return node;
    }

    public void setNode(NodeIdentityConfig node) {
        this.node = node;
    }

    public AutodiscoverConfig getAutodiscover() {
        return autodiscover;
    }

    public void setAutodiscover(AutodiscoverConfig autodiscover) {
        this.autodiscover = autodiscover;
    }

    public ClusterPolicyConfig getCluster() {
        return cluster;
    }

    public void setCluster(ClusterPolicyConfig cluster) {
        this.cluster = cluster;
    }

    public QueuePolicyConfig getQueue() {
        return queue;
    }

    public void setQueue(QueuePolicyConfig queue) {
        this.queue = queue;
    }

    public List<QueuePolicyConfig> getQueues() {
        return queues;
    }

    public void setQueues(List<QueuePolicyConfig> queues) {
        this.queues = queues;
    }

    public List<MapPolicyConfig> getMaps() {
        return maps;
    }

    public void setMaps(List<MapPolicyConfig> maps) {
        this.maps = maps;
    }
}
