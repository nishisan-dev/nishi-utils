package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.ngrid.config.ClusterPolicyConfig;
import dev.nishisan.utils.ngrid.config.MapPolicyConfig;
import dev.nishisan.utils.ngrid.config.QueuePolicyConfig;

import java.util.List;
import java.util.Objects;

/**
 * Payload sent by a seed node containing the global cluster policy.
 */
public final class ConfigFetchResponsePayload {

    private final ClusterPolicyConfig cluster;
    private final QueuePolicyConfig queue;
    private final List<QueuePolicyConfig> queues;
    private final List<MapPolicyConfig> maps;
    private final NodeInfo seedInfo;

    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster, QueuePolicyConfig queue,
            List<MapPolicyConfig> maps) {
        this(cluster, queue, queue != null ? List.of(queue) : List.of(), maps, null);
    }

    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster,
            QueuePolicyConfig queue,
            List<MapPolicyConfig> maps,
            NodeInfo seedInfo) {
        this(cluster, queue, queue != null ? List.of(queue) : List.of(), maps, seedInfo);
    }

    @JsonCreator
    public ConfigFetchResponsePayload(
            @JsonProperty("cluster") ClusterPolicyConfig cluster,
            @JsonProperty("queue") QueuePolicyConfig queue,
            @JsonProperty("queues") List<QueuePolicyConfig> queues,
            @JsonProperty("maps") List<MapPolicyConfig> maps,
            @JsonProperty("seedInfo") NodeInfo seedInfo) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.queues = Objects.requireNonNull(queues, "queues");
        this.maps = Objects.requireNonNull(maps, "maps");
        this.seedInfo = seedInfo;
    }

    public ClusterPolicyConfig cluster() {
        return cluster;
    }

    public QueuePolicyConfig queue() {
        return queue;
    }

    public List<QueuePolicyConfig> queues() {
        return queues;
    }

    public List<MapPolicyConfig> maps() {
        return maps;
    }

    public NodeInfo seedInfo() {
        return seedInfo;
    }
}
