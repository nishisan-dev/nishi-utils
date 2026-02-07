package dev.nishisan.utils.ngrid.common;

import dev.nishisan.utils.ngrid.config.ClusterPolicyConfig;
import dev.nishisan.utils.ngrid.config.MapPolicyConfig;
import dev.nishisan.utils.ngrid.config.QueuePolicyConfig;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Payload sent by a seed node containing the global cluster policy.
 */
public final class ConfigFetchResponsePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 3L;

    private final ClusterPolicyConfig cluster;
    private final QueuePolicyConfig queue;
    private final List<QueuePolicyConfig> queues;
    private final List<MapPolicyConfig> maps;
    private final NodeInfo seedInfo;

    /**
     * Creates a config response payload without seed info.
     *
     * @param cluster the cluster policy config
     * @param queue   the default queue policy config
     * @param maps    the map policy configs
     */
    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster, QueuePolicyConfig queue,
            List<MapPolicyConfig> maps) {
        this(cluster, queue, queue != null ? List.of(queue) : List.of(), maps, null);
    }

    /**
     * Creates a config response payload with seed info.
     *
     * @param cluster  the cluster policy config
     * @param queue    the default queue policy config
     * @param maps     the map policy configs
     * @param seedInfo the seed node info
     */
    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster,
            QueuePolicyConfig queue,
            List<MapPolicyConfig> maps,
            NodeInfo seedInfo) {
        this(cluster, queue, queue != null ? List.of(queue) : List.of(), maps, seedInfo);
    }

    /**
     * Creates a config response payload with full configuration.
     *
     * @param cluster  the cluster policy config
     * @param queue    the default queue policy config
     * @param queues   the list of queue policy configs
     * @param maps     the map policy configs
     * @param seedInfo the seed node info, or {@code null}
     */
    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster,
            QueuePolicyConfig queue,
            List<QueuePolicyConfig> queues,
            List<MapPolicyConfig> maps,
            NodeInfo seedInfo) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.queues = Objects.requireNonNull(queues, "queues");
        this.maps = Objects.requireNonNull(maps, "maps");
        this.seedInfo = seedInfo;
    }

    /**
     * Returns the cluster policy configuration.
     *
     * @return the cluster config
     */
    public ClusterPolicyConfig cluster() {
        return cluster;
    }

    /**
     * Returns the default queue policy configuration.
     *
     * @return the queue config
     */
    public QueuePolicyConfig queue() {
        return queue;
    }

    /**
     * Returns the list of queue policy configurations.
     *
     * @return the queues
     */
    public List<QueuePolicyConfig> queues() {
        return queues;
    }

    /**
     * Returns the list of map policy configurations.
     *
     * @return the maps
     */
    public List<MapPolicyConfig> maps() {
        return maps;
    }

    /**
     * Returns the seed node info, or {@code null}.
     *
     * @return the seed info
     */
    public NodeInfo seedInfo() {
        return seedInfo;
    }
}
