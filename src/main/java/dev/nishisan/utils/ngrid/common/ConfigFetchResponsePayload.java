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
    private static final long serialVersionUID = 2L;

    private final ClusterPolicyConfig cluster;
    private final QueuePolicyConfig queue;
    private final List<MapPolicyConfig> maps;
    private final NodeInfo seedInfo;

    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster, QueuePolicyConfig queue, List<MapPolicyConfig> maps) {
        this(cluster, queue, maps, null);
    }

    public ConfigFetchResponsePayload(ClusterPolicyConfig cluster,
                                      QueuePolicyConfig queue,
                                      List<MapPolicyConfig> maps,
                                      NodeInfo seedInfo) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.maps = Objects.requireNonNull(maps, "maps");
        this.seedInfo = seedInfo;
    }

    public ClusterPolicyConfig cluster() {
        return cluster;
    }

    public QueuePolicyConfig queue() {
        return queue;
    }

    public List<MapPolicyConfig> maps() {
        return maps;
    }

    public NodeInfo seedInfo() {
        return seedInfo;
    }
}
