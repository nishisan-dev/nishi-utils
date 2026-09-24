package dev.nishisan.utils.oss.cluster.placement;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/** Resolves one mode for the entire ACTIVE/reachable set before destination filtering. */
public record DistributionWeights(DistributionMode mode, Map<String, Double> weights) {
    private static final java.util.concurrent.atomic.AtomicReference<String> LAST_WARNING = new java.util.concurrent.atomic.AtomicReference<>();
    private static final Logger LOG = Logger.getLogger(DistributionWeights.class.getName());

    public DistributionWeights {
        weights = Map.copyOf(weights);
    }

    /** Unknown capacity or conflicting modes falls back to equal weights for everyone. */
    public static DistributionWeights resolve(Collection<StorageNodeStatus> nodes, Set<String> reachable) {
        var active = nodes.stream().filter(n -> n.state() == NodeState.ACTIVE && reachable.contains(n.nodeId())).toList();
        DistributionMode mode = active.isEmpty() ? DistributionMode.COUNT : active.getFirst().distributionMode();
        DistributionMode requested = mode;
        if (active.stream().anyMatch(n -> n.distributionMode() != requested)) {
            warnFallback("conflicting_modes");
            mode = DistributionMode.COUNT;
        } else if (mode == DistributionMode.CAPACITY && active.stream().anyMatch(n -> n.capacityBytes() <= 0)) {
            warnFallback("unknown_capacity");
            mode = DistributionMode.COUNT;
        } else {
            LAST_WARNING.set(null);
        }
        Map<String, Double> weights = new TreeMap<>();
        double max = 1;
        for (var node : active) {
            double weight = switch (mode) {
                case COUNT -> 1;
                case CAPACITY -> node.capacityBytes();
                case WEIGHT -> node.weight();
            };
            weights.put(node.nodeId(), weight);
            max = Math.max(max, weight);
        }
        // Normalization prevents overflow in the sum and preserves relative shares.
        final double scale = max;
        weights.replaceAll((id, weight) -> Math.max(Double.MIN_NORMAL, weight / scale));
        return new DistributionWeights(mode, weights);
    }

    private static void warnFallback(String reason) {
        if (!reason.equals(LAST_WARNING.getAndSet(reason))) {
            LOG.warning("NGRRD_DISTRIBUTION fallback=COUNT reason=" + reason);
        }
    }

    /** Positive effective weight for a participant. */
    public double weight(String nodeId) {
        return weights.getOrDefault(nodeId, 1.0);
    }
}
