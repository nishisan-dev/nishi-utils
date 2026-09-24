package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.placement.DistributionWeights;
import dev.nishisan.utils.oss.storage.blob.CapacityBudget;

import java.util.*;

/** Mutable state of a single pure planning invocation; never shared across cycles. */
final class CapacityAwarePlanner {
    private final Map<String, StorageNodeStatus> nodes = new TreeMap<>();
    private final Map<String, Long> loads = new TreeMap<>();
    private final Map<String, Long> incoming = new HashMap<>();
    private final Map<String, List<String>> candidates = new TreeMap<>();
    private final Map<String, Long> sizes;
    private final RebalanceSettings settings;
    private final DistributionWeights weights;
    private final List<Move> moves = new ArrayList<>();
    private final List<String> active;

    CapacityAwarePlanner(Collection<StorageNodeStatus> statuses, Map<String, List<String>> series,
            Set<String> reachable, Set<String> migrating, RebalanceSettings settings, Map<String, Long> sizes,
            Map<String, Long> pendingBytes, Map<String, Long> pendingSeries) {
        this.settings = settings;
        this.sizes = sizes;
        statuses.forEach(n -> {
            nodes.put(n.nodeId(), n);
            loads.put(n.nodeId(), pendingSeries.getOrDefault(n.nodeId(), 0L));
            incoming.put(n.nodeId(), Math.max(n.reservedBytes(), pendingBytes.getOrDefault(n.nodeId(), 0L)));
        });
        series.forEach((owner, keys) -> {
            var owned = keys.stream().filter(k -> !migrating.contains(k)).sorted().toList();
            loads.merge(owner, (long) owned.size(), Long::sum);
            candidates.put(owner, new ArrayList<>(owned.stream().filter(sizes::containsKey).toList()));
        });
        weights = DistributionWeights.resolve(statuses, reachable);
        active = nodes.values().stream().filter(n -> n.state() == NodeState.ACTIVE && reachable.contains(n.nodeId()))
                .map(StorageNodeStatus::nodeId).toList();
    }

    List<Move> plan() {
        for (var node : nodes.values()) {
            if (node.state() != NodeState.DRAINING) { continue; }
            for (String key : List.copyOf(candidates.getOrDefault(node.nodeId(), List.of()))) {
                if (full()) { return List.copyOf(moves); }
                for (String target : destinations()) {
                    if (fits(key, target)) { move(key, node.nodeId(), target); break; }
                }
            }
        }
        if (active.size() < 2) { return List.copyOf(moves); }
        double total = active.stream().mapToDouble(id -> loads.getOrDefault(id, 0L)).sum();
        double sumWeights = active.stream().mapToDouble(weights::weight).sum();
        Map<String, Double> targets = new HashMap<>();
        active.forEach(id -> targets.put(id, total * (weights.weight(id) / sumWeights)));
        double countThreshold = Math.max(settings.rebalanceMinDelta(), settings.rebalanceTolerance() * total / active.size());
        while (!full()) {
            boolean moved = false;
            List<String> receivers = destinations();
            List<String> sources = active.stream().sorted(Comparator
                    .comparingDouble((String id) -> relativeLoad(id)).reversed().thenComparing(id -> id)).toList();
            search:
            for (String source : sources) {
                if (weights.mode() == DistributionMode.COUNT
                        && loads.get(source) - loads.get(receivers.getFirst()) <= Math.max(1, countThreshold)) {
                    continue;
                }
                if (weights.mode() != DistributionMode.COUNT
                        && loads.get(source) - targets.get(source) <= Math.max(settings.rebalanceMinDelta(),
                                settings.rebalanceTolerance() * targets.get(source))) { continue; }
                for (String key : candidates.getOrDefault(source, List.of())) {
                    for (String target : receivers) {
                        if (source.equals(target)) { continue; }
                        if (weights.mode() == DistributionMode.COUNT) {
                            if (loads.get(source) - loads.get(target) <= countThreshold
                                    || loads.get(source) - loads.get(target) <= 1) { continue; }
                        } else {
                            double sourceDelta = loads.get(source) - targets.get(source);
                            double targetDelta = loads.get(target) - targets.get(target);
                            if (targetDelta >= 0 || Math.abs(sourceDelta - 1) + Math.abs(targetDelta + 1)
                                    >= Math.abs(sourceDelta) + Math.abs(targetDelta) - 1e-9) { continue; }
                        }
                        if (fits(key, target)) {
                            move(key, source, target);
                            moved = true;
                            break search;
                        }
                    }
                }
            }
            if (!moved) { break; }
        }
        return List.copyOf(moves);
    }

    private double relativeLoad(String id) { return loads.getOrDefault(id, 0L) / weights.weight(id); }

    private List<String> destinations() {
        return active.stream().sorted(Comparator.comparingDouble(this::relativeLoad).thenComparing(id -> id)).toList();
    }

    private boolean fits(String key, String target) {
        long bytes = sizes.get(key);
        StorageNodeStatus node = nodes.get(target);
        // The legacy overload has no size information: only unknown-capacity targets may use it.
        if (bytes < 0 || (bytes == 0 && node.capacityBytes() > 0)) { return false; }
        return CapacityBudget.fits(node.capacityBytes(), node.usedBytes(), incoming.getOrDefault(target, 0L), bytes);
    }

    private void move(String key, String source, String target) {
        moves.add(new Move(key, source, target));
        candidates.get(source).remove(key);
        loads.merge(source, -1L, Long::sum);
        loads.merge(target, 1L, Long::sum);
        incoming.merge(target, sizes.get(key), Math::addExact);
        // Outgoing regions remain charged until the source confirms FINISH.
    }

    private boolean full() { return moves.size() >= settings.maxMovesPerCycle(); }
}
