package dev.nishisan.utils.oss.cluster.placement;

import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.rebalance.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;
import static org.junit.jupiter.api.Assertions.*;

class WeightedDistributionTest {
    private StorageNodeStatus node(String id, NodeState state, long used, long capacity, DistributionMode mode, double weight) {
        return new StorageNodeStatus(id, state, 0, used, capacity, 1000, mode, weight, 0);
    }

    @Test void placementRespeitaACotaDeSeriesMesmoNoNoDeMaiorPeso() {
        // b tem peso 3 mas cota de 50 séries: depois de 50 placements ele some dos candidatos e a absorve o resto.
        var nodes = List.of(node("a", NodeState.ACTIVE, 0, 0, DistributionMode.WEIGHT, 1),
                new StorageNodeStatus("b", NodeState.ACTIVE, 0, 0, 0, 1000, DistributionMode.WEIGHT, 3, 0,
                        StorageCapabilities.ALL, null, 50, 0, null));
        Set<String> reachable = Set.of("a", "b");
        Map<String, Long> pending = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            String chosen = new LeastLoadedPlacementPolicy().choose(new PlacementContext(nodes, reachable, pending,
                    1000, Duration.ofSeconds(10), null, 4096, Map.of(), "series-" + i, null, PlacementRules.NONE))
                    .orElseThrow();
            pending.merge(chosen, 1L, Long::sum);
        }
        assertEquals(Map.of("a", 150L, "b", 50L), pending);
    }

    @Test void placementAndRebalanceConvergeToSameWeightedShares() {
        var nodes = List.of(node("a", NodeState.ACTIVE, 0, 0, DistributionMode.WEIGHT, 1),
                node("b", NodeState.ACTIVE, 0, 0, DistributionMode.WEIGHT, 3));
        Set<String> reachable = Set.of("a", "b");
        Map<String, Long> pending = new HashMap<>();
        for (int i = 0; i < 400; i++) {
            String chosen = new LeastLoadedPlacementPolicy().choose(new PlacementContext(nodes, reachable, pending,
                    1000, Duration.ofSeconds(10), null, 4096, Map.of())).orElseThrow();
            pending.merge(chosen, 1L, Long::sum);
        }
        assertEquals(Map.of("a", 100L, "b", 300L), pending);
        var keys = IntStream.range(0, 400).mapToObj(i -> "series-" + i).toList();
        Map<String, Long> sizes = new HashMap<>();
        keys.forEach(k -> sizes.put(k, 4096L));
        var plan = RebalancePlanner.plan(nodes, Map.of("a", keys), reachable, Set.of(),
                new RebalanceSettings(0, 0, 1000), sizes, Map.of(), Map.of());
        assertEquals(300, plan.size());
        var remaining = new ArrayList<>(keys);
        remaining.removeAll(plan.stream().map(Move::seriesKey).toList());
        assertTrue(RebalancePlanner.plan(nodes, Map.of("a", remaining, "b", plan.stream().map(Move::seriesKey).toList()),
                reachable, Set.of(), new RebalanceSettings(0, 0, 1000), sizes, Map.of(), Map.of()).isEmpty());
    }

    @Test void capacityFallbackAndConflictingModesAreGlobal() {
        var known = node("a", NodeState.ACTIVE, 0, 1000, DistributionMode.CAPACITY, 1);
        var unknown = node("b", NodeState.ACTIVE, 0, 0, DistributionMode.CAPACITY, 1);
        var weights = DistributionWeights.resolve(List.of(known, unknown), Set.of("a", "b"));
        assertEquals(DistributionMode.COUNT, weights.mode());
        assertEquals(weights.weight("a"), weights.weight("b"));
        var explicit = node("b", NodeState.ACTIVE, 0, 2000, DistributionMode.WEIGHT, 2);
        assertEquals(DistributionMode.COUNT, DistributionWeights.resolve(List.of(known, explicit), Set.of("a", "b")).mode());
        var larger = node("b", NodeState.ACTIVE, 0, 4000, DistributionMode.CAPACITY, 1);
        var proportional = DistributionWeights.resolve(List.of(known, larger), Set.of("a", "b"));
        assertEquals(4, proportional.weight("b") / proportional.weight("a"));
    }

    @Test void fullSmallNodeNeverReceivesRebalanceAndProjectionsAccumulate() {
        var nodes = List.of(node("source", NodeState.ACTIVE, 0, 0, DistributionMode.COUNT, 1),
                node("small", NodeState.ACTIVE, 9500, 10000, DistributionMode.COUNT, 1),
                node("room", NodeState.ACTIVE, 4000, 10000, DistributionMode.COUNT, 1));
        var keys = List.of("a", "b", "c", "d", "e", "f");
        var sizes = Map.of("a", 4096L, "b", 4096L, "c", 4096L, "d", 4096L, "e", 4096L, "f", 4096L);
        var moves = RebalancePlanner.plan(nodes, Map.of("source", keys), Set.of("source", "small", "room"),
                Set.of(), new RebalanceSettings(0, 0, 50), sizes, Map.of(), Map.of());
        assertEquals(1, moves.size());
        assertEquals("room", moves.getFirst().dst());
    }

    @Test void drainSkipsLargeOrUnknownSeriesAndContinuesWithSmallOne() {
        var nodes = List.of(node("source", NodeState.DRAINING, 0, 0, DistributionMode.COUNT, 1),
                node("target", NodeState.ACTIVE, 4096, 10000, DistributionMode.COUNT, 1));
        var moves = RebalancePlanner.plan(nodes, Map.of("source", List.of("a-large", "b-small", "c-unknown")),
                Set.of("source", "target"), Set.of(), new RebalanceSettings(0, 0, 50),
                Map.of("a-large", 8192L, "b-small", 4096L), Map.of(), Map.of());
        assertEquals(List.of(new Move("b-small", "source", "target")), moves);
    }

    @Test void pendingBytesAndRequestedGeometryBlockPlacement() {
        var nodes = List.of(node("a", NodeState.ACTIVE, 4096, 10000, DistributionMode.COUNT, 1));
        var ctx = new PlacementContext(nodes, Set.of("a"), Map.of(), 1000, Duration.ofSeconds(10), "a", 4096, Map.of("a", 4096L));
        assertTrue(new LeastLoadedPlacementPolicy().choose(ctx).isEmpty());
    }

    @Test void weightedToleranceAndInFlightSeriesUseTheProjectedTargets() {
        var nodes = List.of(node("a", NodeState.ACTIVE, 0, 0, DistributionMode.WEIGHT, 1),
                node("b", NodeState.ACTIVE, 0, 0, DistributionMode.WEIGHT, 3));
        var a = IntStream.range(0, 20).mapToObj(i -> "a-" + i).toList();
        var b = IntStream.range(0, 20).mapToObj(i -> "b-" + i).toList();
        Map<String, Long> sizes = new HashMap<>();
        a.forEach(k -> sizes.put(k, 4096L));
        b.forEach(k -> sizes.put(k, 4096L));
        var series = Map.of("a", a, "b", b);
        assertTrue(RebalancePlanner.plan(nodes, series, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(10, 0, 50), sizes, Map.of(), Map.of()).isEmpty());
        assertEquals(1, RebalancePlanner.plan(nodes, series, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0, .9, 50), sizes, Map.of(), Map.of()).size());
        assertTrue(RebalancePlanner.plan(nodes, series, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0, 0, 50), sizes, Map.of(), Map.of("b", 40L)).isEmpty());
    }
}
