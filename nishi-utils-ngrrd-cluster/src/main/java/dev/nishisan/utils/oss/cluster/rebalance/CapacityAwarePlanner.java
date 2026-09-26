package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.placement.DistributionWeights;
import dev.nishisan.utils.oss.cluster.placement.PlacementRules;
import dev.nishisan.utils.oss.storage.blob.CapacityBudget;

import java.util.*;

/**
 * Mutable state of a single pure planning invocation; never shared across cycles.
 *
 * <p>Issue #167 (item 3): cota dura e regras de placement só gateiam DESTINOS. Um nó na cota de séries
 * ({@code loads >= quotaMaxSeries}) sai de {@link #destinations()}; um destino cuja cota de bytes não
 * comporta a série falha {@link #fits}; um destino que a primeira regra casada exclui falha
 * {@link #eligible}. Uma <em>fase 0</em>, antes do drain, corrige séries que já estão num dono que as
 * regras não admitem (adoção, dono preferido, regra nova); um nó acima da cota é tratado como fonte que
 * sempre cede. Alvos ponderados (CAPACITY/WEIGHT) são calculados por <em>water-filling</em>: o alvo de um
 * nó é limitado à sua cota e o excedente é redistribuído por peso entre os nós sem teto.</p>
 */
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
    /**
     * Nós que não podem receber séries neste ciclo (issue #177: réplica do catálogo atrasada). Continuam em
     * {@link #active} — contam na distribuição alvo e podem ser origem —, só saem de {@link #destinations()}.
     */
    private final Set<String> excludedDestinations;
    private final PlacementRules rules;
    private final Map<String, String> definitionNameBySeries;
    /** Séries de drain/fase 0 não movidas porque toda opção restante estava vedada por regra. */
    private int rulesSkipped;

    CapacityAwarePlanner(Collection<StorageNodeStatus> statuses, Map<String, List<String>> series,
            Set<String> reachable, Set<String> migrating, RebalanceSettings settings, Map<String, Long> sizes,
            Map<String, Long> pendingBytes, Map<String, Long> pendingSeries, Set<String> excludedDestinations) {
        this(statuses, series, reachable, migrating, settings, sizes, pendingBytes, pendingSeries,
                excludedDestinations, PlacementRules.NONE, Map.of());
    }

    CapacityAwarePlanner(Collection<StorageNodeStatus> statuses, Map<String, List<String>> series,
            Set<String> reachable, Set<String> migrating, RebalanceSettings settings, Map<String, Long> sizes,
            Map<String, Long> pendingBytes, Map<String, Long> pendingSeries, Set<String> excludedDestinations,
            PlacementRules rules, Map<String, String> definitionNameBySeries) {
        this.settings = settings;
        this.sizes = sizes;
        this.excludedDestinations = Set.copyOf(excludedDestinations);
        this.rules = rules == null ? PlacementRules.NONE : rules;
        this.definitionNameBySeries = definitionNameBySeries == null ? Map.of() : definitionNameBySeries;
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

    /** Quantas séries de drain/fase 0 ficaram sem destino por causa das regras neste plano. */
    int rulesSkipped() { return rulesSkipped; }

    List<Move> plan() {
        // Fase 0 (issue #167): séries num dono ACTIVE que as regras não admitem vão para o primeiro destino
        // elegível que caiba — mesmo com o cluster equilibrado.
        if (!rules.isEmpty()) {
            for (String owner : active) {
                for (String key : List.copyOf(candidates.getOrDefault(owner, List.of()))) {
                    if (full()) { return List.copyOf(moves); }
                    if (eligible(key, owner)) { continue; }
                    boolean moved = false;
                    for (String target : destinations()) {
                        if (target.equals(owner) || !eligible(key, target)) { continue; }
                        if (fits(key, target)) { move(key, owner, target); moved = true; break; }
                    }
                    if (!moved) { rulesSkipped++; }
                }
            }
        }
        for (var node : nodes.values()) {
            if (node.state() != NodeState.DRAINING) { continue; }
            for (String key : List.copyOf(candidates.getOrDefault(node.nodeId(), List.of()))) {
                if (full()) { return List.copyOf(moves); }
                boolean moved = false;
                boolean ruleBlocked = false;
                for (String target : destinations()) {
                    if (!eligible(key, target)) { ruleBlocked = true; continue; }
                    if (fits(key, target)) { move(key, node.nodeId(), target); moved = true; break; }
                }
                if (!moved && ruleBlocked) { rulesSkipped++; }
            }
        }
        if (active.size() < 2) { return List.copyOf(moves); }
        double total = active.stream().mapToDouble(id -> loads.getOrDefault(id, 0L)).sum();
        Map<String, Double> targets = weightedTargets(total);
        double countThreshold = Math.max(settings.rebalanceMinDelta(), settings.rebalanceTolerance() * total / active.size());
        while (!full()) {
            boolean moved = false;
            List<String> receivers = destinations();
            if (receivers.isEmpty()) {
                break;
            }
            List<String> sources = active.stream().sorted(Comparator
                    .comparingDouble((String id) -> relativeLoad(id)).reversed().thenComparing(id -> id)).toList();
            search:
            for (String source : sources) {
                // Issue #167: uma fonte acima da própria cota de séries sempre cede — sem esperar o
                // limiar de contagem/tolerância (adoção, dono preferido ou cota reduzida a puseram lá).
                boolean overQuota = overSeriesQuota(source);
                if (!overQuota && weights.mode() == DistributionMode.COUNT
                        && loads.get(source) - loads.get(receivers.getFirst()) <= Math.max(1, countThreshold)) {
                    continue;
                }
                if (!overQuota && weights.mode() != DistributionMode.COUNT
                        && loads.get(source) - targets.get(source) <= Math.max(settings.rebalanceMinDelta(),
                                settings.rebalanceTolerance() * targets.get(source))) { continue; }
                for (String key : candidates.getOrDefault(source, List.of())) {
                    for (String target : receivers) {
                        if (source.equals(target)) { continue; }
                        if (weights.mode() == DistributionMode.COUNT) {
                            if (!overQuota && (loads.get(source) - loads.get(target) <= countThreshold
                                    || loads.get(source) - loads.get(target) <= 1)) { continue; }
                        } else {
                            double sourceDelta = loads.get(source) - targets.get(source);
                            double targetDelta = loads.get(target) - targets.get(target);
                            if (targetDelta >= 0 || Math.abs(sourceDelta - 1) + Math.abs(targetDelta + 1)
                                    >= Math.abs(sourceDelta) + Math.abs(targetDelta) - 1e-9) { continue; }
                        }
                        if (eligible(key, target) && fits(key, target)) {
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

    /**
     * Alvos proporcionais ao peso com <em>water-filling</em> pela cota de séries: um nó cujo alvo
     * ultrapassaria {@code quotaMaxSeries} fica no teto e o excedente é redistribuído por peso entre os
     * nós ainda sem teto, até nenhum estourar. Determinístico (ids ordenados; todos os que estouram numa
     * rodada são fixados juntos).
     */
    private Map<String, Double> weightedTargets(double total) {
        Map<String, Double> targets = new TreeMap<>();
        List<String> uncapped = new ArrayList<>(active);
        double remaining = total;
        while (!uncapped.isEmpty()) {
            double sumWeights = uncapped.stream().mapToDouble(weights::weight).sum();
            List<String> capped = new ArrayList<>();
            for (String id : uncapped) {
                double share = remaining * (weights.weight(id) / sumWeights);
                long quota = nodes.get(id).quotaMaxSeries();
                if (quota > 0 && share > quota) {
                    targets.put(id, (double) quota);
                    capped.add(id);
                }
            }
            if (capped.isEmpty()) {
                for (String id : uncapped) {
                    targets.put(id, remaining * (weights.weight(id) / sumWeights));
                }
                break;
            }
            for (String id : capped) { remaining -= targets.get(id); }
            uncapped.removeAll(capped);
        }
        if (uncapped.isEmpty()) {
            // Todo mundo no teto: o excedente não tem para onde ir — os alvos ficam nas cotas.
            active.forEach(id -> targets.putIfAbsent(id, (double) nodes.get(id).quotaMaxSeries()));
        }
        return targets;
    }

    private boolean overSeriesQuota(String id) {
        long quota = nodes.get(id).quotaMaxSeries();
        return quota > 0 && loads.getOrDefault(id, 0L) > quota;
    }

    private double relativeLoad(String id) { return loads.getOrDefault(id, 0L) / weights.weight(id); }

    private List<String> destinations() {
        return active.stream().filter(id -> !excludedDestinations.contains(id))
                .filter(id -> {
                    long quota = nodes.get(id).quotaMaxSeries();
                    return quota <= 0 || loads.getOrDefault(id, 0L) < quota;
                })
                .sorted(Comparator.comparingDouble(this::relativeLoad).thenComparing(id -> id)).toList();
    }

    /** Se as regras de placement admitem {@code key} em {@code target} (sem regra casada = admitido). */
    private boolean eligible(String key, String target) {
        return rules.isEmpty() || rules.exclusionReason(key, definitionNameBySeries.get(key), target).isEmpty();
    }

    private boolean fits(String key, String target) {
        long bytes = sizes.get(key);
        StorageNodeStatus node = nodes.get(target);
        // The legacy overload has no size information: only unknown-capacity targets may use it.
        if (bytes < 0 || (bytes == 0 && node.capacityBytes() > 0)) { return false; }
        long incomingBytes = incoming.getOrDefault(target, 0L);
        if (node.quotaMaxBytes() > 0 && node.usedBytes() + incomingBytes + bytes > node.quotaMaxBytes()) { return false; }
        return CapacityBudget.fits(node.capacityBytes(), node.usedBytes(), incomingBytes, bytes);
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
