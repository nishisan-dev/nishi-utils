/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Planejador puro do rebalanceamento (estágio 2): a partir de um snapshot do
 * catálogo, decide quais séries mover e para onde — sem tocar rede, catálogo
 * nem estado do processo. Determinístico: o mesmo snapshot de entrada sempre
 * produz a mesma lista de movimentos, na mesma ordem, independente da ordem de
 * iteração de {@link Collection}/{@link Map} recebidos (todas as chaves de
 * decisão — séries e nós — são ordenadas antes de decidir).
 *
 * <ol>
 *   <li>Nós {@link NodeState#DRAINING} (alcançáveis ou não): todas as séries deles entram na fila de
 *       saída, uma por vez, para o nó {@link NodeState#ACTIVE} alcançável de menor carga corrente
 *       (recalculada a cada movimento planejado). Sem destino disponível, nenhum movimento de
 *       drenagem é planejado (nem para este nó, nem para os seguintes — falta de capacidade é global
 *       neste instante).</li>
 *   <li>Nós {@link NodeState#ACTIVE} alcançáveis: enquanto a diferença entre o mais e o menos
 *       carregado exceder {@code max(rebalanceMinDelta, rebalanceTolerance × média)}, move a série de
 *       menor chave do nó mais carregado para o menos carregado.</li>
 *   <li>Chaves em {@code migratingKeys} nunca são movidas de novo.</li>
 * </ol>
 *
 * <p>A lista final nunca excede {@link RebalanceSettings#maxMovesPerCycle()}.</p>
 */
public final class RebalancePlanner {

    private RebalancePlanner() {
    }

    /**
     * @param nodes          status conhecido (visão local do líder) de todos os storage nodes
     * @param seriesByOwner  séries {@code ACTIVE}, agrupadas pelo dono atual (ver
     *                       {@code CatalogService#seriesByOwnerLocal})
     * @param reachable      nós atualmente alcançáveis segundo o {@code ClusterCoordinator}/{@code Transport}
     * @param migratingKeys  chaves de série já em migração — nunca replanejadas
     * @param settings       limites do ciclo
     * @return movimentos planejados, na ordem em que devem ser submetidos
     */
    public static List<Move> plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, RebalanceSettings settings) {
        Map<String, NodeState> stateByNode = new HashMap<>();
        Map<String, Long> loadByNode = new HashMap<>();
        for (StorageNodeStatus node : nodes) {
            stateByNode.put(node.nodeId(), node.state());
            loadByNode.putIfAbsent(node.nodeId(), 0L);
        }

        Map<String, Deque<String>> queueByOwner = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : seriesByOwner.entrySet()) {
            List<String> keys = new ArrayList<>();
            for (String key : entry.getValue()) {
                if (!migratingKeys.contains(key)) {
                    keys.add(key);
                }
            }
            keys.sort(Comparator.naturalOrder());
            queueByOwner.put(entry.getKey(), new ArrayDeque<>(keys));
            loadByNode.merge(entry.getKey(), (long) keys.size(), Long::sum);
        }

        List<Move> moves = new ArrayList<>();

        planDraining(nodes, stateByNode, loadByNode, queueByOwner, reachable, settings, moves);
        if (moves.size() < settings.maxMovesPerCycle()) {
            planBalance(stateByNode, loadByNode, queueByOwner, reachable, settings, moves);
        }

        return moves.size() > settings.maxMovesPerCycle()
                ? List.copyOf(moves.subList(0, settings.maxMovesPerCycle()))
                : List.copyOf(moves);
    }

    private static void planDraining(Collection<StorageNodeStatus> nodes, Map<String, NodeState> stateByNode,
            Map<String, Long> loadByNode, Map<String, Deque<String>> queueByOwner, Set<String> reachable,
            RebalanceSettings settings, List<Move> moves) {
        List<String> drainingOwners = nodes.stream()
                .filter(node -> node.state() == NodeState.DRAINING)
                .map(StorageNodeStatus::nodeId)
                .sorted()
                .toList();
        for (String owner : drainingOwners) {
            Deque<String> queue = queueByOwner.getOrDefault(owner, new ArrayDeque<>());
            while (!queue.isEmpty()) {
                if (moves.size() >= settings.maxMovesPerCycle()) {
                    return;
                }
                Optional<String> dst = leastLoadedActiveReachable(stateByNode, loadByNode, reachable, owner);
                if (dst.isEmpty()) {
                    // Sem destino disponível agora — não há como continuar drenando este (nem os
                    // próximos) nó neste ciclo: a falta de capacidade ACTIVE+alcançável é global.
                    return;
                }
                String seriesKey = queue.pollFirst();
                moves.add(new Move(seriesKey, owner, dst.get()));
                loadByNode.merge(owner, -1L, Long::sum);
                loadByNode.merge(dst.get(), 1L, Long::sum);
            }
        }
    }

    private static void planBalance(Map<String, NodeState> stateByNode, Map<String, Long> loadByNode,
            Map<String, Deque<String>> queueByOwner, Set<String> reachable, RebalanceSettings settings,
            List<Move> moves) {
        List<String> activeReachable = stateByNode.entrySet().stream()
                .filter(entry -> entry.getValue() == NodeState.ACTIVE && reachable.contains(entry.getKey()))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        if (activeReachable.size() < 2) {
            return;
        }

        long total = 0L;
        for (String nodeId : activeReachable) {
            total += loadByNode.getOrDefault(nodeId, 0L);
        }
        double average = (double) total / activeReachable.size();
        double threshold = Math.max(settings.rebalanceMinDelta(), settings.rebalanceTolerance() * average);

        Comparator<String> byLoadDescThenIdAsc = Comparator
                .comparingLong((String id) -> loadByNode.getOrDefault(id, 0L)).reversed()
                .thenComparing(Comparator.naturalOrder());
        Comparator<String> byLoadAscThenIdAsc = Comparator
                .comparingLong((String id) -> loadByNode.getOrDefault(id, 0L))
                .thenComparing(Comparator.naturalOrder());

        while (moves.size() < settings.maxMovesPerCycle()) {
            String maxNode = activeReachable.stream().min(byLoadDescThenIdAsc).orElseThrow();
            String minNode = activeReachable.stream().min(byLoadAscThenIdAsc).orElseThrow();
            long max = loadByNode.getOrDefault(maxNode, 0L);
            long min = loadByNode.getOrDefault(minNode, 0L);
            if (max - min <= threshold) {
                return;
            }
            Deque<String> queue = queueByOwner.get(maxNode);
            if (queue == null || queue.isEmpty()) {
                // A carga reportada diz que ainda dá para mover, mas não sobrou nenhuma chave conhecida
                // no nó mais carregado (ex.: só séries já em migração) — não há como progredir.
                return;
            }
            String seriesKey = queue.pollFirst();
            moves.add(new Move(seriesKey, maxNode, minNode));
            loadByNode.merge(maxNode, -1L, Long::sum);
            loadByNode.merge(minNode, 1L, Long::sum);
        }
    }

    private static Optional<String> leastLoadedActiveReachable(Map<String, NodeState> stateByNode,
            Map<String, Long> loadByNode, Set<String> reachable, String excludingOwner) {
        return stateByNode.entrySet().stream()
                .filter(entry -> entry.getValue() == NodeState.ACTIVE)
                .map(Map.Entry::getKey)
                .filter(reachable::contains)
                .filter(id -> !id.equals(excludingOwner))
                .min(Comparator.comparingLong((String id) -> loadByNode.getOrDefault(id, 0L))
                        .thenComparing(Comparator.naturalOrder()));
    }
}
