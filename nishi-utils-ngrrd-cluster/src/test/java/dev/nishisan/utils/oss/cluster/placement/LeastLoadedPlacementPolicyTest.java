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

package dev.nishisan.utils.oss.cluster.placement;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeastLoadedPlacementPolicyTest {

    private static final Duration INTERVAL = Duration.ofSeconds(10);

    private final LeastLoadedPlacementPolicy policy = new LeastLoadedPlacementPolicy();

    private static StorageNodeStatus node(String nodeId, NodeState state, long seriesCount,
            long usedBytes, long capacityBytes, long reportedAtEpochMs) {
        return new StorageNodeStatus(nodeId, state, seriesCount, usedBytes, capacityBytes, reportedAtEpochMs);
    }

    private static PlacementContext context(List<StorageNodeStatus> nodes, Set<String> reachable,
            Map<String, Long> pending, long now, String preferredOwnerNodeId) {
        return new PlacementContext(nodes, reachable, pending, now, INTERVAL, preferredOwnerNodeId);
    }

    @Test
    void cargaEfetivaEhOCriterioPrimarioMesmoComFillRatioPior() {
        long now = 1_000L;
        // a tem fillRatio pior (0.9) mas carga efetiva bem menor (5 < 10): a deve vencer,
        // porque a carga efetiva é o critério primário, não o fillRatio.
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 5, 900, 1000, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 10, 100, 1000, now);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-a"), policy.choose(ctx));
    }

    @Test
    void usaFillRatioComoDesempateQuandoCargaEfetivaEmpata() {
        long now = 1_000L;
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 5, 500, 1000, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 5, 200, 1000, now);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void capacidadeDesconhecidaContaComoFillRatioZeroNoDesempate() {
        long now = 1_000L;
        // Mesma carga efetiva (5); a tem capacidade desconhecida (fillRatio sortable = 0.0),
        // b tem capacidade conhecida com fillRatio 0.1 (> 0.0): a vence o desempate.
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 5, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 5, 100, 1000, now);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-a"), policy.choose(ctx));
    }

    @Test
    void guardaDeCapacidadeExcluiNoComFillRatioMaiorOuIgualA95Porcento() {
        long now = 1_000L;
        // a teria a menor carga efetiva (0) mas está a 95% de fillRatio -> excluído pela guarda.
        StorageNodeStatus quaseCheio = node("node-a", NodeState.ACTIVE, 0, 950, 1000, now);
        // b: carga efetiva maior (10), permanece candidato.
        StorageNodeStatus comCarga = node("node-b", NodeState.ACTIVE, 10, 500, 1000, now);
        // c: mesma carga efetiva de a (0), fillRatio 0.9 (abaixo da guarda) -> continua candidato e vence.
        StorageNodeStatus comFolga = node("node-c", NodeState.ACTIVE, 0, 900, 1000, now);
        PlacementContext ctx = context(List.of(quaseCheio, comCarga, comFolga),
                Set.of("node-a", "node-b", "node-c"), Map.of(), now, null);

        assertEquals(Optional.of("node-c"), policy.choose(ctx));
    }

    @Test
    void guardaDeCapacidadeSemCandidatosRestantesRetornaVazio() {
        long now = 1_000L;
        StorageNodeStatus quaseCheio = node("node-a", NodeState.ACTIVE, 0, 950, 1000, now);
        PlacementContext ctx = context(List.of(quaseCheio), Set.of("node-a"), Map.of(), now, null);

        assertEquals(Optional.empty(), policy.choose(ctx));
    }

    @Test
    void ordemDeDesempateEhDeterministicaEmQualquerPermutacaoDaEntrada() {
        long now = 1_000L;
        // Conjunto que quebrava a versão anterior do comparador (não transitiva): a comparação
        // par-a-par de fillRatio "empatava" sempre que um dos dois lados tinha capacidade
        // desconhecida, formando um ciclo x<y, y<z, z<x cujo vencedor dependia da ordem de
        // iteração. Com a chave pré-calculada (carga efetiva -> fillRatio -> nodeId), y vence
        // sempre: tem a menor carga efetiva (0), critério que decide sozinho aqui.
        StorageNodeStatus x = node("node-x", NodeState.ACTIVE, 10, 0, 0, now);
        StorageNodeStatus y = node("node-y", NodeState.ACTIVE, 0, 500, 1000, now);
        StorageNodeStatus z = node("node-z", NodeState.ACTIVE, 20, 100, 1000, now);
        Set<String> reachable = Set.of("node-x", "node-y", "node-z");

        List<List<StorageNodeStatus>> permutations = List.of(
                List.of(x, y, z), List.of(x, z, y), List.of(y, x, z),
                List.of(y, z, x), List.of(z, x, y), List.of(z, y, x));

        List<Optional<String>> winners = permutations.stream()
                .map(nodes -> policy.choose(context(nodes, reachable, Map.of(), now, null)))
                .collect(Collectors.toList());

        assertTrue(winners.stream().allMatch(winner -> winner.equals(Optional.of("node-y"))),
                "toda permutação deveria eleger node-y; resultados: " + winners);
    }

    @Test
    void cargaEfetivaSomaPendenteAoSeriesCount() {
        long now = 1_000L;
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 5, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 6, 0, 0, now);
        // a: 5 + 3 pendentes = 8; b: 6 + 0 = 6 -> b vence mesmo com seriesCount reportado maior.
        Map<String, Long> pending = Map.of("node-a", 3L);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), pending, now, null);

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void empateTotalDesempataPeloMenorNodeId() {
        long now = 1_000L;
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 5, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 5, 0, 0, now);
        PlacementContext ctx = context(List.of(b, a), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-a"), policy.choose(ctx));
    }

    @Test
    void preferidoPresenteVenceMesmoNaoSendoOMenosCarregado() {
        long now = 1_000L;
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 0, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 50, 0, 0, now);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), Map.of(), now, "node-b");

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void preferidoAusenteDosCandidatosCaiParaCriteriosNormais() {
        long now = 1_000L;
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 0, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 50, 0, 0, now);
        PlacementContext ctx = context(List.of(a, b), Set.of("node-a", "node-b"), Map.of(), now, "node-inexistente");

        assertEquals(Optional.of("node-a"), policy.choose(ctx));
    }

    @Test
    void filtraNosEmDraining() {
        long now = 1_000L;
        StorageNodeStatus draining = node("node-a", NodeState.DRAINING, 0, 0, 0, now);
        StorageNodeStatus active = node("node-b", NodeState.ACTIVE, 50, 0, 0, now);
        PlacementContext ctx = context(List.of(draining, active), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void filtraNosInalcancaveis() {
        long now = 1_000L;
        StorageNodeStatus unreachable = node("node-a", NodeState.ACTIVE, 0, 0, 0, now);
        StorageNodeStatus reachable = node("node-b", NodeState.ACTIVE, 50, 0, 0, now);
        // node-a não está em reachableNodeIds, mesmo estando ACTIVE.
        PlacementContext ctx = context(List.of(unreachable, reachable), Set.of("node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void filtraStatusVelho() {
        long now = 1_000_000L;
        // reportado há mais de 2x o intervalo -> não é mais "fresh".
        StorageNodeStatus stale = node("node-a", NodeState.ACTIVE, 0, 0, 0, now - 3 * INTERVAL.toMillis());
        StorageNodeStatus fresh = node("node-b", NodeState.ACTIVE, 50, 0, 0, now);
        PlacementContext ctx = context(List.of(stale, fresh), Set.of("node-a", "node-b"), Map.of(), now, null);

        assertEquals(Optional.of("node-b"), policy.choose(ctx));
    }

    @Test
    void semCandidatosRetornaVazio() {
        long now = 1_000L;
        StorageNodeStatus draining = node("node-a", NodeState.DRAINING, 0, 0, 0, now);
        PlacementContext ctx = context(List.of(draining), Set.of("node-a"), Map.of(), now, null);

        assertEquals(Optional.empty(), policy.choose(ctx));
    }

    @Test
    void rajadaDeDezPlacementsComStatusParadoAlternaEntreDoisNos() {
        long now = 1_000L;
        // Os dois nós reportam o mesmo status (seriesCount=0) durante toda a rajada — simula o
        // relatório de carga "parado" a cada ~10s. Sem o ajuste por `pending`, os 10 placements
        // cairiam todos no mesmo nó (o primeiro escolhido nunca deixaria de "parecer" o mais leve).
        StorageNodeStatus a = node("node-a", NodeState.ACTIVE, 0, 0, 0, now);
        StorageNodeStatus b = node("node-b", NodeState.ACTIVE, 0, 0, 0, now);
        List<StorageNodeStatus> nodes = List.of(a, b);
        Set<String> reachable = Set.of("node-a", "node-b");

        Map<String, Long> pending = new HashMap<>();
        List<String> chosen = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            PlacementContext ctx = context(nodes, reachable, pending, now, null);
            String pick = policy.choose(ctx).orElseThrow();
            chosen.add(pick);
            pending.merge(pick, 1L, Long::sum);
        }

        assertEquals(List.of("node-a", "node-b", "node-a", "node-b", "node-a",
                "node-b", "node-a", "node-b", "node-a", "node-b"), chosen);
        assertEquals(5, chosen.stream().filter("node-a"::equals).count());
        assertEquals(5, chosen.stream().filter("node-b"::equals).count());
    }
}
