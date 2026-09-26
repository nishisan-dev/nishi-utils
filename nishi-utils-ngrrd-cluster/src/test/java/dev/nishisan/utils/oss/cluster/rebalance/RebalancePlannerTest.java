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
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.placement.PlacementRule;
import dev.nishisan.utils.oss.cluster.placement.PlacementRules;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link RebalancePlanner} puramente (sem rede, catálogo ou processo): drenagem primeiro,
 * tolerância/minDelta, determinismo, corte por {@code maxMovesPerCycle}, chaves em migração
 * ignoradas e nó inalcançável nunca escolhido como destino.
 */
class RebalancePlannerTest {

    private static StorageNodeStatus node(String id, NodeState state, long seriesCount) {
        return new StorageNodeStatus(id, state, seriesCount, 0L, 0L, 1_000L);
    }

    private static List<String> seriesRange(String prefix, int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(prefix + "-" + String.format("%03d", i));
        }
        return keys;
    }

    @Test
    void nosDrenandoTemPrioridadeSobreOBalanceamentoNormal() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.DRAINING, 5),
                node("b", NodeState.ACTIVE, 0),
                node("c", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 5));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50));

        assertEquals(5, moves.size());
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a")));
        assertTrue(moves.stream().allMatch(m -> m.dst().equals("b") || m.dst().equals("c")));
    }

    @Test
    void semDestinoDisponivelNenhumMovimentoDeDrenagemEhPlanejado() {
        List<StorageNodeStatus> nodes = List.of(node("a", NodeState.DRAINING, 3));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 3));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50));

        assertTrue(moves.isEmpty());
    }

    @Test
    void respeitaMinDeltaQuandoDiferencaEhPequena() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 105),
                node("b", NodeState.ACTIVE, 100));
        Map<String, List<String>> seriesByOwner = Map.of(
                "a", seriesRange("a-s", 105),
                "b", seriesRange("b-s", 100));

        // diferença = 5 < minDelta(50) e < tolerância(10% de 102.5 ≈ 10.25) -> nada a mover.
        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50));

        assertTrue(moves.isEmpty());
    }

    @Test
    void moveAteRespeitarATolerancia() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 100),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 100));

        // minDelta=0 -> só a tolerância (10% da média 50 = 5) decide. Total é invariante (100 séries
        // só trocam de dono, nunca somem) -> a média não muda ao longo do ciclo. Move até max-min <= 5:
        // após k movimentos, a fica com 100-k e b com k; 100-2k <= 5 só a partir de k=48.
        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0L, 0.10, 1_000));

        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("b")));
        assertEquals(48, moves.size());
    }

    @Test
    void movePrimeiroASerieDeMenorChave() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 3),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", List.of("z-series", "a-series", "m-series"));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0L, 0.0, 1));

        assertEquals(1, moves.size());
        assertEquals("a-series", moves.get(0).seriesKey());
    }

    @Test
    void ehDeterministicoParaOMesmoSnapshotDeEntrada() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 20),
                node("b", NodeState.ACTIVE, 0),
                node("c", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = new LinkedHashMap<>();
        seriesByOwner.put("a", seriesRange("s", 20));
        RebalanceSettings settings = new RebalanceSettings(0L, 0.0, 100);

        List<Move> first = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(), settings);
        List<Move> second = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(), settings);

        assertEquals(first, second);
    }

    @Test
    void cortaEmMaxMovesPerCycle() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 20),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 20));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0L, 0.0, 3));

        assertEquals(3, moves.size());
    }

    @Test
    void chavesEmMigracaoNuncaSaoReplanejadas() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 2),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", List.of("s-1", "s-2"));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of("s-1", "s-2"),
                new RebalanceSettings(0L, 0.0, 100));

        assertTrue(moves.isEmpty(), "as duas únicas séries de 'a' estão em migração; nada sobra para mover");
    }

    @Test
    void noInalcancavelNuncaEhEscolhidoComoDestino() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 10),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 10));

        // "b" está no catálogo mas NÃO em reachable -> não pode ser destino; sem outro candidato, nada
        // é movido.
        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a"), Set.of(),
                new RebalanceSettings(0L, 0.0, 100));

        assertTrue(moves.isEmpty());
    }

    @Test
    void noDrenandoInalcancavelAindaAssimTemSuasSeriesEnfileiradasParaSaida() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.DRAINING, 3),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 3));

        // "a" (DRAINING) não está em reachable -> mesmo assim suas séries entram na fila de saída.
        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("b"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50));

        assertEquals(3, moves.size());
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("b")));
    }

    // ---- Issue #177: destino com réplica do catálogo atrasada ----

    @Test
    void destinoExcluidoNuncaRecebeMasContinuaNaDistribuicao() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 100),
                node("b", NodeState.ACTIVE, 0),
                node("c", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 100));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(),
                new RebalanceSettings(0L, 0.0, 1_000), Set.of("b"));

        assertFalse(moves.isEmpty());
        assertTrue(moves.stream().noneMatch(m -> m.dst().equals("b")), "b está excluído como destino: " + moves);
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("c")));
    }

    @Test
    void destinoExcluidoContinuaSendoOrigem() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 10),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 10));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0L, 0.0, 100), Set.of("a"));

        assertFalse(moves.isEmpty(), "a (excluído só como destino) ainda cede séries");
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("b")));
    }

    @Test
    void todosOsDestinosExcluidosDevolvePlanoVazioSemExcecao() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 10),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 10));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(0L, 0.0, 100), Set.of("a", "b"));

        assertTrue(moves.isEmpty());
    }

    @Test
    void drenagemRespeitaODestinoExcluido() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.DRAINING, 4),
                node("b", NodeState.ACTIVE, 0),
                node("c", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 4));

        List<Move> onlyC = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50), Set.of("b"));
        List<Move> none = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50), Set.of("b", "c"));

        assertEquals(4, onlyC.size());
        assertTrue(onlyC.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("c")));
        assertTrue(none.isEmpty(), "sem destino elegível a drenagem espera: " + none);
    }

    @Test
    void semExclusoesOPlanoEhOMesmoDeAntes() {
        List<StorageNodeStatus> nodes = List.of(
                node("a", NodeState.ACTIVE, 100),
                node("b", NodeState.ACTIVE, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 100));
        RebalanceSettings settings = new RebalanceSettings(0L, 0.10, 1_000);

        assertEquals(RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(), settings),
                RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(), settings, Set.of()));
    }

    // ---- Issue #167 (item 3): cota dura e regras de placement ----

    private static StorageNodeStatus quotaNode(String id, NodeState state, long seriesCount, DistributionMode mode,
            double weight, long quotaMaxSeries, long quotaMaxBytes) {
        return new StorageNodeStatus(id, state, seriesCount, 0L, 0L, 1_000L, mode, weight, 0,
                StorageCapabilities.ALL, null, quotaMaxSeries, quotaMaxBytes, null);
    }

    private static Map<String, Long> unknownSizes(Map<String, List<String>> seriesByOwner) {
        Map<String, Long> sizes = new LinkedHashMap<>();
        seriesByOwner.values().forEach(keys -> keys.forEach(key -> sizes.put(key, 0L)));
        return sizes;
    }

    private static List<Move> planWithRules(List<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, RebalanceSettings settings, PlacementRules rules,
            Map<String, String> definitionNameBySeries) {
        return RebalancePlanner.plan(nodes, seriesByOwner, reachable, Set.of(), settings,
                unknownSizes(seriesByOwner), Map.of(), Map.of(), Set.of(), rules, definitionNameBySeries).moves();
    }

    @Test
    void naoEscolheDestinoNaCotaDeSeries() {
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 100, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 10, 0),
                quotaNode("c", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 100));

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b", "c"),
                new RebalanceSettings(0L, 0.0, 1_000), PlacementRules.NONE, Map.of());

        assertFalse(moves.isEmpty());
        assertEquals(10, moves.stream().filter(m -> m.dst().equals("b")).count(), "b recebe até a cota e para");
        assertTrue(moves.stream().filter(m -> m.dst().equals("c")).count() > 10, "o resto vai para c: " + moves);
    }

    @Test
    void naoEscolheDestinoQueEstouraACotaDeBytes() {
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 10, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 3_000),
                quotaNode("c", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 10));
        Map<String, Long> sizes = new LinkedHashMap<>();
        seriesByOwner.get("a").forEach(key -> sizes.put(key, 1_000L));

        List<Move> moves = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b", "c"), Set.of(),
                new RebalanceSettings(0L, 0.0, 1_000), sizes, Map.of(), Map.of(), Set.of(), PlacementRules.NONE,
                Map.of()).moves();

        assertEquals(3, moves.stream().filter(m -> m.dst().equals("b")).count(), "3 × 1000 bytes cabem na cota de b");
        assertTrue(moves.stream().anyMatch(m -> m.dst().equals("c")));
    }

    @Test
    void alvoPonderadoLimitadoPelaCotaRedistribuiResto() {
        // Pesos 1:3, 400 séries: sem cota b teria alvo 300. Com cota 100 em b, o alvo de b é 100 e os 300
        // restantes vão para a (único nó sem teto) — a fica com 300, não com 100.
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 400, DistributionMode.WEIGHT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.WEIGHT, 3, 100, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 400));

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b"),
                new RebalanceSettings(0L, 0.0, 1_000), PlacementRules.NONE, Map.of());

        assertEquals(100, moves.size(), "b só recebe até a cota: " + moves.size());
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("b")));

        // Com três nós, o resto é redistribuído por peso entre os não limitados (a:1, c:1 → 150 cada).
        List<StorageNodeStatus> three = List.of(
                quotaNode("a", NodeState.ACTIVE, 400, DistributionMode.WEIGHT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.WEIGHT, 3, 100, 0),
                quotaNode("c", NodeState.ACTIVE, 0, DistributionMode.WEIGHT, 1, 0, 0));
        List<Move> threeMoves = planWithRules(three, seriesByOwner, Set.of("a", "b", "c"),
                new RebalanceSettings(0L, 0.0, 1_000), PlacementRules.NONE, Map.of());
        assertEquals(100, threeMoves.stream().filter(m -> m.dst().equals("b")).count());
        assertEquals(150, threeMoves.stream().filter(m -> m.dst().equals("c")).count());
    }

    @Test
    void drainRespeitaPin() {
        PlacementRules rules = PlacementRules.of(List.of(
                new PlacementRule("core", "ifaceStats", null, Set.of("a", "c"), null)));
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.DRAINING, 4, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("c", NodeState.ACTIVE, 5, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 4));
        Map<String, String> names = Map.of("s-000", "ifaceStats", "s-001", "ifaceStats");

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b", "c"),
                new RebalanceSettings(50L, 0.10, 50), rules, names);

        assertEquals(4, moves.size());
        Map<String, String> destBySeries = new LinkedHashMap<>();
        moves.forEach(m -> destBySeries.put(m.seriesKey(), m.dst()));
        assertEquals("c", destBySeries.get("s-000"), "fixada em {a,c}: com a drenando só resta c");
        assertEquals("c", destBySeries.get("s-001"));
        assertEquals("b", destBySeries.get("s-002"), "sem regra vai para o menos carregado");
        assertEquals("b", destBySeries.get("s-003"));
    }

    @Test
    void drainSemDestinoElegivelPorRegraFicaVazio() {
        PlacementRules rules = PlacementRules.of(List.of(
                new PlacementRule("only-a", null, "s-", Set.of("a"), null)));
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.DRAINING, 3, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 3));

        RebalancePlanner.Plan plan = RebalancePlanner.plan(nodes, seriesByOwner, Set.of("a", "b"), Set.of(),
                new RebalanceSettings(50L, 0.10, 50), unknownSizes(seriesByOwner), Map.of(), Map.of(), Set.of(),
                rules, Map.of());

        assertTrue(plan.moves().isEmpty(), "pin nunca transborda: " + plan.moves());
        assertEquals(3, plan.rulesSkipped());
    }

    @Test
    void fonteAcimaDaCotaSempreCedeEmCount() {
        // a tem 12 séries e cota 10; b tem 11 e não tem cota: a diferença (1) está abaixo de minDelta/tolerância
        // e sem cota nada seria movido — com a cota, a cede até voltar a 10.
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 12, DistributionMode.COUNT, 1, 10, 0),
                quotaNode("b", NodeState.ACTIVE, 11, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("a-s", 12), "b", seriesRange("b-s", 11));

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b"),
                new RebalanceSettings(50L, 0.10, 50), PlacementRules.NONE, Map.of());

        assertEquals(2, moves.size(), moves.toString());
        assertTrue(moves.stream().allMatch(m -> m.src().equals("a") && m.dst().equals("b")));
    }

    @Test
    void corrigeSerieForaDaRegra() {
        // "s-000"/"s-001" estão em a, mas a regra as fixa em {b}: a fase 0 as move mesmo com o cluster equilibrado.
        PlacementRules rules = PlacementRules.of(List.of(
                new PlacementRule("core", "ifaceStats", null, Set.of("b"), null)));
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 3, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 3, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a", seriesRange("s", 3), "b", seriesRange("t", 3));
        Map<String, String> names = Map.of("s-000", "ifaceStats", "s-001", "ifaceStats", "t-000", "ifaceStats");

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b"),
                new RebalanceSettings(50L, 0.10, 50), rules, names);

        assertEquals(List.of(new Move("s-000", "a", "b"), new Move("s-001", "a", "b")), moves);

        // Limitado por maxMovesPerCycle.
        List<Move> capped = planWithRules(nodes, seriesByOwner, Set.of("a", "b"),
                new RebalanceSettings(50L, 0.10, 1), rules, names);
        assertEquals(List.of(new Move("s-000", "a", "b")), capped);
    }

    @Test
    void balanceamentoNormalNaoMoveSerieParaNoExcluidoPelaRegra() {
        PlacementRules rules = PlacementRules.of(List.of(
                new PlacementRule("no-lab-on-b", null, "lab/", null, Set.of("b"))));
        List<StorageNodeStatus> nodes = List.of(
                quotaNode("a", NodeState.ACTIVE, 6, DistributionMode.COUNT, 1, 0, 0),
                quotaNode("b", NodeState.ACTIVE, 0, DistributionMode.COUNT, 1, 0, 0));
        Map<String, List<String>> seriesByOwner = Map.of("a",
                List.of("lab/0", "lab/1", "lab/2", "lab/3", "prod/0", "prod/1"));

        List<Move> moves = planWithRules(nodes, seriesByOwner, Set.of("a", "b"),
                new RebalanceSettings(0L, 0.0, 50), rules, Map.of());

        assertEquals(2, moves.size(), "só as séries prod/* podem ir para b: " + moves);
        assertTrue(moves.stream().allMatch(m -> m.seriesKey().startsWith("prod/") && m.dst().equals("b")));
    }
}
