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
}
