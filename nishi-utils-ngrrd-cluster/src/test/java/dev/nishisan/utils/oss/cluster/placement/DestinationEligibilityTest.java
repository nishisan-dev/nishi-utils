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
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre {@link DestinationEligibility}: cota, regras e o aviso de divergência (issue #167, item 3). */
class DestinationEligibilityTest {

    private static final PlacementRules RULES = PlacementRules.of(List.of(
            new PlacementRule("tems-core", "ifaceStats", null, Set.of("storage-1", "storage-2"), null),
            new PlacementRule("no-lab-on-3", null, "lab/", null, Set.of("storage-3"))));

    private final List<LogRecord> records = new CopyOnWriteArrayList<>();
    private final Handler capture = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getMessage() != null && record.getMessage().startsWith("NGRRD_PLACEMENT_RULES")) {
                records.add(record);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    void captureLogs() {
        Logger.getLogger(DestinationEligibility.class.getName()).addHandler(capture);
        DestinationEligibility.resetDivergenceWarningForTests();
    }

    @AfterEach
    void releaseLogs() {
        Logger.getLogger(DestinationEligibility.class.getName()).removeHandler(capture);
        DestinationEligibility.resetDivergenceWarningForTests();
    }

    @Test
    void semCotaNuncaExclui() {
        StorageNodeStatus node = node("storage-1", 1_000_000, 900_000, 0, 0, null);

        assertEquals(Optional.empty(), DestinationEligibility.quotaReason(node, 5_000, 5_000, 4_096));
    }

    @Test
    void cotaDeSeriesContaAsPendentesEAPropriaSerie() {
        StorageNodeStatus node = node("storage-1", 10, 0, 12, 0, null);

        assertEquals(Optional.empty(), DestinationEligibility.quotaReason(node, 1, 0, 4_096), "10+1+1 = 12 cabe");
        assertEquals(Optional.of("quota_series(13/12)"), DestinationEligibility.quotaReason(node, 2, 0, 4_096));
    }

    @Test
    void cotaDeBytesUsaOMaiorEntreReservaEPendenteEAoMenosUmByte() {
        StorageNodeStatus node = new StorageNodeStatus("storage-1", NodeState.ACTIVE, 1, 900, 0, 1_000L,
                DistributionMode.COUNT, 1, 50, StorageCapabilities.ALL, null, 0, 1_000, null);

        assertEquals(Optional.empty(), DestinationEligibility.quotaReason(node, 0, 0, 50), "900+50+50 = 1000 cabe");
        assertEquals(Optional.of("quota_bytes(1001/1000)"), DestinationEligibility.quotaReason(node, 0, 0, 51));
        assertEquals(Optional.of("quota_bytes(1010/1000)"), DestinationEligibility.quotaReason(node, 0, 100, 10),
                "pendente maior que a reserva local prevalece");
        assertEquals(Optional.of("quota_bytes(1001/1000)"), DestinationEligibility.quotaReason(node, 0, 100, 0),
                "requestedBytes 0 conta como 1 byte");
    }

    @Test
    void motivoDeRegraVemDaPrimeiraRegraQueCasa() {
        assertEquals(Optional.of("rule_pinned_elsewhere(tems-core)"),
                DestinationEligibility.ruleReason(RULES, "br-sp/if-1", "ifaceStats", "storage-0"));
        assertEquals(Optional.of("rule_excluded(no-lab-on-3)"),
                DestinationEligibility.ruleReason(RULES, "lab/x", null, "storage-3"));
        assertEquals(Optional.empty(), DestinationEligibility.ruleReason(RULES, "lab/x", null, "storage-1"));
        assertEquals(Optional.empty(), DestinationEligibility.ruleReason(PlacementRules.NONE, "lab/x", null, "storage-3"));
    }

    @Test
    void motivoCombinadoAvaliaCotaAntesDaRegra() {
        StorageNodeStatus full = node("storage-0", 5, 0, 5, 0, null);
        StorageNodeStatus free = node("storage-0", 0, 0, 5, 0, null);

        assertEquals(Optional.of("quota_series(6/5)"),
                DestinationEligibility.reason(full, 0, 0, 1, RULES, "br-sp/if-1", "ifaceStats"));
        assertEquals(Optional.of("rule_pinned_elsewhere(tems-core)"),
                DestinationEligibility.reason(free, 0, 0, 1, RULES, "br-sp/if-1", "ifaceStats"));
        assertEquals(Optional.empty(), DestinationEligibility.reason(free, 0, 0, 1, RULES, "br-rj/x", "cpu"));
    }

    @Test
    void divergenciaDeRegrasAvisaUmaVezPorMudancaEIgnoraNosNaoAtivos() {
        String leaderHash = RULES.fingerprint();
        List<StorageNodeStatus> nodes = List.of(
                node("storage-0", 0, 0, 0, 0, leaderHash),
                node("storage-1", 0, 0, 0, 0, "deadbeefdeadbeef"),
                node("storage-2", 0, 0, 0, 0, null),
                node("storage-3", 0, 0, 0, 0, "deadbeefdeadbeef").withState(NodeState.DRAINED, 1L));

        DestinationEligibility.warnIfRulesDiverge(RULES, nodes);
        DestinationEligibility.warnIfRulesDiverge(RULES, nodes);

        assertEquals(1, records.size(), "mesma divergência só avisa uma vez");
        assertEquals(Level.WARNING, records.get(0).getLevel());
        assertEquals("NGRRD_PLACEMENT_RULES divergent leader=" + leaderHash + " nodes=storage-1(deadbeefdeadbeef),storage-2(-)",
                records.get(0).getMessage());

        // Mudou o conjunto divergente: avisa de novo.
        DestinationEligibility.warnIfRulesDiverge(RULES, List.of(nodes.get(0), nodes.get(1)));
        assertEquals(2, records.size());
        assertTrue(records.get(1).getMessage().endsWith("nodes=storage-1(deadbeefdeadbeef)"), records.get(1).getMessage());

        // Convergiu: silêncio; e uma divergência posterior volta a avisar.
        DestinationEligibility.warnIfRulesDiverge(RULES, List.of(nodes.get(0)));
        assertEquals(2, records.size());
        DestinationEligibility.warnIfRulesDiverge(RULES, List.of(nodes.get(0), nodes.get(1)));
        assertEquals(3, records.size());
    }

    @Test
    void liderSemRegrasENoComRegrasTambemEhDivergencia() {
        DestinationEligibility.warnIfRulesDiverge(PlacementRules.NONE,
                List.of(node("storage-0", 0, 0, 0, 0, null), node("storage-1", 0, 0, 0, 0, "deadbeefdeadbeef")));

        assertEquals(1, records.size());
        assertEquals("NGRRD_PLACEMENT_RULES divergent leader=- nodes=storage-1(deadbeefdeadbeef)",
                records.get(0).getMessage());
    }

    private static StorageNodeStatus node(String id, long series, long used, long quotaSeries, long quotaBytes,
            String rulesHash) {
        return new StorageNodeStatus(id, NodeState.ACTIVE, series, used, 0, 1_000L, DistributionMode.COUNT, 1, 0,
                StorageCapabilities.ALL, null, quotaSeries, quotaBytes, rulesHash);
    }
}
