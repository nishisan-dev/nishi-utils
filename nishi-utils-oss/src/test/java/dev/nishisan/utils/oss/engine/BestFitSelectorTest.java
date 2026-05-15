package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.RraDef;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BestFitSelectorTest {

    private static final RraDef RRA_5M_30D = new RraDef("rra_5m_30d", 300, 8640,
            List.of(ConsolidationFunction.AVERAGE, ConsolidationFunction.MAX), 0.5);
    private static final RraDef RRA_1H_6MO = new RraDef("rra_1h_6mo", 3600, 4320,
            List.of(ConsolidationFunction.AVERAGE, ConsolidationFunction.MAX), 0.5);
    private static final RraDef RRA_2H_1Y = new RraDef("rra_2h_1y", 7200, 4380,
            List.of(ConsolidationFunction.AVERAGE, ConsolidationFunction.MAX), 0.5);

    private static final List<RraDef> ALL = List.of(RRA_5M_30D, RRA_1H_6MO, RRA_2H_1Y);

    @Test
    void dailyEscolheRra5m() {
        ViewQuery q = new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 400);
        Optional<RraDef> pick = BestFitSelector.pick(q, ALL);
        assertTrue(pick.isPresent());
        assertEquals("rra_5m_30d", pick.get().name());
    }

    @Test
    void weeklyEscolheRra1h() {
        ViewQuery q = new ViewQuery(Duration.ofDays(7), 3600, ConsolidationFunction.AVERAGE, 500);
        Optional<RraDef> pick = BestFitSelector.pick(q, ALL);
        assertTrue(pick.isPresent());
        assertEquals("rra_1h_6mo", pick.get().name());
    }

    @Test
    void yearlyEscolheRra2h() {
        ViewQuery q = new ViewQuery(Duration.ofDays(365), 7200, ConsolidationFunction.AVERAGE, 5000);
        Optional<RraDef> pick = BestFitSelector.pick(q, ALL);
        assertTrue(pick.isPresent());
        assertEquals("rra_2h_1y", pick.get().name());
    }

    @Test
    void monthlyEscolheRra1h() {
        ViewQuery q = new ViewQuery(Duration.ofDays(30), 3600, ConsolidationFunction.AVERAGE, 1200);
        Optional<RraDef> pick = BestFitSelector.pick(q, ALL);
        assertTrue(pick.isPresent());
        assertEquals("rra_1h_6mo", pick.get().name());
    }

    @Test
    void semCfCompativelRetornaEmpty() {
        ViewQuery q = new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.LAST, 400);
        Optional<RraDef> pick = BestFitSelector.pick(q, ALL);
        assertTrue(pick.isEmpty());
    }
}
