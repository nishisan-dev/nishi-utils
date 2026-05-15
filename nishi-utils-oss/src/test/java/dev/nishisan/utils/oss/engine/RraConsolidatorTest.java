package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.definition.RraDef;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RraConsolidatorTest {

    @Test
    void consolidaAverageRespeitandoXff() {
        RraDef rra = new RraDef("r", 300, 100, List.of(ConsolidationFunction.AVERAGE), 0.5);
        double cdp = RraConsolidator.consolidate(rra, ConsolidationFunction.AVERAGE,
                List.of(10.0, 20.0, 30.0), 1);
        assertEquals(20.0, cdp, 1e-9);
    }

    @Test
    void retornaNanQuandoXffEstourado() {
        RraDef rra = new RraDef("r", 300, 100, List.of(ConsolidationFunction.AVERAGE), 0.5);
        double cdp = RraConsolidator.consolidate(rra, ConsolidationFunction.AVERAGE,
                List.of(10.0), 5); // 5/6 ≈ 0.83 > xff 0.5
        assertTrue(Double.isNaN(cdp));
    }

    @Test
    void consolidaMaxIgnorandoNaN() {
        RraDef rra = new RraDef("r", 300, 100, List.of(ConsolidationFunction.MAX), 0.5);
        double cdp = RraConsolidator.consolidate(rra, ConsolidationFunction.MAX,
                List.of(5.0, Double.NaN, 12.0, 7.0), 0);
        assertEquals(12.0, cdp, 1e-9);
    }

    @Test
    void retornaNanQuandoTodosPdpsSaoNan() {
        RraDef rra = new RraDef("r", 300, 100, List.of(ConsolidationFunction.AVERAGE), 0.5);
        double cdp = RraConsolidator.consolidate(rra, ConsolidationFunction.AVERAGE,
                List.of(Double.NaN, Double.NaN), 0);
        assertTrue(Double.isNaN(cdp));
    }

    @Test
    void consolidaLast() {
        RraDef rra = new RraDef("r", 300, 100, List.of(ConsolidationFunction.LAST), 0.5);
        double cdp = RraConsolidator.consolidate(rra, ConsolidationFunction.LAST,
                List.of(1.0, 2.0, 3.0), 0);
        assertEquals(3.0, cdp, 1e-9);
    }
}
