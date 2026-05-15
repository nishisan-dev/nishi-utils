package dev.nishisan.utils.oss.engine;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormulaEvaluatorTest {

    @Test
    void avaliaConversaoOctetsParaBps() {
        double result = FormulaEvaluator.evaluate(
                "delta * 8 / deltaT",
                Map.of("delta", 1500.0, "deltaT", 300.0));
        assertEquals(40.0, result, 1e-9);
    }

    @Test
    void avaliaErrosPorSegundo() {
        double result = FormulaEvaluator.evaluate(
                "delta / deltaT",
                Map.of("delta", 30.0, "deltaT", 60.0));
        assertEquals(0.5, result, 1e-9);
    }

    @Test
    void respeitaPrecedenciaEParenteses() {
        double result = FormulaEvaluator.evaluate(
                "(delta + 2) * 3",
                Map.of("delta", 4.0, "deltaT", 1.0));
        assertEquals(18.0, result, 1e-9);
    }

    @Test
    void aceitaUnaryMinus() {
        double result = FormulaEvaluator.evaluate("-delta + 5", Map.of("delta", 2.0, "deltaT", 1.0));
        assertEquals(3.0, result, 1e-9);
    }

    @Test
    void rejeitaIdentifierSemBinding() {
        assertThrows(IllegalArgumentException.class,
                () -> FormulaEvaluator.evaluate("foo + 1", Map.of()));
    }

    @Test
    void rejeitaExpressaoVazia() {
        assertThrows(IllegalArgumentException.class,
                () -> FormulaEvaluator.evaluate("", Map.of()));
    }
}
