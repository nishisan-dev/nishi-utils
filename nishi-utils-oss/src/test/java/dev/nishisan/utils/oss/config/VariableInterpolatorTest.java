package dev.nishisan.utils.oss.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VariableInterpolatorTest {

    @Test
    void substituiVariavelPresente() {
        String out = VariableInterpolator.interpolate(
                "device:${DEVICE}/iface:eth0",
                Map.of("DEVICE", "router-1")::get);
        assertEquals("device:router-1/iface:eth0", out);
    }

    @Test
    void usaDefaultQuandoVariavelAusente() {
        String out = VariableInterpolator.interpolate(
                "port:${HTTP_PORT:8080}",
                Map.<String, String>of()::get);
        assertEquals("port:8080", out);
    }

    @Test
    void usaDefaultQuandoVariavelVazia() {
        String out = VariableInterpolator.interpolate(
                "x:${X:fallback}",
                Map.of("X", "")::get);
        assertEquals("x:fallback", out);
    }

    @Test
    void mantemPlaceholderQuandoVariavelEDefaultAusentes() {
        String out = VariableInterpolator.interpolate(
                "x:${MISSING}",
                Map.<String, String>of()::get);
        assertEquals("x:${MISSING}", out);
    }

    @Test
    void substituiMultiplosPlaceholders() {
        String out = VariableInterpolator.interpolate(
                "${A}-${B:b}-${C}",
                Map.of("A", "a", "C", "c")::get);
        assertEquals("a-b-c", out);
    }

    @Test
    void escapaCaracteresDeReplacement() {
        String out = VariableInterpolator.interpolate(
                "key:${V}",
                Map.of("V", "$1\\back")::get);
        assertEquals("key:$1\\back", out);
    }
}
