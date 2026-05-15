package dev.nishisan.utils.oss;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NgrrdTest {

    @Test
    void apiVersionEstavelEntreReleases() {
        assertEquals("ngrrd/v1", Ngrrd.apiVersion());
    }
}
