/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.common.NodeId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Histerese e cooldown da detecção do seguidor à frente do líder (8.10.1), com relógio controlado.
 */
class FollowerAheadDetectorTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LEADER = NodeId.of("leader-a");
    private static final NodeId OTHER_LEADER = NodeId.of("leader-b");
    private static final int K = 3;
    private static final long T = 30_000L;
    private static final long COOLDOWN = 300_000L;
    private static final long CURSOR = 13_578_849L;
    private static final long LEADER_HWM = 7_200_000L;

    private final FollowerAheadDetector detector = new FollowerAheadDetector(() -> K, () -> T, () -> COOLDOWN);

    @Test
    void respostaIsoladaNaoArma() {
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L));
        assertEquals(1, detector.streakLength(TOPIC));
    }

    @Test
    void menosQueKRespostasNaoArmamMesmoComTVencido() {
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T * 10));
    }

    @Test
    void kRespostasDentroDeTNaoArmamEArmamQuandoTVence() {
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 2_000L),
                "K respostas, mas a condição só persistiu 2 s");
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T - 1L));
        assertTrue(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T),
                "K respostas e T vencido armam o bootstrap");
        assertEquals(0, detector.streakLength(TOPIC), "o disparo encerra a série");
    }

    @Test
    void respostaEmDiaRecomecaASerie() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, CURSOR, 2_000L), "cursor == HWM está em dia");
        assertEquals(0, detector.streakLength(TOPIC));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T + 5_000L));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T + 6_000L));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T + 7_000L),
                "a série recomeçou em T+5 s: o tempo mínimo conta de novo");
    }

    @Test
    void hwmDesconhecidoDoLiderDrenandoRecomecaASerie() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, -1L, T), "HWM -1 = líder em leaderSyncing");
        assertEquals(0, detector.streakLength(TOPIC));
        assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T + 1L));
    }

    @Test
    void liderDiferenteZeraOContador() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertFalse(detector.observe(TOPIC, OTHER_LEADER, 5L, CURSOR, LEADER_HWM, T));
        assertEquals(1, detector.streakLength(TOPIC), "outro líder começa uma série nova");
        assertFalse(detector.observe(TOPIC, OTHER_LEADER, 5L, CURSOR, LEADER_HWM, T + 1L));
        assertFalse(detector.observe(TOPIC, OTHER_LEADER, 5L, CURSOR, LEADER_HWM, T + 2L),
                "três respostas do novo líder, mas só 2 ms de persistência");
        assertTrue(detector.observe(TOPIC, OTHER_LEADER, 5L, CURSOR, LEADER_HWM, 2 * T));
    }

    @Test
    void mandatoDiferenteZeraOContador() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertFalse(detector.observe(TOPIC, LEADER, 6L, CURSOR, LEADER_HWM, T));
        assertEquals(1, detector.streakLength(TOPIC), "novo mandato do mesmo líder começa uma série nova");
    }

    @Test
    void cooldownImpedeNovoDisparoAteVencer() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertTrue(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T));

        // A condição persiste (o bootstrap não resolveu): dentro do cooldown, nada dispara.
        long base = T + 1_000L;
        for (int i = 0; i < 10; i++) {
            assertFalse(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, base + i * T),
                    "dentro do cooldown não dispara de novo");
        }
        assertTrue(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T + COOLDOWN),
                "vencido o cooldown, a condição persistente volta a disparar");
    }

    @Test
    void topicosSaoIndependentes() {
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 0L);
        detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, 1_000L);
        assertTrue(detector.observe(TOPIC, LEADER, 5L, CURSOR, LEADER_HWM, T));
        assertFalse(detector.observe("map:other", LEADER, 5L, CURSOR, LEADER_HWM, T),
                "o cooldown e a série de um tópico não afetam outro");
        assertEquals(1, detector.streakLength("map:other"));
    }
}
