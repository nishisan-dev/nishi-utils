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

package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.replication.TopicFrontiers.Comparison;

/** Semântica do vetor de fronteiras por tópico (issue #178). */
class TopicFrontiersTest {

    @Test
    void totalIsTheSumAndSyntheticKeysAreDropped() {
        TopicFrontiers f = TopicFrontiers.of(Map.of(
                "map:ngrrd.catalog", 10L, "map:ngrrd.nodes", 20L, "_global", 999L, "_topic:x", 5L, "queue:q", 0L));
        assertEquals(30L, f.total());
        assertEquals(0L, f.frontier("_global"));
        assertEquals(0L, f.frontier("queue:q"), "fronteira zero não é registrada");
        assertEquals(0L, f.frontier("unknown"));
        assertTrue(TopicFrontiers.EMPTY.isEmpty());
        assertEquals(0L, TopicFrontiers.of(null).total());
    }

    @Test
    void sameTotalDifferentTopicsIsDecidedPerTopic() {
        // O caso do comentário da #178: A perdeu a última op do catálogo mas tem uma op a mais em
        // nodes — o máximo e a soma empatam; por tópico, B domina A.
        TopicFrontiers a = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 9L, "map:ngrrd.nodes", 20L));
        TopicFrontiers b = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 10L, "map:ngrrd.nodes", 20L));
        assertEquals(Comparison.BEHIND, a.compare(b, 0L));
        assertEquals(Comparison.AHEAD, b.compare(a, 0L));
        assertTrue(a.isBehind(b, 0L));
        assertFalse(b.isBehind(a, 0L));
        assertTrue(b.isAhead(a, 0L));
        assertEquals("map:ngrrd.catalog=9<10", a.describeDivergence(b, 0L));
    }

    @Test
    void incomparableVectorsFallBackToTotalDeterministically() {
        TopicFrontiers a = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 9L, "map:ngrrd.nodes", 21L));
        TopicFrontiers b = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 10L, "map:ngrrd.nodes", 19L));
        assertEquals(Comparison.INCOMPARABLE, a.compare(b, 0L));
        assertTrue(a.isAhead(b, 0L), "soma 30 > 29 decide");
        assertTrue(b.isBehind(a, 0L));
        assertFalse(a.isBehind(b, 0L));
        // Somas iguais e incomparáveis: ninguém está atrás — a afinidade decide no coordinator.
        TopicFrontiers c = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 10L, "map:ngrrd.nodes", 20L));
        TopicFrontiers d = TopicFrontiers.of(Map.of("map:ngrrd.catalog", 11L, "map:ngrrd.nodes", 19L));
        assertFalse(c.isBehind(d, 0L));
        assertFalse(d.isBehind(c, 0L));
        assertFalse(c.isAhead(d, 0L));
    }

    @Test
    void thresholdAppliesPerTopicAndMissingTopicCountsAsZero() {
        TopicFrontiers a = TopicFrontiers.of(Map.of("t1", 10L));
        TopicFrontiers b = TopicFrontiers.of(Map.of("t1", 12L, "t2", 1L));
        assertEquals(Comparison.BEHIND, a.compare(b, 0L));
        assertEquals(Comparison.EQUAL, a.compare(b, 2L), "dentro da tolerância em todos os tópicos");
        assertEquals(Comparison.BEHIND, a.compare(b, 1L), "t1: 12 > 10 + 1");
    }
}
