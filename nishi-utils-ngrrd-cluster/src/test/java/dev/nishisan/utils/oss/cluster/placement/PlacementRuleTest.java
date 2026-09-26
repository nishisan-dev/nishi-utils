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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre a validação e o casamento de uma {@link PlacementRule} (issue #167, item 3). */
class PlacementRuleTest {

    @Test
    void nomeEmBrancoEhRejeitado() {
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule(" ", "ifaceStats", null, Set.of("storage-1"), null));
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule(null, "ifaceStats", null, Set.of("storage-1"), null));
    }

    @Test
    void exigeAoMenosUmCriterioEntreDefinitionEKeyPrefix() {
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", null, null, Set.of("storage-1"), null));
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", " ", "", Set.of("storage-1"), null));
    }

    @Test
    void exigeExatamenteUmEntrePinEExcludeNaoVazio() {
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", "ifaceStats", null, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", "ifaceStats", null, Set.of(), Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", "ifaceStats", null, Set.of("storage-1"), Set.of("storage-2")));
        assertThrows(IllegalArgumentException.class,
                () -> new PlacementRule("r", "ifaceStats", null, Set.of(" "), null));
    }

    @Test
    void criteriosEmBrancoViramNulosEConjuntosSaoCopiasImutaveis() {
        Set<String> pin = new HashSet<>(Set.of("storage-1"));
        PlacementRule rule = new PlacementRule("r", "", "br-sp/", pin, null);
        pin.add("storage-9");

        assertNull(rule.definition());
        assertEquals("br-sp/", rule.keyPrefix());
        assertEquals(Set.of("storage-1"), rule.pin());
        assertEquals(Set.of(), rule.exclude());
        assertThrows(UnsupportedOperationException.class, () -> rule.pin().add("x"));
    }

    @Test
    void casaPorDefinitionEPorKeyPrefixComEQuandoAmbosPresentes() {
        PlacementRule both = new PlacementRule("r", "ifaceStats", "br-sp/", Set.of("storage-1"), null);

        assertTrue(both.matches("br-sp/if-1", "ifaceStats"));
        assertFalse(both.matches("br-rj/if-1", "ifaceStats"), "prefixo diferente");
        assertFalse(both.matches("br-sp/if-1", "cpuStats"), "definição diferente");
        assertFalse(both.matches("br-sp/if-1", null), "série legada sem definição não casa regra com definition");
    }

    @Test
    void serieLegadaSemDefinicaoCasaSomenteRegraSemCriterioDeDefinition() {
        PlacementRule byPrefix = new PlacementRule("r", null, "lab/", null, Set.of("storage-2"));
        PlacementRule byDefinition = new PlacementRule("d", "ifaceStats", null, null, Set.of("storage-2"));

        assertTrue(byPrefix.matches("lab/x", null));
        assertFalse(byDefinition.matches("lab/x", null));
    }

    @Test
    void pinExcluiTodoNoForaDaListaEExcludeSoOsListados() {
        PlacementRule pinned = new PlacementRule("tems-core", "ifaceStats", null, Set.of("storage-1", "storage-2"), null);
        PlacementRule excluded = new PlacementRule("no-lab-on-3", null, "lab/", null, Set.of("storage-3"));

        assertEquals(Optional.empty(), pinned.exclusionReason("storage-1"));
        assertEquals(Optional.of("rule_pinned_elsewhere(tems-core)"), pinned.exclusionReason("storage-0"));
        assertEquals(Optional.of("rule_excluded(no-lab-on-3)"), excluded.exclusionReason("storage-3"));
        assertEquals(Optional.empty(), excluded.exclusionReason("storage-1"));
    }

    @Test
    void linhaCanonicaOrdenaOsConjuntosEUsaVazioParaCriterioAusente() {
        PlacementRule rule = new PlacementRule("r", null, "lab/", null, Set.of("storage-3", "storage-1"));

        assertEquals("r||lab/||storage-1,storage-3", rule.canonicalLine());
        assertEquals("r|ifaceStats||storage-2,storage-9|",
                new PlacementRule("r", "ifaceStats", null, Set.of("storage-9", "storage-2"), null).canonicalLine());
    }
}
