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

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre o conjunto ordenado de regras {@link PlacementRules} e o seu fingerprint (issue #167, item 3). */
class PlacementRulesTest {

    private static final PlacementRule PINNED =
            new PlacementRule("tems-core", "ifaceStats", "br-sp/", Set.of("storage-1", "storage-2"), null);
    private static final PlacementRule NO_LAB =
            new PlacementRule("no-lab-on-3", null, "lab/", null, Set.of("storage-3"));

    @Test
    void conjuntoVazioNaoTemFingerprintNemRestringeNada() {
        assertTrue(PlacementRules.NONE.isEmpty());
        assertNull(PlacementRules.NONE.fingerprint());
        assertEquals(Optional.empty(), PlacementRules.NONE.match("lab/x", "ifaceStats"));
        assertEquals(Optional.empty(), PlacementRules.NONE.exclusionReason("lab/x", "ifaceStats", "storage-3"));
        assertEquals(PlacementRules.NONE, PlacementRules.of(List.of()));
    }

    @Test
    void nomesDuplicadosSaoRejeitados() {
        PlacementRule sameName = new PlacementRule("tems-core", null, "x/", null, Set.of("storage-0"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> PlacementRules.of(List.of(PINNED, sameName)));
        assertTrue(error.getMessage().contains("tems-core"), error.getMessage());
    }

    @Test
    void primeiraRegraQueCasaVenceESemCasamentoFicaIrrestrito() {
        PlacementRule broad = new PlacementRule("broad", null, "br-sp/", null, Set.of("storage-2"));
        PlacementRules rules = PlacementRules.of(List.of(PINNED, broad, NO_LAB));

        assertEquals(Optional.of(PINNED), rules.match("br-sp/if-1", "ifaceStats"));
        assertEquals(Optional.of(broad), rules.match("br-sp/if-1", "cpuStats"), "a segunda casa só pelo prefixo");
        assertEquals(Optional.of(NO_LAB), rules.match("lab/x", null), "série legada casa a regra sem definition");
        assertEquals(Optional.empty(), rules.match("br-rj/if-1", "ifaceStats"));

        assertEquals(Optional.of("rule_pinned_elsewhere(tems-core)"),
                rules.exclusionReason("br-sp/if-1", "ifaceStats", "storage-0"));
        assertEquals(Optional.of("rule_excluded(broad)"), rules.exclusionReason("br-sp/if-1", "cpuStats", "storage-2"));
        assertEquals(Optional.empty(), rules.exclusionReason("br-rj/if-1", "ifaceStats", "storage-3"));
    }

    @Test
    void fingerprintEhDeterministicoSensivelAOrdemEIndependenteDaOrdemDosConjuntos() {
        PlacementRule pinnedShuffled =
                new PlacementRule("tems-core", "ifaceStats", "br-sp/", Set.of("storage-2", "storage-1"), null);
        String hash = PlacementRules.of(List.of(PINNED, NO_LAB)).fingerprint();

        assertEquals(16, hash.length());
        assertTrue(hash.matches("[0-9a-f]{16}"), hash);
        assertEquals(hash, PlacementRules.of(List.of(pinnedShuffled, NO_LAB)).fingerprint());
        assertNotEquals(hash, PlacementRules.of(List.of(NO_LAB, PINNED)).fingerprint(), "a ordem faz parte da regra");
        assertNotEquals(hash, PlacementRules.of(List.of(PINNED)).fingerprint());
    }

    @Test
    void fingerprintEhOSha256DoTextoCanonico() {
        // sha256("tems-core|ifaceStats|br-sp/|storage-1,storage-2|\nno-lab-on-3||lab/||storage-3") — primeiros 16 hex.
        String expected = java.util.HexFormat.of().formatHex(sha256(
                "tems-core|ifaceStats|br-sp/|storage-1,storage-2|\nno-lab-on-3||lab/||storage-3")).substring(0, 16);

        assertEquals(expected, PlacementRules.of(List.of(PINNED, NO_LAB)).fingerprint());
    }

    @Test
    void listaExpostaEhImutavelEIgualdadeEhPorConteudo() {
        PlacementRules rules = PlacementRules.of(List.of(PINNED, NO_LAB));

        assertEquals(2, rules.size());
        assertEquals(rules, PlacementRules.of(List.of(PINNED, NO_LAB)));
        assertThrows(UnsupportedOperationException.class, () -> rules.rules().add(NO_LAB));
    }

    private static byte[] sha256(String text) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
