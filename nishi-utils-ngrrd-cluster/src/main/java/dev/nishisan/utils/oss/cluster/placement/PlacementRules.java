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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Conjunto <em>ordenado</em> de {@link PlacementRule}s de um storage node ({@code ngrrd.placement.rules},
 * issue #167, item 3). A primeira regra que casa a série vence; sem casamento a série é irrestrita.
 *
 * <p>As regras são configuração uniforme: todo storage node carrega a mesma lista no seu YAML (mesmo
 * precedente de {@code distribution.mode}) e o líder aplica a cópia dele. Cada nó publica o
 * {@link #fingerprint()} da sua lista no {@code StorageNodeStatus.placementRulesHash}, o que permite ao
 * líder e ao operador detectarem divergência ({@code NGRRD_PLACEMENT_RULES divergent …} / coluna
 * {@code RULES} com {@code !} na CLI). Divergência nunca descarta regras.</p>
 */
public final class PlacementRules {

    /** Sem regras: nenhuma restrição e {@link #fingerprint()} {@code null}. */
    public static final PlacementRules NONE = new PlacementRules(List.of());

    private final List<PlacementRule> rules;
    private final String fingerprint;

    private PlacementRules(List<PlacementRule> rules) {
        this.rules = List.copyOf(rules);
        Set<String> names = new HashSet<>();
        for (PlacementRule rule : this.rules) {
            if (!names.add(rule.name())) {
                throw new IllegalArgumentException("nome de regra duplicado: '" + rule.name() + "'");
            }
        }
        this.fingerprint = this.rules.isEmpty() ? null : sha256Prefix(canonicalText(this.rules));
    }

    /**
     * Cria o conjunto a partir da lista ordenada.
     *
     * @throws IllegalArgumentException se houver nomes de regra duplicados
     */
    public static PlacementRules of(List<PlacementRule> rules) {
        Objects.requireNonNull(rules, "rules");
        return rules.isEmpty() ? NONE : new PlacementRules(rules);
    }

    /** Lista imutável, na ordem de avaliação. */
    public List<PlacementRule> rules() {
        return rules;
    }

    public boolean isEmpty() {
        return rules.isEmpty();
    }

    public int size() {
        return rules.size();
    }

    /**
     * Primeiros 16 hex do SHA-256 do texto canônico (uma {@link PlacementRule#canonicalLine()} por linha,
     * na ordem); {@code null} sem regras. Sensível à ordem das regras, insensível à ordem dos conjuntos.
     */
    public String fingerprint() {
        return fingerprint;
    }

    /** A primeira regra que casa a série, ou vazio (série irrestrita). */
    public Optional<PlacementRule> match(String seriesKey, String definitionName) {
        for (PlacementRule rule : rules) {
            if (rule.matches(seriesKey, definitionName)) {
                return Optional.of(rule);
            }
        }
        return Optional.empty();
    }

    /**
     * Motivo pelo qual {@code nodeId} não pode receber a série segundo a primeira regra que a casa
     * ({@link PlacementRule#exclusionReason(String)}), ou vazio quando nenhuma regra casa ou o nó é elegível.
     */
    public Optional<String> exclusionReason(String seriesKey, String definitionName, String nodeId) {
        return match(seriesKey, definitionName).flatMap(rule -> rule.exclusionReason(nodeId));
    }

    private static String canonicalText(List<PlacementRule> rules) {
        return rules.stream().map(PlacementRule::canonicalLine).collect(Collectors.joining("\n"));
    }

    private static String sha256Prefix(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível", e);
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PlacementRules that && rules.equals(that.rules);
    }

    @Override
    public int hashCode() {
        return rules.hashCode();
    }

    @Override
    public String toString() {
        return "PlacementRules" + rules;
    }
}
