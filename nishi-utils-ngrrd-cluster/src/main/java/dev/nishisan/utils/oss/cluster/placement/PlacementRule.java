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

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Uma regra de placement ({@code ngrrd.placement.rules[]}, issue #167, item 3): restringe em quais
 * storage nodes uma série que <em>casa</em> os critérios pode ser colocada ou migrada.
 *
 * <p>Critérios (ao menos um; quando ambos estão presentes valem em conjunto — E lógico):</p>
 * <ul>
 *   <li>{@code definition}: igual ao {@code metadata.name} da definição ngrrd da série. Uma série
 *       cujo {@code definitionName} é desconhecido (placement legado, anterior a esta versão) <b>não</b>
 *       casa uma regra com este critério — só regras apenas por {@code keyPrefix};</li>
 *   <li>{@code keyPrefix}: prefixo da chave da série ({@code seriesKey}).</li>
 * </ul>
 *
 * <p>Efeito (exatamente um, não vazio): {@code pin} = a série só pode viver nos nós listados (nunca
 * "transborda" para fora deles — sem nó elegível o placement falha e o drain fica pendente);
 * {@code exclude} = a série pode viver em qualquer nó, exceto os listados.</p>
 *
 * @param name       nome único da regra (aparece nos motivos de exclusão e nos logs)
 * @param definition {@code metadata.name} da definição; {@code null} = sem este critério
 * @param keyPrefix  prefixo da chave da série; {@code null} = sem este critério
 * @param pin        nós permitidos; vazio quando a regra é de exclusão — nunca {@code null}, cópia imutável
 * @param exclude    nós proibidos; vazio quando a regra é de pin — nunca {@code null}, cópia imutável
 */
public record PlacementRule(String name, String definition, String keyPrefix, Set<String> pin, Set<String> exclude) {

    public PlacementRule {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name é obrigatório");
        }
        name = name.strip();
        definition = blankToNull(definition);
        keyPrefix = blankToNull(keyPrefix);
        if (definition == null && keyPrefix == null) {
            throw new IllegalArgumentException("regra '" + name + "': informe ao menos um de definition/keyPrefix");
        }
        pin = copyNodeIds(pin, name, "pin");
        exclude = copyNodeIds(exclude, name, "exclude");
        if (pin.isEmpty() == exclude.isEmpty()) {
            throw new IllegalArgumentException("regra '" + name + "': informe exatamente um de pin/exclude, não vazio");
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static Set<String> copyNodeIds(Set<String> ids, String name, String field) {
        if (ids == null) {
            return Set.of();
        }
        for (String id : ids) {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("regra '" + name + "': " + field + " contém um nodeId vazio");
            }
        }
        return Set.copyOf(ids);
    }

    /**
     * Se a série ({@code seriesKey}, {@code definitionName}) casa esta regra. {@code definitionName}
     * {@code null} (série legada) só casa regras sem o critério {@code definition}.
     */
    public boolean matches(String seriesKey, String definitionName) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        if (definition != null && !definition.equals(definitionName)) {
            return false;
        }
        return keyPrefix == null || seriesKey.startsWith(keyPrefix);
    }

    /**
     * Motivo pelo qual {@code nodeId} não pode receber uma série que casa esta regra —
     * {@code rule_pinned_elsewhere(<name>)} ou {@code rule_excluded(<name>)} — ou vazio se o nó é elegível.
     */
    public Optional<String> exclusionReason(String nodeId) {
        if (!pin.isEmpty()) {
            return pin.contains(nodeId) ? Optional.empty() : Optional.of("rule_pinned_elsewhere(" + name + ")");
        }
        return exclude.contains(nodeId) ? Optional.of("rule_excluded(" + name + ")") : Optional.empty();
    }

    /**
     * Linha canônica {@code name|definition|keyPrefix|pin(ordenado)|exclude(ordenado)} usada no fingerprint
     * de {@link PlacementRules}; critério ausente vira vazio e os conjuntos saem ordenados e separados por vírgula.
     */
    public String canonicalLine() {
        return name + '|' + (definition == null ? "" : definition) + '|' + (keyPrefix == null ? "" : keyPrefix)
                + '|' + String.join(",", new TreeSet<>(pin)) + '|' + String.join(",", new TreeSet<>(exclude));
    }
}
