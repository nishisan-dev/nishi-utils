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

package dev.nishisan.utils.oss.cluster.catalog;

import java.util.Objects;

/**
 * Entrada do catálogo {@code ngrrd.catalog}: onde uma série vive hoje e, durante
 * uma migração, para onde está indo.
 *
 * <p>Fora de migração ({@link PlacementState#ACTIVE}), {@code targetNodeId} e
 * {@code migrationId} são sempre {@code null}. Em {@link PlacementState#MIGRATING}
 * ambos são obrigatórios. O construtor compacto garante essa invariante — não há
 * caminho para construir uma instância inconsistente.
 *
 * @param ownerNodeId      nó dono atual da série; obrigatório em qualquer estado
 * @param targetNodeId     nó de destino da migração em curso; {@code null} fora de migração
 * @param state            estado da entrada
 * @param migrationId      identificador da migração em curso; {@code null} fora de migração
 * @param createdAtEpochMs instante em que a série foi colocada pela primeira vez
 * @param updatedAtEpochMs instante da última transição desta entrada
 */
public record SeriesPlacement(
        String ownerNodeId,
        String targetNodeId,
        PlacementState state,
        String migrationId,
        long createdAtEpochMs,
        long updatedAtEpochMs) {

    public SeriesPlacement {
        Objects.requireNonNull(ownerNodeId, "ownerNodeId é obrigatório");
        Objects.requireNonNull(state, "state é obrigatório");
        // Switch expression (não statement) de propósito: é exaustiva sobre PlacementState sem
        // `default` — se um novo estado for adicionado ao enum sem cobrir o caso aqui, a compilação
        // quebra, em vez de deixar a validação passar em branco silenciosamente.
        String violation = switch (state) {
            case MIGRATING -> (targetNodeId == null || migrationId == null)
                    ? "MIGRATING exige targetNodeId e migrationId não nulos"
                    : null;
            case ACTIVE -> (targetNodeId != null || migrationId != null)
                    ? "ACTIVE exige targetNodeId e migrationId nulos"
                    : null;
        };
        if (violation != null) {
            throw new IllegalArgumentException(violation);
        }
    }

    /** Cria o placement inicial de uma série recém-colocada em {@code owner}. */
    public static SeriesPlacement active(String owner, long now) {
        return new SeriesPlacement(owner, null, PlacementState.ACTIVE, null, now, now);
    }

    /**
     * Inicia a migração de {@code current} para {@code target}, preservando o dono e a criação.
     *
     * @throws IllegalStateException se {@code current} não estiver {@code ACTIVE}
     */
    public static SeriesPlacement migrating(SeriesPlacement current, String target, String migrationId, long now) {
        if (current.state() != PlacementState.ACTIVE) {
            throw new IllegalStateException(
                    "migrating() exige um placement ACTIVE; estado atual: " + current.state());
        }
        return new SeriesPlacement(current.ownerNodeId(), target, PlacementState.MIGRATING, migrationId,
                current.createdAtEpochMs(), now);
    }

    /**
     * Conclui a migração: o dono passa a ser o antigo alvo, volta a {@code ACTIVE}.
     *
     * @throws IllegalStateException se {@code migrating} não estiver {@code MIGRATING}
     */
    public static SeriesPlacement completed(SeriesPlacement migrating, long now) {
        if (migrating.state() != PlacementState.MIGRATING) {
            throw new IllegalStateException(
                    "completed() exige um placement MIGRATING; estado atual: " + migrating.state());
        }
        return new SeriesPlacement(migrating.targetNodeId(), null, PlacementState.ACTIVE, null,
                migrating.createdAtEpochMs(), now);
    }

    /**
     * Aborta a migração: a série volta a {@code ACTIVE} no dono original.
     *
     * @throws IllegalStateException se {@code migrating} não estiver {@code MIGRATING}
     */
    public static SeriesPlacement aborted(SeriesPlacement migrating, long now) {
        if (migrating.state() != PlacementState.MIGRATING) {
            throw new IllegalStateException(
                    "aborted() exige um placement MIGRATING; estado atual: " + migrating.state());
        }
        return new SeriesPlacement(migrating.ownerNodeId(), null, PlacementState.ACTIVE, null,
                migrating.createdAtEpochMs(), now);
    }

    /** Indica se {@code nodeId} é o dono atual desta série. */
    public boolean isOwnedBy(String nodeId) {
        return ownerNodeId.equals(nodeId);
    }
}
