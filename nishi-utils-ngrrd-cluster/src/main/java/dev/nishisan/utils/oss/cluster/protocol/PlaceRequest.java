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

package dev.nishisan.utils.oss.cluster.protocol;

/**
 * Pedido ao líder para criar (ou confirmar, idempotentemente) o placement de
 * uma série.
 *
 * @param seriesKey           chave lógica da série
 * @param definitionHashHex   hash hexadecimal da definição YAML da série
 * @param preferredOwnerNodeId dono preferido, ou {@code null}; usado por adoção
 *                             ({@code LocalReconciler}) e por retomada após migração abortada
 * @param geometry exact requested geometry, or null for legacy/adoption requests
 * @param definitionName {@code metadata.name} da definição ngrrd da série (issue #167, item 3), usado pelas
 *                       regras de placement do líder e gravado no catálogo; {@code null} num pedido de
 *                       adoção ({@code LocalReconciler}) ou vindo de um cliente anterior a este campo
 */
public record PlaceRequest(String seriesKey, String definitionHashHex, String preferredOwnerNodeId,
        dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry, String definitionName) {
    public PlaceRequest(String seriesKey, String definitionHashHex, String preferredOwnerNodeId) {
        this(seriesKey, definitionHashHex, preferredOwnerNodeId, null);
    }

    /** Forma da 8.7.0, sem {@code definitionName}. */
    public PlaceRequest(String seriesKey, String definitionHashHex, String preferredOwnerNodeId,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry) {
        this(seriesKey, definitionHashHex, preferredOwnerNodeId, geometry, null);
    }
}
