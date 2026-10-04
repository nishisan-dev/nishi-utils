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

package dev.nishisan.utils.oss.cluster.api;

import java.util.List;
import java.util.Objects;

/**
 * Exceção de domínio do cliente do cluster ngrrd. Toda falha reportada ao
 * chamador carrega um {@link ErrorCode} para permitir tratamento programático
 * (retry, fallback, alerta) sem depender de parsing de mensagem.
 */
public class NgrrdClusterException extends RuntimeException {

    private final ErrorCode code;
    private final List<String> unavailableNodeIds;

    public NgrrdClusterException(ErrorCode code, String message) {
        this(code, message, List.of());
    }

    public NgrrdClusterException(ErrorCode code, String message, Throwable cause) {
        this(code, message, List.of(), cause);
    }

    /**
     * @param unavailableNodeIds storages inalcançáveis ou sem resposta que motivaram a falha (ver
     *                           {@link ErrorCode#PLACEMENT_UNAVAILABLE}); {@code null} equivale a vazia
     */
    public NgrrdClusterException(ErrorCode code, String message, List<String> unavailableNodeIds) {
        // Sem causa: deixa initCause disponível, como no construtor de dois argumentos.
        super(message);
        this.code = Objects.requireNonNull(code, "code");
        this.unavailableNodeIds = copyOf(unavailableNodeIds);
    }

    /**
     * @param unavailableNodeIds storages inalcançáveis ou sem resposta que motivaram a falha (ver
     *                           {@link ErrorCode#PLACEMENT_UNAVAILABLE}); {@code null} equivale a vazia
     */
    public NgrrdClusterException(ErrorCode code, String message, List<String> unavailableNodeIds, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
        this.unavailableNodeIds = copyOf(unavailableNodeIds);
    }

    private static List<String> copyOf(List<String> nodeIds) {
        return nodeIds == null ? List.of() : List.copyOf(nodeIds);
    }

    /** Código de erro que motivou a exceção. */
    public ErrorCode code() {
        return code;
    }

    /**
     * Storages inalcançáveis ou sem resposta que motivaram a falha, em ordem crescente — preenchida em
     * {@link ErrorCode#PLACEMENT_UNAVAILABLE}; lista imutável e vazia nos demais casos.
     */
    public List<String> unavailableNodeIds() {
        return unavailableNodeIds;
    }
}
