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

import java.util.Objects;

/**
 * Exceção de domínio do cliente do cluster ngrrd. Toda falha reportada ao
 * chamador carrega um {@link ErrorCode} para permitir tratamento programático
 * (retry, fallback, alerta) sem depender de parsing de mensagem.
 */
public class NgrrdClusterException extends RuntimeException {

    private final ErrorCode code;

    public NgrrdClusterException(ErrorCode code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public NgrrdClusterException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    /** Código de erro que motivou a exceção. */
    public ErrorCode code() {
        return code;
    }
}
