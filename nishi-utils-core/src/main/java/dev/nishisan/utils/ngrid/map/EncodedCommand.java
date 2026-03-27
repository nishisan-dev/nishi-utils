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

package dev.nishisan.utils.ngrid.map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Wrapper para transportar um {@link MapReplicationCommand} codificado como
 * {@code byte[]} através do
 * {@link dev.nishisan.utils.ngrid.common.ClientRequestPayload}.
 *
 * <p>O Jackson serializa {@code byte[]} diretamente como uma string Base64,
 * perdendo a informação de tipo — o líder receberia uma {@code String} ao
 * invés de um {@code byte[]}. Este wrapper é um POJO reconhecível pelo
 * {@code @JsonTypeInfo(CLASS)} aplicado ao campo {@code body} de
 * {@link dev.nishisan.utils.ngrid.common.ClientRequestPayload}: o Jackson
 * escreve {@code {"@class":"...EncodedCommand","payload":"<base64>"}} e
 * deserializa de volta ao tipo correto sem ambiguidade.
 *
 * <p>O {@code executeLocal()} do
 * {@link dev.nishisan.utils.ngrid.structures.DistributedMap} detecta
 * {@code EncodedCommand} via {@code instanceof} e extrai o {@code byte[]}
 * original para decodificá-lo via {@link MapReplicationCodec}.
 *
 * <p>A classe pertence ao mesmo {@code LaunchedClassLoader} do
 * {@code nishi-utils}, eliminando o problema anterior de
 * {@code Class.forName("[B")} que causava falha ao usar {@code byte[]} puro.
 */
public final class EncodedCommand {

    private final byte[] payload;

    @JsonCreator
    public EncodedCommand(@JsonProperty("payload") byte[] payload) {
        this.payload = payload;
    }

    public byte[] payload() {
        return payload;
    }
}
