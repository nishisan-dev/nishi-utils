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

package dev.nishisan.utils.ngrid.structures;

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;

/**
 * Serializable representation of an optional value used in RPC style responses.
 *
 * @param <T> the value type
 */
public final class SerializableOptional<T extends Serializable> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final boolean present;
    private final T value;

    private SerializableOptional(boolean present, T value) {
        this.present = present;
        this.value = value;
    }

    /**
     * Creates a present optional wrapping the given value.
     *
     * @param <T>   the value type
     * @param value the value
     * @return a present optional
     */
    public static <T extends Serializable> SerializableOptional<T> of(T value) {
        return new SerializableOptional<>(true, value);
    }

    /**
     * Creates an empty optional.
     *
     * @param <T> the value type
     * @return an empty optional
     */
    public static <T extends Serializable> SerializableOptional<T> empty() {
        return new SerializableOptional<>(false, null);
    }

    /**
     * Converts this to a standard {@link Optional}.
     *
     * @return the optional value
     */
    public Optional<T> toOptional() {
        return present ? Optional.ofNullable(value) : Optional.empty();
    }

    /**
     * Returns whether a value is present.
     *
     * @return {@code true} if present
     */
    public boolean isPresent() {
        return present;
    }

    /**
     * Returns the value, or {@code null} if empty.
     *
     * @return the value
     */
    public T value() {
        return value;
    }
}
