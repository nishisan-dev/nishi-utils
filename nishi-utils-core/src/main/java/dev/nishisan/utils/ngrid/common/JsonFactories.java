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

package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.core.JsonFactory;

/**
 * Jackson factories shared by the cluster's JSON codecs.
 */
public final class JsonFactories {

    private JsonFactories() {
    }

    /**
     * A {@link JsonFactory} for payloads whose object keys are data (series keys, map keys, node ids)
     * rather than a fixed schema: field-name canonicalization (and therefore {@code String.intern}) is
     * disabled.
     *
     * <p>With the Jackson defaults, every distinct field name read is added to the factory's shared
     * symbol table and interned. A {@code Map<String, ?>} keyed by series key turns each response into
     * thousands of new names: the table keeps rehashing, {@code String.intern} grows without bound, and
     * all of it runs on the connection's single reader thread. Measured in production: ~300 ms to decode
     * one {@code WRITE_BATCH} response with 8,000 series, serializing the writes to that node (a
     * micro-benchmark of 8,000 new keys per response: ~15-22 ms with the defaults vs ~2 ms here).</p>
     *
     * <p>Trade-off: without canonicalization, fixed schema names are allocated per parse instead of
     * reused — roughly +90 ns per field name (a small 8-field message parses in ~1.3 µs instead of
     * ~0.6 µs; an 8,000-sample {@code WRITE_BATCH} request in ~3.1 ms instead of ~2.3 ms). Heartbeats
     * and pings use the binary codec and are unaffected. The wire format is unchanged.</p>
     *
     * @return a new factory with {@link JsonFactory.Feature#CANONICALIZE_FIELD_NAMES} and
     *         {@link JsonFactory.Feature#INTERN_FIELD_NAMES} disabled
     */
    public static JsonFactory dynamicKeys() {
        return JsonFactory.builder()
                .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
                .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                .build();
    }
}
