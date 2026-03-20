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

import dev.nishisan.utils.map.NMapPersistenceMode;

import java.util.Objects;

/**
 * Configuration for a single distributed map.
 * <p>
 * Encapsulates all settings needed to create and manage a distributed map,
 * including the map name and persistence mode.
 *
 * @since 2.2.0
 */
public final class MapConfig {
    private final String name;
    private final NMapPersistenceMode persistenceMode;

    private MapConfig(Builder builder) {
        this.name = builder.name;
        this.persistenceMode = builder.persistenceMode;
    }

    /**
     * Returns the map name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the persistence mode for this map.
     *
     * @return the persistence mode
     */
    public NMapPersistenceMode persistenceMode() {
        return persistenceMode;
    }

    /**
     * Creates a builder for a map config with the given name.
     *
     * @param name the map name
     * @return the builder
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public String toString() {
        return "MapConfig{name='" + name + "', persistenceMode=" + persistenceMode + "}";
    }

    public static final class Builder {
        private final String name;
        private NMapPersistenceMode persistenceMode = NMapPersistenceMode.DISABLED;

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "map name cannot be null");
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("map name cannot be empty");
            }
        }

        /**
         * Sets the persistence mode for this map.
         *
         * @param mode the persistence mode
         * @return this builder
         */
        public Builder persistenceMode(NMapPersistenceMode mode) {
            this.persistenceMode = Objects.requireNonNull(mode, "persistenceMode cannot be null");
            return this;
        }

        /**
         * Builds the map config.
         *
         * @return the immutable map config
         */
        public MapConfig build() {
            return new MapConfig(this);
        }
    }
}
