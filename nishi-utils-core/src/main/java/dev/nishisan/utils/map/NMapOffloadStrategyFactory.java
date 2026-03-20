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

package dev.nishisan.utils.map;

import java.io.Serializable;
import java.nio.file.Path;

/**
 * Factory for creating {@link NMapOffloadStrategy} instances.
 * <p>
 * Because strategies often need a base directory and map name (for disk-backed
 * implementations), the factory defers construction to {@link NMap#open} time.
 */
@FunctionalInterface
public interface NMapOffloadStrategyFactory {

    /**
     * Creates a new strategy instance.
     *
     * @param baseDir the base directory for data storage
     * @param name    the map name (used as subdirectory)
     * @param <K>     the key type
     * @param <V>     the value type
     * @return the strategy
     */
    <K extends Serializable, V extends Serializable> NMapOffloadStrategy<K, V> create(Path baseDir, String name);
}
