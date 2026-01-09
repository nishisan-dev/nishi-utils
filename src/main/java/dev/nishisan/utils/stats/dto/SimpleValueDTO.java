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

package dev.nishisan.utils.stats.dto;

import java.util.concurrent.atomic.AtomicLong;

/**
 * DTO that stores a named value backed by an atomic counter.
 */
public class SimpleValueDTO {
    private String name;
    private AtomicLong value;

    public SimpleValueDTO(String name, Long value) {
        this.name = name;
        this.value = new AtomicLong(value);
    }

    public SimpleValueDTO(String name)   {
        this.name = name;
        this.value = new AtomicLong(0L);
    }

    public SimpleValueDTO()   {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return this.value.get();
    }

    public void setValue(Long value) {
        this.value.set(value);
    }
}
