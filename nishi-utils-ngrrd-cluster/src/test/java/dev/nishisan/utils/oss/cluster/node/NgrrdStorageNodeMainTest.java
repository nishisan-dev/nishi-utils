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

package dev.nishisan.utils.oss.cluster.node;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Cobre {@link NgrrdStorageNodeMain#parseConfigPath(String[])} — BAIXO do Refuter (M4, rodada 2). */
class NgrrdStorageNodeMainTest {

    @Test
    void parseConfigPathComFlagValidaDevolveOCaminho() {
        Path path = NgrrdStorageNodeMain.parseConfigPath(new String[] {"--config", "/etc/ngrrd/storage-0.yaml"});

        assertEquals(Path.of("/etc/ngrrd/storage-0.yaml"), path);
    }

    @Test
    void parseConfigPathIgnoraArgumentosAntesDaFlag() {
        Path path = NgrrdStorageNodeMain.parseConfigPath(
                new String[] {"--outra-flag", "x", "--config", "storage.yaml"});

        assertEquals(Path.of("storage.yaml"), path);
    }

    @Test
    void parseConfigPathSemArgumentosLancaIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> NgrrdStorageNodeMain.parseConfigPath(new String[0]));
    }

    @Test
    void parseConfigPathComArgsNuloLancaIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> NgrrdStorageNodeMain.parseConfigPath(null));
    }

    @Test
    void parseConfigPathComFlagSemValorLancaIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> NgrrdStorageNodeMain.parseConfigPath(new String[] {"--config"}));
    }
}
