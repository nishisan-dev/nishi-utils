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

package dev.nishisan.utils.map.examples;

import dev.nishisan.utils.map.NMap;
import dev.nishisan.utils.map.NMapConfig;
import dev.nishisan.utils.map.NMapPersistenceMode;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Demonstrates standalone usage of {@link NMap} — no NGrid required.
 */
public final class NMapExample {

    private NMapExample() {
    }

    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/nmap-demo");
        NMapConfig config = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(100)
                .snapshotIntervalTime(Duration.ofMinutes(1))
                .batchSize(50)
                .batchTimeout(Duration.ofMillis(10))
                .build();

        System.out.println("=== NMap Standalone Example ===");
        System.out.println();

        // Phase 1 — write some data
        try (NMap<String, String> map = NMap.open(baseDir, "demo", config)) {
            map.put("name", "Lucas");
            map.put("project", "nishi-utils");
            map.put("component", "NMap");
            System.out.println("Written 3 entries.");
            System.out.println("  name    = " + map.get("name").orElse("?"));
            System.out.println("  project = " + map.get("project").orElse("?"));
            System.out.println("  size    = " + map.size());
        }

        System.out.println();
        System.out.println("Map closed. Reopening...");
        System.out.println();

        // Phase 2 — reopen and verify
        try (NMap<String, String> map = NMap.open(baseDir, "demo", config)) {
            System.out.println("Recovered " + map.size() + " entries from disk:");
            map.forEach((k, v) -> System.out.println("  " + k + " = " + v));

            // Mutate and close
            map.remove("component");
            map.put("version", "1.0");
            System.out.println();
            System.out.println("After mutation: " + map.size() + " entries.");
        }

        System.out.println();
        System.out.println("Done.");
    }
}
