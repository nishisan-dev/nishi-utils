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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre {@link NgrrdClusterConfig#fromYaml(String, Function)} (seção 3 da spec do M4). */
class NgrrdClusterConfigYamlTest {

    private static final Function<String, String> NO_ENV = name -> null;

    @Test
    void yamlCompletoPreencheTodosOsCampos() {
        String yaml = """
                client:
                  id: ngrrd-client-1
                  host: 127.0.0.1
                  port: 0
                  dataDir: /var/ngrrd/client-1/data
                  seed: 127.0.0.1:9000
                  peers:
                    - 127.0.0.1:9101
                    - 127.0.0.1:9102
                  batchMaxSamples: 250
                  batchMaxDelay: 100ms
                  maxBufferedSamplesPerNode: 50000
                  bufferFullPolicy: FAIL
                  requestTimeout: 10s
                  retryTimeout: 2m
                  closeTimeout: 45s
                  leaderWaitTimeout: 20s
                """;

        NgrrdClusterConfig config = NgrrdClusterConfig.fromYaml(yaml, NO_ENV);

        assertEquals("ngrrd-client-1", config.clientId());
        assertEquals("127.0.0.1", config.host());
        assertEquals(0, config.port());
        assertEquals(Path.of("/var/ngrrd/client-1/data"), config.dataDir());
        assertEquals("127.0.0.1:9000", config.seed());
        assertEquals(List.of("127.0.0.1:9101", "127.0.0.1:9102"), config.peers());
        assertEquals(250, config.batchMaxSamples());
        assertEquals(Duration.ofMillis(100), config.batchMaxDelay());
        assertEquals(50_000L, config.maxBufferedSamplesPerNode());
        assertEquals(NgrrdClusterConfig.BufferFullPolicy.FAIL, config.bufferFullPolicy());
        assertEquals(Duration.ofSeconds(10), config.requestTimeout());
        assertEquals(Duration.ofMinutes(2), config.retryTimeout());
        assertEquals(Duration.ofSeconds(45), config.closeTimeout());
        assertEquals(Duration.ofSeconds(20), config.leaderWaitTimeout());
    }

    @Test
    void yamlMinimoUsaOsDefaultsDoBuilder() {
        String yaml = """
                client:
                  id: ngrrd-client-1
                  host: 127.0.0.1
                  seed: 127.0.0.1:9000
                """;

        NgrrdClusterConfig config = NgrrdClusterConfig.fromYaml(yaml, NO_ENV);
        NgrrdClusterConfig defaults = NgrrdClusterConfig.builder()
                .clientId("ngrrd-client-1")
                .host("127.0.0.1")
                .seed("127.0.0.1:9000")
                .build();

        assertEquals(defaults.batchMaxSamples(), config.batchMaxSamples());
        assertEquals(defaults.bufferFullPolicy(), config.bufferFullPolicy());
        assertEquals(defaults.closeTimeout(), config.closeTimeout());
        assertEquals(defaults.leaderWaitTimeout(), config.leaderWaitTimeout());
    }

    @Test
    void variaveisDeAmbienteComDefaultSaoInterpoladas() {
        String yaml = """
                client:
                  id: ${CLIENT_ID:ngrrd-client-fallback}
                  host: 127.0.0.1
                  seed: 127.0.0.1:9000
                  requestTimeout: ${CLIENT_REQUEST_TIMEOUT:20s}
                """;

        NgrrdClusterConfig withoutEnv = NgrrdClusterConfig.fromYaml(yaml, NO_ENV);
        assertEquals("ngrrd-client-fallback", withoutEnv.clientId());
        assertEquals(Duration.ofSeconds(20), withoutEnv.requestTimeout());

        Map<String, String> env = Map.of("CLIENT_ID", "ngrrd-client-9", "CLIENT_REQUEST_TIMEOUT", "5s");
        NgrrdClusterConfig withEnv = NgrrdClusterConfig.fromYaml(yaml, env::get);
        assertEquals("ngrrd-client-9", withEnv.clientId());
        assertEquals(Duration.ofSeconds(5), withEnv.requestTimeout());
    }

    @Test
    void semSeedNemPeersFalhaComMensagemClara() {
        String yaml = """
                client:
                  id: ngrrd-client-1
                  host: 127.0.0.1
                """;

        assertThrows(IllegalArgumentException.class, () -> NgrrdClusterConfig.fromYaml(yaml, NO_ENV));
    }

    @Test
    void secaoClientAusenteFalhaComMensagemClara() {
        String yaml = "outraCoisa: 1";

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> NgrrdClusterConfig.fromYaml(yaml, NO_ENV));
        assertTrue(e.getMessage().contains("client"), "mensagem: " + e.getMessage());
    }
}
