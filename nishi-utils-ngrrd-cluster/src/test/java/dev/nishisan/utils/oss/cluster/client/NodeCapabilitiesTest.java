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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link NodeCapabilities}: réplica local primeiro; leitura forte quando o local não basta; status
 * ausente ou leitura forte falhando são relidos dentro do prazo — com relógio e espera falsos, sem dormir.
 */
class NodeCapabilitiesTest {

    private static final Duration MAX_WAIT = Duration.ofSeconds(5);

    private final AtomicInteger strongReads = new AtomicInteger();
    private final FakeClock clock = new FakeClock();

    @Test
    void statusLocalComACapacidadeSegueSemLeituraForte() {
        NodeCapabilities capabilities = capabilities(id -> Optional.of(status(id, StorageCapabilities.ALL)),
                id -> Optional.empty());

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));
        assertEquals(0, strongReads.get());
    }

    @Test
    void statusLocalAtrasadoSemACapacidadeEForteComElaSegue() {
        NodeCapabilities capabilities = capabilities(id -> Optional.of(status(id, Set.of())),
                id -> Optional.of(status(id, StorageCapabilities.ALL)));

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));
        assertEquals(1, strongReads.get());
        assertEquals(0, clock.sleptMillis(), "decidido na primeira leitura forte");
    }

    @Test
    void statusSemACapacidadeTambemNoLiderFalhaNaHoraComUnsupportedByNode() {
        NodeCapabilities capabilities = capabilities(id -> Optional.of(status(id, Set.of())),
                id -> Optional.of(status(id, Set.of())));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code());
        assertEquals("storage-a não anuncia catalog.lookup", ex.getMessage());
        assertEquals(1, strongReads.get());
        assertEquals(0, clock.sleptMillis(), "o líder confirmou: nada a esperar");
    }

    @Test
    void statusLocalAusenteConfereComUmaLeituraForte() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(),
                id -> Optional.of(status(id, StorageCapabilities.ALL)));

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.OPEN_CREATE_IF_MISSING,
                MAX_WAIT));
        assertEquals(1, strongReads.get());
    }

    @Test
    void statusQueApareceDepoisDeAlgunsInstantesSegue() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(),
                id -> strongReads.get() < 4 ? Optional.empty() : Optional.of(status(id, StorageCapabilities.ALL)));

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));
        assertEquals(4, strongReads.get());
        assertTrue(clock.sleptMillis() > 0 && clock.sleptMillis() < MAX_WAIT.toMillis(), "" + clock.sleptMillis());
    }

    @Test
    void statusQueNuncaApareceFalhaComUnsupportedByNodeNoFimDoPrazo() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(), id -> Optional.empty());

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.OPEN_CREATE_IF_MISSING, MAX_WAIT));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code());
        assertTrue(ex.getMessage().startsWith("status de storage-a indisponível"), ex.getMessage());
        assertTrue(ex.getMessage().contains("open.createIfMissing"), ex.getMessage());
        assertEquals(MAX_WAIT.toMillis(), clock.sleptMillis(), "releu até o fim do prazo, nem antes nem depois");
        assertTrue(strongReads.get() > 2);
    }

    @Test
    void falhaDeTransporteNaLeituraForteEhRetentadaESegue() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(), id -> {
            if (strongReads.get() == 1) {
                throw new IllegalStateException("falha ao chamar o líder", new IOException("conexão caiu"));
            }
            return Optional.of(status(id, StorageCapabilities.ALL));
        });

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));
        assertEquals(2, strongReads.get());
    }

    @Test
    void leituraForteQueSoFalhaViraTimeoutNoFimDoPrazoENuncaUnsupportedByNode() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(), id -> {
            throw new IllegalStateException("No leader available after 5 attempts");
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));

        assertEquals(ErrorCode.TIMEOUT, ex.code(), "sem confirmar o status, não se sabe se falta a capacidade");
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertEquals(MAX_WAIT.toMillis(), clock.sleptMillis());
    }

    @Test
    void falhaDeAplicacaoDoClusterNaLeituraForteSobeNaHora() {
        NodeCapabilities capabilities = capabilities(id -> Optional.empty(), id -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "erro de aplicação");
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
        assertFalse(clock.sleptMillis() > 0);
    }

    private NodeCapabilities capabilities(Function<String, Optional<StorageNodeStatus>> local,
            Function<String, Optional<StorageNodeStatus>> strong) {
        return new NodeCapabilities(local, id -> {
            strongReads.incrementAndGet();
            return strong.apply(id);
        }, clock, clock::sleep);
    }

    private static StorageNodeStatus status(String nodeId, Set<String> capabilities) {
        return CapabilityFixtures.status(nodeId, capabilities);
    }

    /** Relógio que só anda quando {@link NodeCapabilities} "dorme". */
    private static final class FakeClock extends Clock {
        private final AtomicLong now = new AtomicLong(1_000_000L);
        private final AtomicLong slept = new AtomicLong();

        void sleep(Duration duration) {
            now.addAndGet(duration.toMillis());
            slept.addAndGet(duration.toMillis());
        }

        long sleptMillis() {
            return slept.get();
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public long millis() {
            return now.get();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(now.get());
        }
    }
}
