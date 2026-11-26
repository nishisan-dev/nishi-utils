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

package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Índice de exclusões pendentes do {@link CatalogService}: mantido em {@code putPlacement}/{@code removePlacement},
 * podado por {@link CatalogService#pendingDeletionsLocal()} e reconstruído por
 * {@link CatalogService#rebuildPendingDeletions()} para as escritas que chegam à réplica sem passar pela
 * instância (replicação, snapshot, carga do disco). Catálogo real de um nó ({@link NGrid#local(int)}).
 */
class CatalogServicePendingDeletionsTest {

    private static final long NOW = 1_700_000_000_000L;

    private NGridCluster cluster;
    private CatalogService catalog;
    /** O mesmo mapa replicado, escrito sem passar pelo {@link CatalogService}. */
    private DistributedMap<String, SeriesPlacement> rawMap;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        catalog = CatalogService.from(cluster.node(0));
        rawMap = cluster.node(0).getMap(CatalogService.CATALOG_MAP, String.class, SeriesPlacement.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.close();
    }

    private static SeriesPlacement active() {
        return SeriesPlacement.active("storage-a", NOW);
    }

    private static SeriesPlacement pending(String operationId) {
        return active().withDeletion(new SeriesDeletion(operationId, 0, List.of("storage-a"), false), NOW);
    }

    /** Verdade: a varredura completa do catálogo, como o {@code recover()} fazia até a 8.14. */
    private Map<String, SeriesPlacement> scanPending() {
        Map<String, SeriesPlacement> pending = new HashMap<>();
        catalog.placementsLocal().forEach((key, placement) -> {
            if (placement.deletion() != null) pending.put(key, placement);
        });
        return pending;
    }

    @Test
    void putERemoveMantemOIndice() {
        catalog.putPlacement("a", active());
        catalog.putPlacement("b", pending("op-b"));
        assertEquals(Map.of("b", pending("op-b")).keySet(), catalog.pendingDeletionsLocal().keySet());

        SeriesPlacement committed = catalog.placementLocal("b").orElseThrow();
        committed = committed.withDeletion(committed.deletion().commit(), NOW);
        catalog.putPlacement("b", committed);
        assertEquals(committed, catalog.pendingDeletionsLocal().get("b"), "o índice devolve o placement atual");

        catalog.putPlacement("b", committed.withDeletion(null, NOW));
        assertTrue(catalog.pendingDeletionsLocal().isEmpty(), "reserva cancelada sai do índice");

        catalog.putPlacement("c", pending("op-c"));
        catalog.removePlacement("c");
        assertTrue(catalog.pendingDeletionsLocal().isEmpty(), "placement removido sai do índice");
    }

    @Test
    void escritaForaDaInstanciaSoApareceDepoisDaReconstrucao() {
        // Reserva gravada por outro líder e recebida por replicação/snapshot: o índice local não a viu.
        catalog.putPlacement("a", active());
        rawMap.put("replicada", pending("op-r"));
        assertTrue(catalog.pendingDeletionsLocal().isEmpty());

        assertEquals(1, catalog.rebuildPendingDeletions());

        assertEquals(scanPending(), catalog.pendingDeletionsLocal());
    }

    @Test
    void anotacaoDoLiderPoeAReservaNoIndiceEAChaveSemReservaEPodada() {
        rawMap.put("r", pending("op-r"));
        catalog.putPlacement("a", active());
        assertTrue(catalog.pendingDeletionsLocal().isEmpty());

        catalog.notePendingDeletion("r");
        catalog.notePendingDeletion("a");

        assertEquals(scanPending(), catalog.pendingDeletionsLocal());
    }

    @Test
    void entradaVelhaDoIndiceEPodadaNaConsulta() {
        catalog.putPlacement("x", pending("op-x"));
        catalog.putPlacement("y", pending("op-y"));
        // A reserva some fora da instância (outro líder concluiu ou cancelou).
        rawMap.put("x", active());
        rawMap.remove("y");

        assertTrue(catalog.pendingDeletionsLocal().isEmpty());
        assertEquals(0, catalog.rebuildPendingDeletions());
        assertTrue(catalog.pendingDeletionsLocal().isEmpty());
    }

    @Test
    void reconstrucaoNaoApagaReservaConhecida() {
        catalog.putPlacement("p", pending("op-p"));
        rawMap.put("q", pending("op-q"));
        catalog.rebuildPendingDeletions();
        catalog.rebuildPendingDeletions();

        assertEquals(scanPending(), catalog.pendingDeletionsLocal());
    }

    @Test
    void podaNaoPerdeReservaGravadaSobOLock() throws Exception {
        // Janela de putPlacement: a chave já está no índice e o escritor segura o lock de placement, mas a
        // reserva ainda não chegou ao catálogo. A poda tem de esperar o lock e reler, sem descartar a chave.
        catalog.putPlacement("k", pending("op-k"));
        rawMap.put("k", active());
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Map<String, SeriesPlacement>> pruned = new AtomicReference<>();
        Thread writer = Thread.ofPlatform().start(() -> {
            try (var guard = CoordinationLocks.acquire(catalog.placementLock("k"))) {
                locked.countDown();
                release.await();
                rawMap.put("k", pending("op-k"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(locked.await(5, TimeUnit.SECONDS));
        Thread pruner = Thread.ofPlatform().start(() -> pruned.set(catalog.pendingDeletionsLocal()));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (pruner.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.WAITING, pruner.getState(), "a poda espera o lock do escritor");
        release.countDown();
        writer.join(5_000);
        pruner.join(5_000);

        assertEquals(Map.of("k", pending("op-k")).keySet(), pruned.get().keySet());
        assertEquals(scanPending(), catalog.pendingDeletionsLocal());
    }

    @Test
    void gravacoesConcorrentesConvergemParaAVarreduraCompleta() throws Exception {
        int threads = 8;
        int keys = 64;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> writers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                writers.add(pool.submit(() -> {
                    start.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    for (int i = 0; i < 400; i++) {
                        String key = "s-" + random.nextInt(keys);
                        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                            switch (random.nextInt(3)) {
                                case 0 -> catalog.putPlacement(key, active());
                                case 1 -> catalog.putPlacement(key, pending("op-" + key + "-" + i));
                                default -> catalog.removePlacement(key);
                            }
                        }
                    }
                    return null;
                }));
            }
            Future<?> reader = pool.submit(() -> {
                start.await();
                while (writers.stream().anyMatch(w -> !w.isDone())) {
                    catalog.pendingDeletionsLocal();
                }
                return null;
            });
            start.countDown();
            for (Future<?> w : writers) w.get(60, TimeUnit.SECONDS);
            reader.get(60, TimeUnit.SECONDS);

            assertEquals(scanPending(), catalog.pendingDeletionsLocal(),
                    "sem reconstrução: o índice mantido nas gravações cobre todas as reservas");
        } finally {
            pool.shutdownNow();
        }
    }
}
