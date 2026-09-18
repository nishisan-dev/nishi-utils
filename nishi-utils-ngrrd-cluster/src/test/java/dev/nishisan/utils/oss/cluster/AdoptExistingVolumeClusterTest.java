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

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeConfig;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.client.SeriesKeyTemplate;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Seção 5 da spec do M4 — também o caminho de <b>migração do ngrrd single-node para o cluster</b>
 * (seção 2 da spec): 20 séries são criadas com o oss single-node ({@code Ngrrd.open} direto sobre um
 * {@link BlobVolume}, sem cluster nenhum), e só então um storage node novo sobe apontando para esse
 * MESMO volume, dentro de um cluster de 2 nós. O {@code LocalReconciler} do nó novo deve adotar as 20
 * séries (uma a uma, via {@code ngrrd.place}) sem que nenhum dado seja perdido — o cliente do cluster lê
 * de volta os dados escritos antes do cluster sequer existir.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdoptExistingVolumeClusterTest {

    private static final int SERIES_COUNT = 20;
    private static final int SETUP_SAMPLES = 3;
    private static final long BASE_STEP_MS = 300_000L;
    private static final String LEGACY_VOLUME_NAME = "ngrrd";
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(120);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void storageNodeNovoApontandoParaVolumeExistenteAdotaTodasAsSeries(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        Path legacyBase = base.resolve("legacy-volume-base");

        // 1) Cria as 20 séries com o oss single-node, SEM cluster algum — mesma API que um usuário do
        // ngrrd standalone usaria hoje.
        Map<String, Long> sampleCountBySeriesKey = new LinkedHashMap<>();
        String template = SeriesKeyTemplate.templateOf(yaml);
        BlobVolumeRegistry legacyRegistry = NgrrdBlob.registry()
                .basePath(legacyBase)
                .shardCount(BlobVolumeConfig.DEFAULT_SHARD_COUNT)
                .segmentBytes(BlobVolumeConfig.DEFAULT_SEGMENT_BYTES)
                .initialShardCapacityBytes(BlobVolumeConfig.DEFAULT_SEGMENT_BYTES)
                .volume(LEGACY_VOLUME_NAME)
                .build();
        try {
            BlobVolume legacyVolume = legacyRegistry.require(LEGACY_VOLUME_NAME);
            for (int i = 0; i < SERIES_COUNT; i++) {
                Map<String, String> tags = Map.of("deviceId", "legacy" + i, "interfaceId", "eth0",
                        "region", "br-sp", "vendor", "x", "role", "core");
                String seriesKey = SeriesKeyTemplate.resolve(template, tags);
                NgrrdHandle handle = Ngrrd.open(legacyVolume, NgrrdUri.of(LEGACY_VOLUME_NAME, seriesKey), yaml,
                        Ngrrd.OpenOptions.defaults());
                long t0 = alignedBase(seriesKey);
                for (int sample = 0; sample < SETUP_SAMPLES; sample++) {
                    long ts = t0 + sample * BASE_STEP_MS;
                    handle.write("in_octets", new Sample(ts, 1_000d + sample * 1_000d));
                    handle.write("out_octets", new Sample(ts, 500d + sample * 500d));
                }
                handle.checkpoint();
                handle.close();
                sampleCountBySeriesKey.put(seriesKey, (long) SETUP_SAMPLES);
            }
        } finally {
            legacyRegistry.close();
        }
        assertEquals(SERIES_COUNT, sampleCountBySeriesKey.size(), "seriesKey deveria ser único por conjunto de tags");

        // 2) Sobe um cluster de 2 nós: o primeiro com um volume novo e vazio, o segundo apontando para
        // o MESMO diretório físico usado acima — reconcileInterval/orphanGrace curtos só para não
        // esperar os defaults de produção (10 min / 5 min) num teste.
        harness = NgrrdClusterTestHarness.start(base, 1, builder -> builder
                .reconcileInterval(Duration.ofSeconds(2))
                .orphanGrace(Duration.ofSeconds(5)));
        harness.awaitLeader();
        harness.awaitNodeStatuses(1);

        NgrrdStorageNode adopter = harness.addStorageNode(builder -> builder
                .volumeDir(legacyBase)
                .volumeName(LEGACY_VOLUME_NAME)
                .shardCount(BlobVolumeConfig.DEFAULT_SHARD_COUNT)
                .segmentBytes(BlobVolumeConfig.DEFAULT_SEGMENT_BYTES)
                .initialShardCapacityBytes(BlobVolumeConfig.DEFAULT_SEGMENT_BYTES)
                .reconcileInterval(Duration.ofSeconds(2))
                .orphanGrace(Duration.ofSeconds(5)));
        harness.awaitNodeStatuses(2);

        // 3) O LocalReconciler do nó novo adota as 20 séries — todas ACTIVE, todas no nó novo.
        harness.awaitPlacements(SERIES_COUNT);
        awaitTrue("as " + SERIES_COUNT + " séries adotadas deveriam pertencer ao nó novo (" + adopter.nodeId() + ")",
                () -> {
                    Map<String, SeriesPlacement> placements = harness.nodes().get(0).catalog().placementsLocal();
                    return placements.size() == SERIES_COUNT && placements.values().stream()
                            .allMatch(p -> p.state() == PlacementState.ACTIVE && p.ownerNodeId().equals(adopter.nodeId()));
                });

        // 4) O cliente do cluster lê de volta os dados escritos ANTES do cluster existir.
        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(20)));
        try {
            for (int i = 0; i < SERIES_COUNT; i++) {
                Map<String, String> tags = Map.of("deviceId", "legacy" + i, "interfaceId", "eth0",
                        "region", "br-sp", "vendor", "x", "role", "core");
                NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
                String seriesKey = handle.seriesKey();
                long endExclusive = alignedBase(seriesKey) + (SETUP_SAMPLES + 1) * BASE_STEP_MS;
                ViewQuery query = new ViewQuery(Duration.ofDays(1), (int) (BASE_STEP_MS / 1_000L),
                        ConsolidationFunction.AVERAGE, 500);
                SeriesResult result = awaitReadWithPoints(handle, query, endExclusive);
                long nonNullPoints = result.points().stream().filter(p -> !Double.isNaN(p.value())).count();
                assertTrue(nonNullPoints > 0, "série " + seriesKey + " deveria ter dados legíveis após a adoção "
                        + "(pontos=" + result.points().size() + ")");
            }
        } finally {
            client.close();
        }
    }

    private static SeriesResult awaitReadWithPoints(NgrrdHandle handle, ViewQuery query, long endExclusive)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        SeriesResult last = null;
        do {
            try {
                last = handle.read("in_bps", query, endExclusive);
                if (last.points().stream().anyMatch(point -> !Double.isNaN(point.value()))) {
                    return last;
                }
            } catch (NgrrdClusterException ignored) {
                // transitório (série ainda reabrindo no novo dono) — o laço tenta de novo
            }
            Thread.sleep(200L);
        } while (System.currentTimeMillis() < deadline);
        return last != null ? last : handle.read("in_bps", query, endExclusive);
    }

    private static long alignedBase(String seriesKey) {
        long base = 1_700_000_000_000L + Math.floorMod(seriesKey.hashCode(), 1_000) * BASE_STEP_MS;
        return base - (base % BASE_STEP_MS);
    }

    private static <T> T retryUntilSuccess(Supplier<T> action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        NgrrdClusterException lastFailure = null;
        do {
            try {
                return action.get();
            } catch (NgrrdClusterException e) {
                lastFailure = e;
                Thread.sleep(200L);
            }
        } while (System.currentTimeMillis() < deadline);
        throw lastFailure;
    }

    private void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }
}
