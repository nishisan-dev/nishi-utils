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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link StorageRequestHandler} sem rede real: {@link PlacementLookupFake}
 * substitui o {@code CatalogService}, e um {@link FakeTransport} mínimo (só
 * {@code local()}) satisfaz {@code RequestHandlerSupport} sem subir um
 * {@code NGridNode}. O {@link SeriesHandleRegistry} é real, sobre um
 * {@link BlobVolume} em {@code @TempDir} — é a peça que decide {@code NOT_OPEN}
 * vs. aberto, então fingi-la esconderia justamente o que se quer testar.
 */
class StorageRequestHandlerTest {

    private static final NodeId SELF = NodeId.of("node-self");
    private static final NodeId OTHER = NodeId.of("node-other");
    private static final NodeId SOURCE = NodeId.of("node-client");

    /** Mesmo default programático de {@code StorageNodeConfig} — casa com o {@code seriesPrefix: "series"} do YAML de teste. */
    private static final String SERIES_OBJECT_PREFIX = "series";

    private String yaml;
    private BlobVolumeRegistry volumeRegistry;
    private BlobVolume volume;
    private SeriesHandleRegistry registry;
    private MutableClock clock;
    private PlacementLookupFake placementLookup;
    private StorageRequestHandler handler;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        volumeRegistry = NgrrdBlob.registry().basePath(tempDir).volume("ngrrd").build();
        volume = volumeRegistry.require("ngrrd");
        clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        registry = new SeriesHandleRegistry(volume, "ngrrd", Duration.ofMinutes(15), 10_000, clock);
        placementLookup = new PlacementLookupFake();
        handler = new StorageRequestHandler(new FakeTransport(SELF), placementLookup, registry, volume,
                SERIES_OBJECT_PREFIX, SELF, Durability.FSYNC, OnGeometryChange.FAIL, clock);
    }

    @AfterEach
    void tearDown() {
        registry.close();
        volumeRegistry.close();
    }

    private OpenRequest openRequest(String seriesKey, SeriesPlacement hint) {
        return new OpenRequest(seriesKey, yaml, Map.of(), null, null, hint);
    }

    private OpenRequest openRequestNoCreate(String seriesKey, SeriesPlacement hint) {
        return new OpenRequest(seriesKey, yaml, Map.of(), null, null, hint, false);
    }

    @Test
    void onlineCopyServesWritesAndBarriersUntilTheFinalFreeze() {
        String key = "live-copy";
        var active = SeriesPlacement.active(SELF.value(), 1000);
        placementLookup.put(key, active);
        assertEquals(SeriesStatus.OK, ((SeriesStatusResponse) handler.handle(
                Commands.OPEN, openRequest(key, null), SOURCE)).status());
        placementLookup.put(key, SeriesPlacement.migrating(active, OTHER.value(), "migration", 2000));
        registry.beginMigrationCopy(key);
        var writes = new WriteBatchRequest(List.of(new SeriesWrite(key, "in_octets", 1_700_000_100_000L, 1000)));
        assertEquals(SeriesStatus.OK, ((WriteBatchResponse) handler.handle(
                Commands.WRITE_BATCH, writes, SOURCE)).statusBySeries().get(key));
        assertEquals(SeriesStatus.OK, ((SeriesStatusResponse) handler.handle(
                Commands.CHECKPOINT, new SeriesCommandRequest(key), SOURCE)).status());
        assertEquals(SeriesStatus.MIGRATING, ((SeriesStatusResponse) handler.handle(
                Commands.OPEN, openRequest(key, null), SOURCE)).status(), "geometry changes cannot race the copy");
        registry.markMigrating(key);
        assertEquals(SeriesStatus.MIGRATING, ((WriteBatchResponse) handler.handle(
                Commands.WRITE_BATCH, writes, SOURCE)).statusBySeries().get(key));
    }

    @Test
    void donoConfirmadoNoCatalogoAbreEscreveFazCheckpointELe() {
        String seriesKey = "series-1";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse openResponse = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);
        assertEquals(SeriesStatus.OK, openResponse.status());

        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", t0, 1_000d),
                new SeriesWrite(seriesKey, "in_octets", t0 + baseStepMs, 1_500d)));
        WriteBatchResponse writeResponse = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, batch, SOURCE);
        assertEquals(Map.of(seriesKey, SeriesStatus.OK), writeResponse.statusBySeries());

        SeriesStatusResponse checkpointResponse = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(SeriesStatus.OK, checkpointResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, t0 + 2 * baseStepMs);
        ReadResponse readResponse = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);
        assertEquals(SeriesStatus.OK, readResponse.status());
        assertNotNull(readResponse.result());
    }

    @Test
    void hintConfirmadoPeloLiderAceitaComoOk() {
        // Seção 0 do M3: o hint por si só NUNCA basta — só é aceito quando placementStrong (round-trip
        // real ao líder) confirma o MESMO dono. Réplica local ainda vazia (ex.: corrida líder→dono logo
        // após um PLACE), simulada via putStrongOnly.
        String seriesKey = "series-hint-confirmado";
        SeriesPlacement hint = SeriesPlacement.active(SELF.value(), 1_000L);
        placementLookup.putStrongOnly(seriesKey, hint);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, hint), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void hintNaoConfirmadoPeloLiderRespondeWrongOwner() {
        // Seção 0 do M3 (defeito pré-existente corrigido): sob churn de liderança, o líder pode "não
        // achar" uma série já colocada e mandar um hint de um dono errado — sem confirmação de
        // placementStrong, aceitar o hint criaria uma cópia vazia no lugar errado (dados órfãos no
        // dono verdadeiro). O líder aqui não conhece a série (nem no hint, nem noutro dono).
        String seriesKey = "series-hint-nao-confirmado";
        SeriesPlacement hint = SeriesPlacement.active(SELF.value(), 1_000L);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, hint), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertNull(response.ownerNodeId());
        assertFalse(registry.isOpen(seriesKey), "OPEN não deveria ter criado uma série vazia sem confirmação do líder");
    }

    @Test
    void hintDeUmDonoMasLiderConfirmaOutroRespondeWrongOwnerComODonoCorreto() {
        String seriesKey = "series-hint-divergente";
        SeriesPlacement hint = SeriesPlacement.active(SELF.value(), 1_000L);
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(OTHER.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, hint), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
        assertFalse(registry.isOpen(seriesKey));
    }

    @Test
    void placementDeOutroDonoRespondeWrongOwnerComODonoCorreto() {
        String seriesKey = "series-alheia";
        placementLookup.put(seriesKey, SeriesPlacement.active(OTHER.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
    }

    @Test
    void semPlacementSemHintESemHandleAbertoRespondeWrongOwnerComDonoNulo() {
        String seriesKey = "series-desconhecida";

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertNull(response.ownerNodeId());
    }

    @Test
    void replicaLocalVaziaMasLiderConfirmaSelfComoDonoRespondeNotOpenEmVezDeWrongOwner() {
        // Simula o catálogo persistido ainda não convergido após um restart: a réplica LOCAL não tem a
        // série, mas o líder (placementStrong) confirma que o dono é este próprio nó. Sem handle aberto
        // nem definição em cache, o self-healing decide NOT_OPEN — nunca WRONG_OWNER(null), que faria o
        // cliente re-enfileirar para sempre num nó que na verdade é o dono correto (achado do Debugger).
        String seriesKey = "series-catalogo-nao-convergido";
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.NOT_OPEN, response.status());
    }

    @Test
    void replicaLocalVaziaMasLiderConfirmaOutroDonoRespondeWrongOwnerComODono() {
        String seriesKey = "series-catalogo-nao-convergido-outro-dono";
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(OTHER.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
    }

    @Test
    void writeBatchComDonoConfirmadoMasSemDefinicaoCachedaNaoCriaSerieNoVolume() {
        // Seção 0 do M3: "writeBatch/read nunca criam" — mesmo com o líder confirmando este nó como
        // dono, sem handle aberto nem definição em cache (registry.reopenIfKnown não tem o que reabrir)
        // o self-healing decide NOT_OPEN, nunca cria a série do zero a partir de um WRITE_BATCH.
        String seriesKey = "series-write-sem-definicao";
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        WriteBatchRequest batch = new WriteBatchRequest(List.of(new SeriesWrite(seriesKey, "in_octets", 1_000L, 1d)));
        WriteBatchResponse response = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, batch, SOURCE);

        assertEquals(Map.of(seriesKey, SeriesStatus.NOT_OPEN), response.statusBySeries());
        assertFalse(registry.isOpen(seriesKey), "WRITE_BATCH não deveria ter criado a série no volume");
    }

    @Test
    void consultaAoLiderEhCacheadaNegativamentePorCincoSegundos() {
        String seriesKey = "series-nunca-colocada";

        handler.handle(Commands.FLUSH, new SeriesCommandRequest(seriesKey), SOURCE);
        handler.handle(Commands.FLUSH, new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(1, placementLookup.strongCalls(), "a 2a consulta dentro de 5s deveria usar o cache negativo");

        clock.advance(Duration.ofSeconds(6));
        handler.handle(Commands.FLUSH, new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(2, placementLookup.strongCalls(), "após o TTL, uma nova consulta ao líder é esperada");
    }

    @Test
    void cacheNegativoExpiradoEhVarridoDoMapaAoRegistrarUmaNovaEntrada() throws Exception {
        // item 7 (achado do Refuter): uma série que recebe um cache negativo e nunca mais é consultada
        // não pode ficar parada no mapa para sempre — sem alguma varredura, o mapa cresceria sem limite
        // ao longo da vida do processo. A varredura acontece ao registrar uma nova entrada negativa.
        String staleKey = "series-nunca-mais-consultada";
        String freshKey = "series-outra-nunca-colocada";

        handler.handle(Commands.FLUSH, new SeriesCommandRequest(staleKey), SOURCE);
        assertEquals(1, negativeCacheSize());

        clock.advance(Duration.ofSeconds(6));
        handler.handle(Commands.FLUSH, new SeriesCommandRequest(freshKey), SOURCE);

        assertEquals(1, negativeCacheSize(),
                "a entrada expirada de " + staleKey + " deveria ter sido varrida ao inserir " + freshKey);
    }

    private int negativeCacheSize() throws Exception {
        Field field = StorageRequestHandler.class.getDeclaredField("negativeLookupCacheExpiryMs");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(handler)).size();
    }

    @Test
    void donoSemHandleAbertoRespondeNotOpen() {
        String seriesKey = "series-fechada";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.NOT_OPEN, response.status());
    }

    @Test
    void serieMarcadaMigratingBloqueiaOperacaoMesmoSendoODono() {
        String seriesKey = "series-migrando";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        registry.markMigrating(seriesKey);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.MIGRATING, response.status());
    }

    @Test
    void placementEmMigracaoRespondeMigratingComODonoAtual() {
        String seriesKey = "series-em-migracao";
        SeriesPlacement migrating = SeriesPlacement.migrating(
                SeriesPlacement.active(SELF.value(), 1_000L), OTHER.value(), "migration-1", 2_000L);
        assertEquals(PlacementState.MIGRATING, migrating.state());
        placementLookup.put(seriesKey, migrating);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.MIGRATING, response.status());
        assertEquals(SELF.value(), response.ownerNodeId());
    }

    @Test
    void serieEsquecidaComReplicaLocalDesatualizadaEliderConfirmandoOutroDonoRespondeWrongOwner() {
        // Próximo passo do M3: depois de um MIGRATE_FINISH (simulado aqui por registry.forget), a
        // réplica LOCAL do catálogo pode continuar dizendo ACTIVE(self) por um instante — ownership()
        // não pode confiar nela (nem no hint, ausente aqui) enquanto a série estiver isForgotten; só
        // placementStrong decide, e o líder já confirma o novo dono (OTHER).
        String seriesKey = "series-migrada-replica-local-atrasada";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        registry.forget(seriesKey);
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(OTHER.value(), 2_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
        assertFalse(registry.isOpen(seriesKey),
                "OPEN não deveria recriar a série esquecida sem confirmação forte do líder");
    }

    @Test
    void serieEsquecidaComLiderConfirmandoSelfAbreEDesmarcaEsquecida() {
        // Caso simétrico: a série voltou a este nó de verdade (ex.: um rebalanceamento posterior) e o
        // líder confirma ACTIVE(self) — o OPEN deve prosseguir e a marca de esquecida deve ser limpa.
        String seriesKey = "series-migrada-de-volta";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        registry.forget(seriesKey);
        placementLookup.putStrongOnly(seriesKey, SeriesPlacement.active(SELF.value(), 3_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertFalse(registry.isForgotten(seriesKey),
                "a marca de esquecida deveria ter sido limpa após a confirmação forte do líder");
        assertTrue(registry.isOpen(seriesKey));
    }

    @Test
    void writeBatchComVariasSeriesDevolveStatusMistoPorSerie() {
        String owned = "series-dono-aberta";
        String ownedButClosed = "series-dono-fechada";
        String notOwned = "series-de-outro-no";

        placementLookup.put(owned, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(owned, null), SOURCE);
        placementLookup.put(ownedButClosed, SeriesPlacement.active(SELF.value(), 1_000L));
        placementLookup.put(notOwned, SeriesPlacement.active(OTHER.value(), 1_000L));

        long ts = 1_700_000_000_000L;
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(owned, "in_octets", ts, 1.0),
                new SeriesWrite(ownedButClosed, "in_octets", ts, 1.0),
                new SeriesWrite(notOwned, "in_octets", ts, 1.0)));

        WriteBatchResponse response = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, batch, SOURCE);

        assertEquals(SeriesStatus.OK, response.statusBySeries().get(owned));
        assertEquals(SeriesStatus.NOT_OPEN, response.statusBySeries().get(ownedButClosed));
        assertEquals(SeriesStatus.WRONG_OWNER, response.statusBySeries().get(notOwned));
        assertEquals(OTHER.value(), response.ownerBySeries().get(notOwned));
    }

    @Test
    void excecaoDuranteLeituraViraStatusErrorComMensagem() {
        String seriesKey = "series-cf-invalido";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        // "MIN" não é declarado por nenhuma RRA da definição (só AVERAGE/MAX) -> NgrrdQueryException
        // síncrona dentro de NgrrdReader.read, capturada pelo handler e convertida em ERROR.
        ReadRequest badRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.MIN, 500, 1_700_000_000_000L);

        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, badRequest, SOURCE);

        assertEquals(SeriesStatus.ERROR, response.status());
        assertNotNull(response.message());
        assertNull(response.result());
    }

    @Test
    void closeEhIdempotenteMesmoSemHandleAberto() {
        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CLOSE,
                new SeriesCommandRequest("nunca-existiu"), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void readAposFechamentoPorOciosidadeSeAutoCuraEDevolveOsDados() {
        // A1: read/readPreset agora passam por withHandleSelfHealing — um fechamento por
        // ociosidade/LRU (nunca um CLOSE explícito) não deveria exigir um novo OPEN do cliente.
        String seriesKey = "series-ociosa";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", t0, 1_000d),
                new SeriesWrite(seriesKey, "in_octets", t0 + baseStepMs, 1_500d)));
        handler.handle(Commands.WRITE_BATCH, batch, SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);

        clock.advance(Duration.ofMinutes(16));
        assertEquals(1, registry.closeIdle());
        assertTrue(registry.existing(seriesKey).isEmpty(), "closeIdle deveria ter fechado o handle");

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, t0 + 2 * baseStepMs);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertNotNull(response.result());
        assertFalse(response.result().points().isEmpty(), "esperava dados após a auto-cura do READ");
    }

    @Test
    void readAposCloseExplicitoRespondeNotOpen() {
        String seriesKey = "series-fechada-close";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        SeriesStatusResponse closeResponse = (SeriesStatusResponse) handler.handle(Commands.CLOSE,
                new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(SeriesStatus.OK, closeResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, 1_700_000_000_000L);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.NOT_OPEN, response.status());
    }

    @Test
    void readAposCloseSeguidoDeNovoOpenVoltaAFuncionar() {
        String seriesKey = "series-close-depois-open";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        handler.handle(Commands.CLOSE, new SeriesCommandRequest(seriesKey), SOURCE);

        SeriesStatusResponse reopenResponse = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);
        assertEquals(SeriesStatus.OK, reopenResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, 1_700_000_000_000L);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void handleLocalDespachaComandoAtendidoEHandlesDistingueComandoDeOutroHandler() {
        String seriesKey = "series-local";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        assertTrue(handler.handles(Commands.OPEN), "OPEN é um OWNER_COMMAND");
        Object result = handler.handleLocal(Commands.OPEN, openRequest(seriesKey, null));
        assertEquals(SeriesStatus.OK, ((SeriesStatusResponse) result).status());

        assertFalse(handler.handles(Commands.PLACE), "PLACE não é um OWNER_COMMAND");
    }

    @Test
    void metricsSnapshotContabilizaOperacoesEErros() {
        String seriesKey = "series-metricas";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        long ts = 1_700_000_000_000L;
        handler.handle(Commands.WRITE_BATCH, new WriteBatchRequest(
                List.of(new SeriesWrite(seriesKey, "in_octets", ts, 1.0))), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest("nunca-aberta"), SOURCE);

        StorageRequestHandler.StorageHandlerMetrics metrics = handler.metricsSnapshot();

        assertEquals(1, metrics.writeBatches());
        assertEquals(1, metrics.samplesWritten());
        assertEquals(1, metrics.checkpoints());
        assertEquals(1L, metrics.errorsByStatus().get(SeriesStatus.WRONG_OWNER));
    }

    @Test
    void metricsSnapshotPreencheLatenciasDeWriteBatchCheckpointEReadAposOperacoes() {
        String seriesKey = "series-latencias";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", t0, 1_000d),
                new SeriesWrite(seriesKey, "in_octets", t0 + baseStepMs, 1_500d)));
        handler.handle(Commands.WRITE_BATCH, batch, SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);
        handler.handle(Commands.FLUSH, new SeriesCommandRequest(seriesKey), SOURCE);
        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, t0 + 2 * baseStepMs);
        handler.handle(Commands.READ, readRequest, SOURCE);

        StorageRequestHandler.StorageHandlerMetrics metrics = handler.metricsSnapshot();

        assertEquals(1L, metrics.writeBatchLatency().count());
        assertTrue(metrics.writeBatchLatency().maxMicros() >= 0);
        assertEquals(1L, metrics.checkpointLatency().count());
        assertEquals(1L, metrics.readLatency().count());
        assertEquals(1L, metrics.flushes());
    }

    @Test
    void openComPrefixoDivergenteDaDefinicaoRespondeErrorSemAbrir() {
        // MÉDIO-6 do Refuter: todas as definições servidas por um cluster devem usar o mesmo
        // storage.objectNaming.seriesPrefix — este nó está configurado com "series" (default), a
        // definição abaixo declara "legacy-series".
        String customYaml = yaml.replace("seriesPrefix: \"series\"", "seriesPrefix: \"legacy-series\"");
        assertTrue(customYaml.contains("legacy-series"), "fixture deveria ter substituído o seriesPrefix");
        String seriesKey = "series-prefixo-divergente";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                new OpenRequest(seriesKey, customYaml, Map.of(), null, null, null), SOURCE);

        assertEquals(SeriesStatus.ERROR, response.status());
        assertTrue(response.message() != null && response.message().contains("legacy-series"),
                "mensagem deveria citar o prefixo divergente: " + response.message());
        assertFalse(registry.isOpen(seriesKey), "não deveria ter aberto a série com prefixo divergente");
    }

    @Test
    void seriesExistsRespondeTrueSemLerOObjetoInteiro() {
        // BAIXO-E do Refuter: bytes é sempre -1 quando exists=true — handleSeriesExists nunca chama
        // storage().get() (que carregaria o objeto inteiro), só storage().exists().
        String seriesKey = "series-exists-presente";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        handler.handle(Commands.WRITE_BATCH, new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", 1_700_000_000_000L, 1_000d))), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);

        SeriesExistsResponse response = (SeriesExistsResponse) handler.handle(Commands.SERIES_EXISTS,
                new SeriesExistsRequest(seriesKey), SOURCE);

        assertTrue(response.exists());
        assertEquals(-1L, response.bytes(), "bytes deveria ser -1 (sem API barata de tamanho) quando exists=true");
    }

    @Test
    void seriesExistsRespondeFalseSemAbrirHandleQuandoAusente() {
        SeriesExistsResponse response = (SeriesExistsResponse) handler.handle(Commands.SERIES_EXISTS,
                new SeriesExistsRequest("series-inexistente"), SOURCE);

        assertFalse(response.exists());
        assertEquals(0L, response.bytes());
        assertFalse(registry.isOpen("series-inexistente"), "SERIES_EXISTS nunca deveria abrir handle");
    }

    @Test
    void openSemCriarComObjetoAusenteRespondeNotFoundENadaCria() {
        String seriesKey = "series-sem-criar-ausente";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequestNoCreate(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.NOT_FOUND, response.status());
        assertFalse(registry.isOpen(seriesKey), "OPEN sem criar não deveria ter aberto a série");
        assertFalse(volume.storage().exists(SeriesObjectKeys.objectKey(SERIES_OBJECT_PREFIX, seriesKey)),
                "OPEN sem criar não deveria ter criado o objeto físico");
    }

    @Test
    void openSemCriarComObjetoPresenteAbre() {
        String seriesKey = "series-sem-criar-presente";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        handler.handle(Commands.CLOSE, new SeriesCommandRequest(seriesKey), SOURCE);
        assertTrue(volume.storage().exists(SeriesObjectKeys.objectKey(SERIES_OBJECT_PREFIX, seriesKey)),
                "setup deveria ter deixado o objeto físico no volume após o close");
        assertFalse(registry.isOpen(seriesKey), "setup deveria ter fechado o handle antes do OPEN sem criar");

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequestNoCreate(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertTrue(registry.isOpen(seriesKey));
    }

    @Test
    void openSemFlagContinuaCriando() {
        String seriesKey = "series-sem-flag-cria";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                new OpenRequest(seriesKey, yaml, Map.of(), null, null, null, null), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertTrue(registry.isOpen(seriesKey));
        assertTrue(volume.storage().exists(SeriesObjectKeys.objectKey(SERIES_OBJECT_PREFIX, seriesKey)));
    }

    @Test
    void openSemCriarComSerieJaAbertaRespondeOk() {
        String seriesKey = "series-sem-criar-ja-aberta";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        assertTrue(registry.isOpen(seriesKey));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequestNoCreate(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void openSemCriarDeNaoDonoContinuaRespondendoWrongOwner() {
        // Ownership é decidido ANTES do NOT_FOUND: mesmo com createIfMissing=false, um nó que não é
        // dono continua respondendo WRONG_OWNER, nunca NOT_FOUND.
        String seriesKey = "series-sem-criar-nao-dono";
        placementLookup.put(seriesKey, SeriesPlacement.active(OTHER.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequestNoCreate(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
    }

    @Test
    void seriesExistsBatchDevolveSoAsPresentes() {
        String present = "series-batch-presente";
        String absent = "series-batch-ausente";
        placementLookup.put(present, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(present, null), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(present), SOURCE);

        SeriesExistsBatchResponse response = (SeriesExistsBatchResponse) handler.handle(
                Commands.SERIES_EXISTS_BATCH, new SeriesExistsBatchRequest(List.of(present, absent)), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(Set.of(present), response.present());
    }

    @Test
    void openSemCriarComRegistryLancandoSeriesNotFoundExceptionRespondeNotFound(@TempDir Path checkOnlyDir)
            throws IOException {
        // Cobre o catch(SeriesNotFoundException) de openWithMetadata — a corrida em que o objeto existe
        // no instante do pré-check, mas sumiu quando registry.open() de fato tenta abrir (ex.: apagado
        // por um reconciler entre as duas chamadas). Não há hook de produção para pausar exatamente
        // entre o pré-check e o registry.open() dentro do mesmo método síncrono, então a divergência é
        // obtida por um seam JÁ EXISTENTE no construtor de StorageRequestHandler: `volume` (usado só
        // pelo pré-check e por SERIES_EXISTS/BATCH) é um parâmetro INDEPENDENTE do volume interno da
        // SeriesHandleRegistry (usado pelo open de fato). Aqui o "volume de checagem" tem o objeto
        // (pré-check vê exists=true); o registry real (do setUp, compartilhado com este handler racy)
        // nunca teve o objeto — registry.open() lança SeriesNotFoundException de verdade, capturada
        // pelo catch de openWithMetadata.
        String seriesKey = "series-corrida-check-open";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        try (BlobVolumeRegistry checkVolumeRegistry = NgrrdBlob.registry().basePath(checkOnlyDir).volume("ngrrd").build()) {
            BlobVolume checkVolume = checkVolumeRegistry.require("ngrrd");
            try (SeriesHandleRegistry checkOnlyRegistry = new SeriesHandleRegistry(
                    checkVolume, "ngrrd", Duration.ofMinutes(15), 10_000, clock)) {
                checkOnlyRegistry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
                checkOnlyRegistry.close(seriesKey);
            }
            String objectKey = SeriesObjectKeys.objectKey(SERIES_OBJECT_PREFIX, seriesKey);
            assertTrue(checkVolume.storage().exists(objectKey),
                    "setup deveria ter deixado o objeto físico só no volume de checagem");
            assertFalse(volume.storage().exists(objectKey),
                    "o volume real do registry nunca deveria ter recebido este objeto");

            StorageRequestHandler racyHandler = new StorageRequestHandler(new FakeTransport(SELF), placementLookup,
                    registry, checkVolume, SERIES_OBJECT_PREFIX, SELF, Durability.FSYNC, OnGeometryChange.FAIL, clock);

            SeriesStatusResponse response = (SeriesStatusResponse) racyHandler.handle(Commands.OPEN,
                    openRequestNoCreate(seriesKey, null), SOURCE);

            assertEquals(SeriesStatus.NOT_FOUND, response.status());
            assertFalse(registry.isOpen(seriesKey),
                    "não deveria ter aberto a série real após a SeriesNotFoundException do registry.open()");
        }
    }

    @Test
    void seriesExistsBatchAcimaDoLimiteRespondeErro() {
        List<String> tooMany = IntStream.rangeClosed(1, SeriesExistsBatchRequest.MAX_KEYS + 1)
                .mapToObj(i -> "series-batch-" + i)
                .collect(Collectors.toList());

        SeriesExistsBatchResponse response = (SeriesExistsBatchResponse) handler.handle(
                Commands.SERIES_EXISTS_BATCH, new SeriesExistsBatchRequest(tooMany), SOURCE);

        assertEquals(SeriesStatus.ERROR, response.status());
        assertTrue(response.present().isEmpty());
    }

    /** {@link Clock} determinístico para forçar fechamento por ociosidade via {@link SeriesHandleRegistry#closeIdle()}. */
    private static final class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant start) {
            this.instant = start;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("não usado nos testes");
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    /**
     * Fake de {@link StorageRequestHandler.PlacementLookup}: dois mapas em memória — {@code local}
     * (réplica eventual, pode ficar vazia mesmo com a série colocada — simula o catálogo logo após um
     * restart) e {@code strong} (o que o líder responderia num round-trip). {@code strongCalls} conta
     * as consultas de {@link #placementStrong}, para os testes confirmarem o cache negativo do handler.
     */
    private static final class PlacementLookupFake implements StorageRequestHandler.PlacementLookup {
        private final Map<String, SeriesPlacement> local = new HashMap<>();
        private final Map<String, SeriesPlacement> strong = new HashMap<>();
        private int strongCalls;

        void put(String seriesKey, SeriesPlacement placement) {
            local.put(seriesKey, placement);
            strong.put(seriesKey, placement);
        }

        /** Coloca a série apenas na visão forte (líder), simulando réplica local ainda não convergida. */
        void putStrongOnly(String seriesKey, SeriesPlacement placement) {
            strong.put(seriesKey, placement);
        }

        int strongCalls() {
            return strongCalls;
        }

        @Override
        public Optional<SeriesPlacement> placementLocal(String seriesKey) {
            return Optional.ofNullable(local.get(seriesKey));
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            strongCalls++;
            return Optional.ofNullable(strong.get(seriesKey));
        }
    }

    /** {@link Transport} mínimo: só {@code local()} é usado por {@code RequestHandlerSupport} neste teste. */
    private static final class FakeTransport implements Transport {
        private final NodeInfo local;

        FakeTransport(NodeId id) {
            this.local = new NodeInfo(id, "127.0.0.1", 0);
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            return List.of();
        }

        @Override
        public void addListener(TransportListener listener) {
        }

        @Override
        public void removeListener(TransportListener listener) {
        }

        @Override
        public void broadcast(ClusterMessage message) {
        }

        @Override
        public void send(ClusterMessage message) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            return new CompletableFuture<>();
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return false;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return false;
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() {
        }
    }
}
