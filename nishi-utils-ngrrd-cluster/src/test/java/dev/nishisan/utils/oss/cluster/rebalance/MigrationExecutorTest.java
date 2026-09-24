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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.SeriesHandleRegistry;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateChunkRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigratePrepareRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateCommitRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStartRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre {@link MigrationExecutor} com dois volumes REAIS em {@code @TempDir} (origem e destino) e um
 * {@link RoutingClusterRpc} fake que despacha {@code MIGRATE_*} entre os dois executores diretamente
 * (sem rede) — é justamente o {@link SeriesHandleRegistry}/{@code BlobStorage} reais que decidem se a
 * migração de fato leu/gravou os bytes certos, então fingi-los esconderia o que se quer testar.
 */
class MigrationExecutorTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(15);
    private static final long MAX_SERIES_BYTES = 64L * 1024L * 1024L;

    private BlobVolumeRegistry srcVolumeRegistry;
    private BlobVolumeRegistry dstVolumeRegistry;
    private BlobVolume srcVolume;
    private BlobVolume dstVolume;
    private SeriesHandleRegistry srcRegistry;
    private SeriesHandleRegistry dstRegistry;
    private MigrationExecutor srcExecutor;
    private MigrationExecutor dstExecutor;
    private RoutingClusterRpc rpc;
    private FakeCatalogView srcCatalog;
    private FakeCatalogView dstCatalog;
    private String yaml;

    private static final NodeId SRC = NodeId.of("storage-src");
    private static final NodeId DST = NodeId.of("storage-dst");

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        srcVolumeRegistry = NgrrdBlob.registry().basePath(tempDir.resolve("src")).volume("ngrrd").build();
        srcVolume = srcVolumeRegistry.require("ngrrd");
        dstVolumeRegistry = NgrrdBlob.registry().basePath(tempDir.resolve("dst")).volume("ngrrd").build();
        dstVolume = dstVolumeRegistry.require("ngrrd");

        srcRegistry = new SeriesHandleRegistry(srcVolume, "ngrrd", Duration.ofMinutes(15), 10_000, Clock.systemUTC());
        dstRegistry = new SeriesHandleRegistry(dstVolume, "ngrrd", Duration.ofMinutes(15), 10_000, Clock.systemUTC());

        rpc = new RoutingClusterRpc();
        // Each transfer test publishes the coordinator's MIGRATING placement explicitly.
        srcCatalog = new FakeCatalogView();
        dstCatalog = new FakeCatalogView();
        srcExecutor = newExecutor(SRC, srcRegistry, srcVolume, srcCatalog, 4_096L);
        dstExecutor = newExecutor(DST, dstRegistry, dstVolume, dstCatalog, 4_096L);
        rpc.register(SRC, srcExecutor);
        rpc.register(DST, dstExecutor);
    }

    private MigrationExecutor newExecutor(NodeId id, SeriesHandleRegistry registry, BlobVolume volume,
            CatalogView catalog, long chunkBytes) {
        return new MigrationExecutor(new FakeTransport(id), registry, volume, rpc, catalog, id, chunkBytes,
                MAX_SERIES_BYTES, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() {
        srcExecutor.close();
        dstExecutor.close();
        srcRegistry.close();
        dstRegistry.close();
        srcVolumeRegistry.close();
        dstVolumeRegistry.close();
    }

    private byte[] writeAndCheckpointSeries(String seriesKey) {
        NgrrdHandle handle = srcRegistry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        for (int i = 0; i < 20; i++) {
            handle.write("in_octets", new Sample(t0 + i * baseStepMs, 1_000d + i));
            handle.write("out_octets", new Sample(t0 + i * baseStepMs, 500d + i));
        }
        handle.checkpoint();
        return srcVolume.storage().get(objectKey(seriesKey))
                .orElseThrow(() -> new AssertionError("série não gravada no volume de origem"));
    }

    /** {@code {seriesPrefix}/{seriesKey}.ngrr}, conforme o {@code objectNaming} de {@code iface-traffic-blob.yaml}. */
    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    private MigrateResponse status(MigrationExecutor executor, String migrationId) {
        return (MigrateResponse) executor.handleLocal(Commands.MIGRATE_STATUS,
                new MigrateControlRequest("qualquer-serie", migrationId));
    }

    private void awaitCommittedOnSource(String migrationId) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            MigrateResponse response = status(srcExecutor, migrationId);
            if (response.status() == MigrateStatus.COMMITTED) {
                return;
            }
            if (response.status() == MigrateStatus.ERROR) {
                fail("migração falhou: " + response.message());
            }
            sleepQuietly();
        }
        fail("migração não chegou a COMMITTED a tempo");
    }

    private static void sleepQuietly() {
        try {
            Thread.sleep(20L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void origemLeEFragmentaDestinoReconstroiEValidaSha() {
        String seriesKey = "series-happy-path";
        byte[] original = writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        MigrateResponse start = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, migrationId, DST.value()));
        assertEquals(MigrateStatus.OK, start.status());

        awaitCommittedOnSource(migrationId);

        assertTrue(rpc.chunkCallCount() > 1, "a série deveria ter sido fragmentada em mais de um chunk");
        Optional<byte[]> atDest = dstVolume.storage().get(objectKey(seriesKey));
        assertTrue(atDest.isPresent());
        assertArrayEquals(original, atDest.get());
        assertEquals(sha256Hex(original), sha256Hex(atDest.get()));
    }

    @Test
    void mismatchDeShaDescartaOStagingEFasesFailed() {
        String seriesKey = "series-hash-mismatch";
        byte[] original = writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        // Envia os chunks manualmente e faz o commit com um SHA errado.
        byte[] data = original;
        chunk(seriesKey, migrationId, data);
        String wrongSha = "0".repeat(64);
        MigrateResponse commit = (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(seriesKey, migrationId, wrongSha, data.length, objectKey(seriesKey)));

        assertEquals(MigrateStatus.HASH_MISMATCH, commit.status());
        assertFalse(dstVolume.storage().exists(objectKey(seriesKey)),
                "nada deveria ter sido ativado no destino após o mismatch");
    }

    @Test
    void commitEhIdempotenteAposJaConfirmado() {
        String seriesKey = "series-commit-idempotente";
        byte[] original = writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        String sha = sha256Hex(original);
        chunk(seriesKey, migrationId, original);
        MigrateResponse first = (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(seriesKey, migrationId, sha, original.length, objectKey(seriesKey)));
        assertEquals(MigrateStatus.COMMITTED, first.status());

        MigrateResponse second = (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(seriesKey, migrationId, sha, original.length, objectKey(seriesKey)));
        assertEquals(MigrateStatus.COMMITTED, second.status());
        assertEquals(first.bytes(), second.bytes());
    }

    @Test
    void abortAposCommitApagaACopiaNoDestino() {
        String seriesKey = "series-abort-pos-commit";
        byte[] original = writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);
        String sha = sha256Hex(original);
        chunk(seriesKey, migrationId, original);
        dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(seriesKey, migrationId, sha, original.length, objectKey(seriesKey)));
        assertTrue(dstVolume.storage().exists(objectKey(seriesKey)));

        MigrateResponse abort = (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_ABORT,
                new MigrateControlRequest(seriesKey, migrationId));

        assertEquals(MigrateStatus.OK, abort.status());
        assertFalse(dstVolume.storage().exists(objectKey(seriesKey)),
                "abort após commit deveria apagar a cópia no destino");
    }

    @Test
    void abortAposCommitNaoApagaSeOLiderConfirmaAtivaNoDestino() {
        // Achado bloqueante do Refuter: um ABORT pode chegar depois que outro líder já completou de
        // verdade a migração (dual-leader/partição) — apagar aqui apagaria a ÚNICA cópia real da série.
        String seriesKey = "series-abort-pos-commit-ativa";
        byte[] original = writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);
        String sha = sha256Hex(original);
        chunk(seriesKey, migrationId, original);
        dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(seriesKey, migrationId, sha, original.length, objectKey(seriesKey)));
        assertTrue(dstVolume.storage().exists(objectKey(seriesKey)));
        dstCatalog.put(seriesKey, SeriesPlacement.active(DST.value(), 1_000L));

        MigrateResponse abort = (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_ABORT,
                new MigrateControlRequest(seriesKey, migrationId));

        assertEquals(MigrateStatus.OK, abort.status());
        assertTrue(dstVolume.storage().exists(objectKey(seriesKey)),
                "não deveria apagar a cópia já ativa segundo o líder");
    }

    @Test
    void finishApagaACopiaNaOrigem() {
        String seriesKey = "series-finish";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        MigrateResponse start = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, migrationId, DST.value()));
        assertEquals(MigrateStatus.OK, start.status());
        awaitCommittedOnSource(migrationId);
        assertTrue(srcVolume.storage().exists(objectKey(seriesKey)),
                "a origem ainda deveria ter a cópia antes do FINISH");

        srcCatalog.put(seriesKey, SeriesPlacement.active(DST.value(), 2L));
        MigrateResponse finish = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_FINISH,
                new MigrateControlRequest(seriesKey, migrationId));

        assertEquals(MigrateStatus.OK, finish.status());
        assertFalse(srcVolume.storage().exists(objectKey(seriesKey)), "FINISH deveria ter apagado a cópia na origem");
        assertFalse(srcRegistry.isOpen(seriesKey));

        // Idempotente: um segundo FINISH (ex.: retentativa do coordenador) continua OK.
        MigrateResponse finishAgain = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_FINISH,
                new MigrateControlRequest(seriesKey, migrationId));
        assertEquals(MigrateStatus.OK, finishAgain.status());
    }

    @Test
    void finishNaoApagaSeOLiderConfirmaAtivaNaOrigem() {
        // Mesmo achado bloqueante: um FINISH atrasado/duplicado não pode apagar a cópia local se o
        // líder ainda confirma ACTIVE(self) — o flip para o destino nunca aconteceu de verdade (ou foi
        // revertido), e apagar aqui deixaria a série sem NENHUMA cópia.
        String seriesKey = "series-finish-ativa";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);
        srcCatalog.put(seriesKey, SeriesPlacement.active(SRC.value(), 1_000L));

        MigrateResponse finish = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_FINISH,
                new MigrateControlRequest(seriesKey, migrationId));

        assertEquals(MigrateStatus.OK, finish.status());
        assertTrue(srcVolume.storage().exists(objectKey(seriesKey)),
                "não deveria apagar a cópia ainda ativa segundo o líder");
    }

    @Test
    void aposFinishAOrigemNaoRecriaASerieSozinha() {
        String seriesKey = "series-nao-recria";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        assertEquals(MigrateStatus.OK, ((MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, migrationId, DST.value()))).status());
        awaitCommittedOnSource(migrationId);
        srcCatalog.put(seriesKey, SeriesPlacement.active(DST.value(), 2L));
        assertEquals(MigrateStatus.OK, ((MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_FINISH,
                new MigrateControlRequest(seriesKey, migrationId))).status());

        // É exatamente isto que a auto-cura de StorageRequestHandler faz ao receber uma escrita
        // atrasada: pede ao registry para reabrir pela definição em cache. Depois do FINISH a origem
        // não pode mais conseguir — senão recria o arquivo, vazio, e deixa uma órfã no dono antigo.
        assertTrue(srcRegistry.reopenIfKnown(seriesKey).isEmpty(),
                "a origem não deveria conseguir reabrir a série sozinha depois do FINISH");
        assertFalse(srcVolume.storage().exists(objectKey(seriesKey)),
                "nenhuma tentativa de reabertura pode recriar a imagem no dono antigo");
    }

    @Test
    void markMigratingBloqueiaWritesDuranteATransferencia() {
        String seriesKey = "series-bloqueio";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        MigrateResponse start = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, migrationId, DST.value()));
        assertEquals(MigrateStatus.OK, start.status());

        // markMigrating (chamado dentro de handleStart) já bloqueia open() imediatamente — não precisa
        // esperar a transferência assíncrona terminar para observar o efeito.
        assertThrows(IllegalStateException.class,
                () -> srcRegistry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults()));

        awaitCommittedOnSource(migrationId);
    }

    /**
     * Achado dos MÉDIOS do Refuter: se {@code MIGRATE_ABORT} nunca chega à origem (rede, nó reiniciado
     * entre o envio e a entrega), a série ficava bloqueada em {@code markMigrating} para sempre, mesmo
     * depois de o catálogo já confirmar que ela nunca saiu daqui. Simula o estado preso diretamente (sem
     * depender de um START/transferência reais) e confirma que {@code healStuckMigrations} libera a
     * marca quando a leitura FORTE do placement confirma {@code ACTIVE(self)}.
     */
    @Test
    void healStuckMigrationsLiberaAMarcaQuandoLiderConfirmaAtivaNoSelfAposAbortPerdido() throws Exception {
        String seriesKey = "series-stuck-abort-perdido";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey), 2_000L,
                MigrationExecutor.MigratePhase.STARTED);
        srcCatalog.put(seriesKey, SeriesPlacement.active(SRC.value(), 1_000L));

        int healed = srcExecutor.healStuckMigrations(Duration.ofSeconds(1));

        assertEquals(1, healed);
        assertFalse(srcRegistry.isMigrating(seriesKey), "deveria ter liberado a marca de migrating");
        assertTrue(srcVolume.storage().exists(objectKey(seriesKey)), "não deveria apagar nada neste caso");
        // Confirma que a série volta a atender normalmente (OPEN não lança mais IllegalStateException).
        srcRegistry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
    }

    /**
     * Mesmo achado, causa espelhada: {@code MIGRATE_FINISH} nunca chega (mesma classe de perda), mas o
     * catálogo confirma que a migração completou de verdade em OUTRO nó. {@code healStuckMigrations}
     * aplica a mesma limpeza de {@code handleFinish} — só depois de confirmar isso pela via forte.
     */
    @Test
    void healStuckMigrationsAplicaFinishLocalQuandoLiderConfirmaAtivaEmOutroNoAposFinishPerdido() throws Exception {
        String seriesKey = "series-stuck-finish-perdido";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey), 2_000L,
                MigrationExecutor.MigratePhase.STARTED);
        srcCatalog.put(seriesKey, SeriesPlacement.active(DST.value(), 1_000L));

        int healed = srcExecutor.healStuckMigrations(Duration.ofSeconds(1));

        assertEquals(1, healed);
        assertFalse(srcRegistry.isMigrating(seriesKey), "clearMigrating faz parte da limpeza do FINISH");
        assertFalse(srcVolume.storage().exists(objectKey(seriesKey)), "deveria ter apagado a cópia na origem");
        assertTrue(srcRegistry.reopenIfKnown(seriesKey).isEmpty(),
                "forget não pode deixar a série reabrir sozinha");
    }

    /**
     * Achado do Refuter (r2, MÉDIO): {@code COMMITTED} do lado {@code SOURCE} é o caso real de "a
     * transferência terminou com sucesso, mas o {@code MIGRATE_FINISH} nunca chegou" — antes ficava de
     * fora de {@code healStuckMigrations} porque {@code COMMITTED} é fase terminal. Mesmo cenário do
     * teste acima (líder confirma {@code ACTIVE(self)} → libera a marca), só que partindo de uma
     * transferência que JÁ terminou, não de uma que nunca chegou a começar.
     */
    @Test
    void healStuckMigrationsLiberaAMarcaParaEstadoCommittedQuandoLiderConfirmaAtivaNoSelf() throws Exception {
        String seriesKey = "series-stuck-committed-abort-perdido";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey), 2_000L,
                MigrationExecutor.MigratePhase.COMMITTED);
        srcCatalog.put(seriesKey, SeriesPlacement.active(SRC.value(), 1_000L));

        int healed = srcExecutor.healStuckMigrations(Duration.ofSeconds(1));

        assertEquals(1, healed);
        assertFalse(srcRegistry.isMigrating(seriesKey), "deveria ter liberado a marca de migrating");
        assertTrue(srcVolume.storage().exists(objectKey(seriesKey)), "não deveria apagar nada neste caso");
    }

    /** Mesmo achado, causa espelhada: transferência COMMITTED, mas o líder confirma ACTIVE em OUTRO nó. */
    @Test
    void healStuckMigrationsAplicaFinishLocalParaEstadoCommittedQuandoLiderConfirmaAtivaEmOutroNo() throws Exception {
        String seriesKey = "series-stuck-committed-finish-perdido";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey), 2_000L,
                MigrationExecutor.MigratePhase.COMMITTED);
        srcCatalog.put(seriesKey, SeriesPlacement.active(DST.value(), 1_000L));

        int healed = srcExecutor.healStuckMigrations(Duration.ofSeconds(1));

        assertEquals(1, healed);
        assertFalse(srcRegistry.isMigrating(seriesKey), "clearMigrating faz parte da limpeza do FINISH");
        assertFalse(srcVolume.storage().exists(objectKey(seriesKey)), "deveria ter apagado a cópia na origem");
    }

    /**
     * Achado do Refuter (r3): transferência que FALHOU (chunk/commit recusado, falha de transporte —
     * ver {@link MigrationExecutor#transfer}) também deixa {@code markMigrating} preso se o {@code
     * MIGRATE_ABORT} do coordenador nunca chegar — com a revalidação do item 1 da rodada anterior (r2),
     * isso acontece mesmo SEM perda de rede: se {@code MigrationCoordinator#abort} não conseguir gravar
     * a reversão do catálogo (precondição falhou), ele não manda ABORT a lugar nenhum, de propósito. Só
     * cobre o caso {@code ACTIVE(self)} — o espelhado ({@code ACTIVE(outro)} → FINISH local) só é
     * alcançável se o destino tiver COMMITADO apesar de a origem ter marcado FAILED (ex.: falha de
     * transporte na resposta do commit, não no commit em si) e está documentado no Javadoc de {@link
     * MigrationExecutor#healStuckMigrations} em vez de replicado aqui, já que o código que resolve é o
     * MESMO (nenhum branch novo, só {@code isStuckCandidatePhase} passou a aceitar {@code FAILED}).
     */
    @Test
    void healStuckMigrationsLiberaAMarcaParaEstadoFailedQuandoLiderConfirmaAtivaNoSelf() throws Exception {
        String seriesKey = "series-stuck-failed-abort-perdido";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey), 2_000L,
                MigrationExecutor.MigratePhase.FAILED);
        srcCatalog.put(seriesKey, SeriesPlacement.active(SRC.value(), 1_000L));

        int healed = srcExecutor.healStuckMigrations(Duration.ofSeconds(1));

        assertEquals(1, healed);
        assertFalse(srcRegistry.isMigrating(seriesKey), "deveria ter liberado a marca de migrating");
        assertTrue(srcVolume.storage().exists(objectKey(seriesKey)), "não deveria apagar nada neste caso");
    }

    /**
     * Achado do Refuter (r2, MÉDIO): sem essa guarda, {@code sweepExpiredStates} podia varrer uma
     * entrada {@code COMMITTED}/{@code SOURCE} cuja série ainda está {@code registry.isMigrating} —
     * apagando o único registro que {@code healStuckMigrations} usaria para resolver o FINISH perdido
     * mais tarde, deixando a série bloqueada para sempre e sem rastro nenhum em {@code states}.
     */
    @Test
    void sweepExpiredStatesNaoRemoveEntradaCujaSerieAindaEstaMigrating() throws Exception {
        String seriesKey = "series-sweep-ainda-migrating";
        writeAndCheckpointSeries(seriesKey);
        String migrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, migrationId);

        srcRegistry.markMigrating(seriesKey);
        putStuckSourceState(srcExecutor, migrationId, seriesKey, objectKey(seriesKey),
                Duration.ofMinutes(20).toMillis(), MigrationExecutor.MigratePhase.COMMITTED);

        int removedWhileMigrating = srcExecutor.sweepExpiredStates(Duration.ofMinutes(10));
        assertEquals(0, removedWhileMigrating,
                "não deveria varrer uma entrada cuja série ainda está migrating no registry");

        srcRegistry.clearMigrating(seriesKey);
        int removedAfterClear = srcExecutor.sweepExpiredStates(Duration.ofMinutes(10));
        assertEquals(1, removedAfterClear, "depois de clearMigrating, a varredura normal volta a funcionar");
    }

    /**
     * Insere diretamente uma {@code MigrationState} de papel {@code SOURCE}, na {@code phase} dada, já
     * "velha" há {@code ageMs} — equivalente a uma migração real presa sem nunca ter recebido {@code
     * MIGRATE_ABORT}/{@code MIGRATE_FINISH}, sem depender de bloquear uma transferência de verdade em
     * trânsito.
     */
    private void putStuckSourceState(MigrationExecutor executor, String migrationId, String seriesKey,
            String storageKey, long ageMs, MigrationExecutor.MigratePhase phase) throws ReflectiveOperationException {
        Field statesField = MigrationExecutor.class.getDeclaredField("states");
        statesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, MigrationExecutor.MigrationState> states =
                (Map<String, MigrationExecutor.MigrationState>) statesField.get(executor);
        states.put(migrationId, new MigrationExecutor.MigrationState(seriesKey, migrationId,
                MigrationExecutor.Role.SOURCE, phase, 0, 1, 100L, "deadbeef",
                storageKey, null, System.currentTimeMillis() - ageMs));
    }

    @Test
    void startComMigrationIdDiferenteParaSerieJaAtivaRespondeError() {
        String seriesKey = "series-conflito";
        writeAndCheckpointSeries(seriesKey);
        String firstMigrationId = UUID.randomUUID().toString();
        publishMigration(seriesKey, firstMigrationId);
        MigrateResponse first = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, firstMigrationId, DST.value()));
        assertEquals(MigrateStatus.OK, first.status());

        MigrateResponse second = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(seriesKey, UUID.randomUUID().toString(), DST.value()));

        assertEquals(MigrateStatus.ERROR, second.status());
        awaitCommittedOnSource(firstMigrationId);
    }

    @Test
    void statusDeMigracaoDesconhecidaRespondeUnknown() {
        MigrateResponse response = status(srcExecutor, "migracao-que-nunca-existiu");
        assertEquals(MigrateStatus.UNKNOWN, response.status());
    }

    @ParameterizedTest
    @ValueSource(strings = {"series", "tenant/archive"})
    void persistedSeriesMigratesWithFreshRegistryAndCanMoveAgainWithoutOpen(String prefix) {
        String key = "device:router/iface:eth0";
        String physicalKey = prefix + "/" + key + ".ngrr";
        String definition = yaml.replace("seriesPrefix: \"series\"", "seriesPrefix: \"" + prefix + "\"");
        NgrrdHandle handle = srcRegistry.open(key, definition, Ngrrd.OpenOptions.defaults());
        handle.write("in_octets", new Sample(1_700_000_100_000L, 1000));
        handle.checkpoint();
        srcRegistry.close();
        byte[] original = srcVolume.storage().get(physicalKey).orElseThrow();
        srcExecutor.close();
        dstExecutor.close();
        srcRegistry = new SeriesHandleRegistry(srcVolume, "ngrrd", Duration.ofMinutes(15), 100, Clock.systemUTC());
        srcExecutor = new MigrationExecutor(new FakeTransport(SRC), srcRegistry, srcVolume, rpc, srcCatalog,
                SRC, prefix, 4096L, MAX_SERIES_BYTES, Clock.systemUTC());
        dstExecutor = new MigrationExecutor(new FakeTransport(DST), dstRegistry, dstVolume, rpc, dstCatalog,
                DST, prefix, 4096L, MAX_SERIES_BYTES, Clock.systemUTC());
        rpc.register(SRC, srcExecutor);
        rpc.register(DST, dstExecutor);
        assertTrue(srcRegistry.cachedYaml(key).isEmpty());
        publishMigration(key, "outbound");
        assertEquals(MigrateStatus.OK, ((MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(key, "outbound", DST.value()))).status());
        awaitCommittedOnSource("outbound");
        srcCatalog.put(key, SeriesPlacement.active(DST.value(), 2L));
        srcExecutor.handleLocal(Commands.MIGRATE_FINISH, new MigrateControlRequest(key, "outbound"));
        assertFalse(srcVolume.storage().exists(physicalKey));
        assertArrayEquals(original, dstVolume.storage().get(physicalKey).orElseThrow());
        assertTrue(dstRegistry.cachedYaml(key).isEmpty());

        SeriesPlacement returning = SeriesPlacement.migrating(SeriesPlacement.active(DST.value(), 2),
                SRC.value(), "return", 3);
        srcCatalog.put(key, returning);
        dstCatalog.put(key, returning);
        assertEquals(MigrateStatus.OK, ((MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(key, "return", SRC.value()))).status());
        long deadline = System.nanoTime() + AWAIT_TIMEOUT.toNanos();
        while (status(dstExecutor, "return").status() == MigrateStatus.PARTIAL && System.nanoTime() < deadline) {
            sleepQuietly();
        }
        assertEquals(MigrateStatus.COMMITTED, status(dstExecutor, "return").status());
        assertArrayEquals(original, srcVolume.storage().get(physicalKey).orElseThrow());
    }

    @Test
    void earlyAbortRejectsDelayedChunksEvenBeforeCatalogConverges() {
        String key = "early-abort";
        byte[] image = writeAndCheckpointSeries(key);
        publishMigration(key, "old");
        dstExecutor.handleLocal(Commands.MIGRATE_ABORT, new MigrateControlRequest(key, "old"));
        assertEquals(MigrateStatus.ERROR, chunk(key, "old", image).status());
        assertEquals(MigrateStatus.ERROR, commit(key, "old", image).status());
        assertFalse(dstVolume.storage().exists(objectKey(key)));
    }

    @Test
    void staleCommitCannotOverwriteImageFromNewerMigration() {
        String key = "stale-commit";
        byte[] old = writeAndCheckpointSeries(key);
        publishMigration(key, "old");
        assertEquals(MigrateStatus.OK, chunk(key, "old", old).status());
        srcRegistry.withHandle(key, handle -> {
            handle.write("in_octets", new Sample(1_700_000_000_000L + 30 * 300_000L, 50_000d));
            handle.checkpoint();
            return true;
        });
        byte[] newer = srcVolume.storage().get(objectKey(key)).orElseThrow();
        assertFalse(java.util.Arrays.equals(old, newer));
        publishMigration(key, "new");
        assertEquals(MigrateStatus.OK, chunk(key, "new", newer).status());
        assertEquals(MigrateStatus.COMMITTED, commit(key, "new", newer).status());
        dstCatalog.put(key, SeriesPlacement.active(DST.value(), 2L));
        assertEquals(MigrateStatus.ERROR, commit(key, "old", old).status());
        assertArrayEquals(newer, dstVolume.storage().get(objectKey(key)).orElseThrow());
    }

    @Test
    void expiredAbortStillCannotResurrectAnObsoleteMigration() {
        String key = "expired-abort";
        byte[] image = writeAndCheckpointSeries(key);
        dstExecutor.handleLocal(Commands.MIGRATE_ABORT, new MigrateControlRequest(key, "old"));
        dstExecutor.sweepExpiredStates(Duration.ofMillis(-1));
        publishMigration(key, "new");
        assertEquals(MigrateStatus.ERROR, chunk(key, "old", image).status());
        dstCatalog.put(key, SeriesPlacement.migrating(SeriesPlacement.active(SRC.value(), 0),
                "another-target", "new", 1));
        assertEquals(MigrateStatus.ERROR, chunk(key, "new", image).status());
        dstCatalog.placements.clear();
        assertEquals(MigrateStatus.ERROR, chunk(key, "new", image).status());
        assertFalse(dstVolume.storage().exists(objectKey(key)));
    }

    @Test
    void concurrentDuplicateCommitsAreIdempotent() throws Exception {
        String key = "concurrent-commit";
        byte[] image = writeAndCheckpointSeries(key);
        publishMigration(key, "move");
        assertEquals(MigrateStatus.OK, chunk(key, "move", image).status());
        var start = new CountDownLatch(1);
        var first = CompletableFuture.supplyAsync(() -> { await(start); return commit(key, "move", image); });
        var second = CompletableFuture.supplyAsync(() -> { await(start); return commit(key, "move", image); });
        start.countDown();
        assertEquals(MigrateStatus.COMMITTED, first.get(5, TimeUnit.SECONDS).status());
        assertEquals(MigrateStatus.COMMITTED, second.get(5, TimeUnit.SECONDS).status());
        assertArrayEquals(image, dstVolume.storage().get(objectKey(key)).orElseThrow());
    }

    @Test
    void sourceStopsAfterAbortWhileChunkResponseIsInFlight() throws Exception {
        String key = "cancel-transfer";
        writeAndCheckpointSeries(key);
        publishMigration(key, "move");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        rpc.beforeChunk = () -> { entered.countDown(); await(release); };
        rpc.afterChunk = returned::countDown;
        try {
            srcExecutor.handleLocal(Commands.MIGRATE_START, new MigrateStartRequest(key, "move", DST.value()));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            srcExecutor.handleLocal(Commands.MIGRATE_ABORT, new MigrateControlRequest(key, "move"));
            release.countDown();
            assertTrue(returned.await(5, TimeUnit.SECONDS));
            // Wait for the transfer task to leave the pool, so a late phase update is observable.
            Field poolField = MigrationExecutor.class.getDeclaredField("transferExecutor");
            poolField.setAccessible(true);
            var pool = (java.util.concurrent.ExecutorService) poolField.get(srcExecutor);
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(1, rpc.chunkCallCount());
            assertEquals(MigrateStatus.ERROR, status(srcExecutor, "move").status());
            assertFalse(srcRegistry.isMigrating(key));
            assertFalse(dstVolume.storage().exists(objectKey(key)));
        } finally {
            release.countDown();
        }
    }

    @Test
    void rejectsBeforeChunksWhenActualImageExceedsCapacityAndPreservesSource() {
        String key = "capacity-series";
        String id = "capacity-move";
        byte[] original = writeAndCheckpointSeries(key);
        publishMigration(key, id);
        dstVolume.storage().configureCapacity(4096, () -> Long.MAX_VALUE);
        var response = (MigrateResponse) srcExecutor.handleLocal(Commands.MIGRATE_START,
                new MigrateStartRequest(key, id, DST.value()));
        assertEquals(MigrateStatus.ERROR, response.status());
        assertArrayEquals(original, srcVolume.storage().get(objectKey(key)).orElseThrow());
        assertFalse(dstVolume.storage().exists(objectKey(key)));
        assertEquals(0, dstVolume.storage().reservedBytes());
        assertFalse(srcRegistry.isMigrating(key));
    }

    @Test
    void destinationEnteringDrainAfterPreparationRejectsCommitAndReleasesReservation() {
        String key = "draining-target";
        byte[] original = writeAndCheckpointSeries(key);
        publishMigration(key, "drain-move");
        assertEquals(MigrateStatus.OK, chunk(key, "drain-move", original).status());
        assertTrue(dstVolume.storage().reservedBytes() > 0);
        dstCatalog.status = new StorageNodeStatus(DST.value(), NodeState.DRAINING, 0, 0, 0, 0);
        assertEquals(MigrateStatus.ERROR, commit(key, "drain-move", original).status());
        assertEquals(0, dstVolume.storage().reservedBytes());
        assertFalse(dstVolume.storage().exists(objectKey(key)));
        assertArrayEquals(original, srcVolume.storage().get(objectKey(key)).orElseThrow());
    }

    @Test
    void reservationBlocksOtherAllocationsAndAbortReleasesIt() {
        String key = "reserved";
        publishMigration(key, "reservation");
        dstVolume.storage().configureCapacity(9000, () -> Long.MAX_VALUE);
        assertEquals(MigrateStatus.OK, prepare(key, "reservation", 8192).status());
        assertEquals(MigrateStatus.OK, prepare(key, "reservation", 8192).status());
        assertEquals(8192, dstVolume.storage().reservedBytes());
        assertThrows(dev.nishisan.utils.oss.storage.blob.BlobCapacityException.class,
                () -> dstVolume.storage().put("another", new byte[4096]));
        dstExecutor.handleLocal(Commands.MIGRATE_ABORT, new MigrateControlRequest(key, "reservation"));
        assertEquals(0, dstVolume.storage().reservedBytes());
        dstVolume.storage().put("another", new byte[4096]);
    }

    @Test
    void chunksRequirePreparationAndOversizedChunkReleasesReservation() {
        String key = "prepared";
        publishMigration(key, "prep");
        var chunk = new MigrateChunkRequest(key, "prep", 0, 1, new byte[8192]);
        assertEquals(MigrateStatus.ERROR,
                ((MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_CHUNK, chunk)).status());
        assertEquals(MigrateStatus.OK, prepare(key, "prep", 4096).status());
        assertEquals(MigrateStatus.ERROR,
                ((MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_CHUNK, chunk)).status());
        assertEquals(0, dstVolume.storage().reservedBytes());
    }

    @Test
    void expiryAndRestartRequireANewReservation() throws InterruptedException {
        String key = "expire";
        publishMigration(key, "expired");
        assertEquals(MigrateStatus.OK, prepare(key, "expired", 4096).status());
        Thread.sleep(5);
        dstExecutor.expireReservations(Duration.ZERO);
        assertEquals(0, dstVolume.storage().reservedBytes());
        assertEquals(MigrateStatus.ERROR, prepare(key, "expired", 4096).status());
        publishMigration(key, "restart");
        assertEquals(MigrateStatus.OK, prepare(key, "restart", 4096).status());
        dstExecutor.close();
        assertEquals(0, dstVolume.storage().reservedBytes());
        dstExecutor = newExecutor(DST, dstRegistry, dstVolume, dstCatalog, 4096);
        rpc.register(DST, dstExecutor);
        var chunk = new MigrateChunkRequest(key, "restart", 0, 1, new byte[4096]);
        assertEquals(MigrateStatus.ERROR, ((MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_CHUNK, chunk)).status());
        assertEquals(MigrateStatus.OK, prepare(key, "restart", 4096).status());
    }

    private MigrateResponse prepare(String key, String id, long bytes) {
        return (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_PREPARE,
                new MigratePrepareRequest(key, id, objectKey(key), bytes));
    }

    private MigrateResponse chunk(String key, String id, byte[] bytes) {
        MigrateResponse prepared = prepare(key, id, bytes.length);
        if (prepared.status() != MigrateStatus.OK) { return prepared; }
        return (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_CHUNK,
                new MigrateChunkRequest(key, id, 0, 1, bytes));
    }

    private MigrateResponse commit(String key, String id, byte[] bytes) {
        return (MigrateResponse) dstExecutor.handleLocal(Commands.MIGRATE_COMMIT,
                new MigrateCommitRequest(key, id, sha256Hex(bytes), bytes.length, objectKey(key)));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "latch não liberado");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void publishMigration(String seriesKey, String migrationId) {
        SeriesPlacement placement = SeriesPlacement.migrating(SeriesPlacement.active(SRC.value(), 0L),
                DST.value(), migrationId, 1L);
        srcCatalog.put(seriesKey, placement);
        dstCatalog.put(seriesKey, placement);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * {@link CatalogView} fake, um por nó: {@code placementStrong} devolve exatamente o que o teste
     * colocar via {@link #put}, sem round-trip nenhum — é o que permite simular o líder confirmando (ou
     * não) {@code ACTIVE(self)} nas guardas de {@code handleAbort}/{@code handleFinish}.
     */
    private static final class FakeCatalogView implements CatalogView {
        private final Map<String, SeriesPlacement> placements = new ConcurrentHashMap<>();
        private StorageNodeStatus status;

        void put(String seriesKey, SeriesPlacement placement) {
            placements.put(seriesKey, placement);
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.ofNullable(status);
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            // Não usado por MigrationExecutor — sem estado de nó a manter neste fake.
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            placements.put(seriesKey, placement);
        }
    }

    /** {@link ClusterRpc} fake que despacha para o {@link MigrationExecutor} registrado do alvo, sem rede. */
    private static final class RoutingClusterRpc implements ClusterRpc {
        private final Map<NodeId, MigrationExecutor> executors = new ConcurrentHashMap<>();
        private final LongAdder chunkCalls = new LongAdder();
        private Runnable beforeChunk = () -> { };
        private Runnable afterChunk = () -> { };

        void register(NodeId id, MigrationExecutor executor) {
            executors.put(id, executor);
        }

        int chunkCallCount() {
            return chunkCalls.intValue();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            if (Commands.MIGRATE_CHUNK.equals(command)) {
                chunkCalls.increment();
                beforeChunk.run();
            }
            MigrationExecutor executor = executors.get(target);
            if (executor == null) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "nó desconhecido: " + target);
            }
            R result = (R) executor.handleLocal(command, body);
            if (Commands.MIGRATE_CHUNK.equals(command)) {
                afterChunk.run();
            }
            return result;
        }

        @Override
        public NodeId localId() {
            return NodeId.of("test-rpc");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
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
            return true;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return true;
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() {
        }
    }
}
