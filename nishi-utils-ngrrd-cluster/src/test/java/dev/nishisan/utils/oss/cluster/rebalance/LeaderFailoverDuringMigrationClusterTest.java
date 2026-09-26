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

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.NgrrdClusterTestHarness;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Seção 7 da spec do M3: queda do líder no meio de uma migração — o próximo líder resolve via
 * {@link MigrationCoordinator#onLeaderChanged}/{@code resumeInFlight()}, tanto no caso
 * {@code COMMITTED} (completa) quanto no caso ainda não iniciado de verdade no destino (aborta).
 *
 * <p>Vive no pacote {@code rebalance} (não no pacote raiz {@code cluster}, como os demais testes de
 * cluster) porque precisa referenciar {@link MigrationCoordinator.MigrationHooks} — público apenas
 * para essa composição entre pacotes (ver Javadoc da própria interface), não parte da API do
 * cliente.</p>
 */
@Timeout(value = 400, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LeaderFailoverDuringMigrationClusterTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(150);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void quedaDoLiderAposCommittedNoDestinoNovoLiderCompletaAMigracao(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        BlockOnceHook hook = new BlockOnceHook(BlockOnceHook.Point.BEFORE_COMPLETE);

        harness = NgrrdClusterTestHarness.start(base, 3, builder -> { }, index -> hook);
        harness.awaitLeader();
        harness.awaitNodeStatuses(3);

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(3))
                .retryTimeout(Duration.ofSeconds(20))
                .closeTimeout(Duration.ofSeconds(10)));

        String seriesKey = openWriteAndCheckpointOnNonLeaderOwner(client, yaml);
        NgrrdStorageNode leaderNode = harness.leaderNode();
        SeriesPlacement placementBefore = harness.nodes().get(0).catalog().placementStrong(seriesKey).orElseThrow();
        String src = placementBefore.ownerNodeId();
        String dst = pickDestination(leaderNode.nodeId(), src);
        byte[] originalImage = imageAt(src, seriesKey).orElseThrow();
        String originalSha = sha256Hex(originalImage);
        harness.awaitCatalogReplicaCaughtUp(dst);

        CompletableFuture<MigrationCoordinator.MigrationResult> migrationFuture =
                leaderNode.migrationCoordinator().migrate(seriesKey, src, dst);

        assertTrue(hook.blocked.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                "coordenador deveria ter chegado a beforeComplete (destino já confirmou COMMITTED)"
                        + outcomeSoFar(migrationFuture));

        // Deixa a migração se pendurada no líder atual (bloqueada) e derruba exatamente esse nó.
        String deadLeaderId = leaderNode.nodeId();
        leaderNode.close();

        awaitTrue("novo líder eleito entre os 2 nós sobreviventes", () ->
                survivingNodes(deadLeaderId).stream().anyMatch(NgrrdStorageNode::isLeader));

        awaitTrue("catálogo flipado para ACTIVE(" + dst + ") após resumeInFlight do novo líder", () -> {
            Optional<SeriesPlacement> current = placementStrongOrEmpty(anySurvivor(deadLeaderId), seriesKey);
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(dst);
        }, () -> catalogDiagnostics(seriesKey));
        awaitTrue("imagem apagada na origem " + src, () -> imageAt(src, seriesKey).isEmpty());

        byte[] imageAtDst = imageAt(dst, seriesKey).orElseThrow(() ->
                new AssertionError("imagem deveria existir no destino " + dst));
        assertEquals(originalSha, sha256Hex(imageAtDst), "SHA-256 deveria sobreviver ao failover");

        client.close();
        // A migração original nunca resolve (o líder que a executava morreu com o hook bloqueado) —
        // não é uma falha do teste, é exatamente o cenário: cancela para não vazar a espera.
        migrationFuture.cancel(true);
    }

    @Test
    void quedaDoLiderAntesDoStartChegarAoDestinoNovoLiderAbortaEDevolveParaOSrc(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        BlockOnceHook hook = new BlockOnceHook(BlockOnceHook.Point.BEFORE_START);

        harness = NgrrdClusterTestHarness.start(base, 3, builder -> { }, index -> hook);
        harness.awaitLeader();
        harness.awaitNodeStatuses(3);

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(3))
                .retryTimeout(Duration.ofSeconds(20))
                .closeTimeout(Duration.ofSeconds(10)));

        String seriesKey = openWriteAndCheckpointOnNonLeaderOwner(client, yaml);
        NgrrdStorageNode leaderNode = harness.leaderNode();
        SeriesPlacement placementBefore = harness.nodes().get(0).catalog().placementStrong(seriesKey).orElseThrow();
        String src = placementBefore.ownerNodeId();
        String dst = pickDestination(leaderNode.nodeId(), src);
        byte[] originalImage = imageAt(src, seriesKey).orElseThrow();
        String originalSha = sha256Hex(originalImage);
        harness.awaitCatalogReplicaCaughtUp(dst);

        CompletableFuture<MigrationCoordinator.MigrationResult> migrationFuture =
                leaderNode.migrationCoordinator().migrate(seriesKey, src, dst);

        assertTrue(hook.blocked.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                "coordenador deveria ter chegado a beforeStart (catálogo já MIGRATING, START ainda não enviado)"
                        + outcomeSoFar(migrationFuture));
        awaitTrue("catálogo já reflete MIGRATING antes da queda", () -> {
            Optional<SeriesPlacement> current = placementStrongOrEmpty(anySurvivor(leaderNode.nodeId()), seriesKey);
            return current.isEmpty() || current.get().state() == PlacementState.MIGRATING;
        });

        String deadLeaderId = leaderNode.nodeId();
        leaderNode.close();

        awaitTrue("novo líder eleito entre os 2 nós sobreviventes", () ->
                survivingNodes(deadLeaderId).stream().anyMatch(NgrrdStorageNode::isLeader));

        awaitTrue("catálogo revertido para ACTIVE(" + src + ") após o abort do novo líder", () -> {
            Optional<SeriesPlacement> current = placementStrongOrEmpty(anySurvivor(deadLeaderId), seriesKey);
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(src);
        }, () -> catalogDiagnostics(seriesKey));

        byte[] imageAfter = imageAt(src, seriesKey).orElseThrow(() ->
                new AssertionError("a origem deveria continuar com a imagem íntegra"));
        assertEquals(originalSha, sha256Hex(imageAfter), "nada deveria ter sido transferido; SHA idêntico ao original");

        client.close();
        migrationFuture.cancel(true);
    }

    /** Abre, escreve e faz checkpoint de uma série cujo dono NÃO é o líder atual (retenta com outra série se cair no líder). */
    private String openWriteAndCheckpointOnNonLeaderOwner(NgrrdClusterClient client, String yaml)
            throws InterruptedException {
        String leaderId = harness.leaderNode().nodeId();
        for (int i = 0; i < 10; i++) {
            Map<String, String> tags = Map.of("deviceId", "failover" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            handle.write("in_octets", new Sample(1_700_000_000_000L, 1_000d));
            handle.write("out_octets", new Sample(1_700_000_000_000L, 500d));
            handle.checkpoint();
            harness.awaitPlacements(harness.nodes().get(0).catalog().placementsLocal().size());
            SeriesPlacement placement =
                    harness.nodes().get(0).catalog().placementStrong(handle.seriesKey()).orElseThrow();
            if (!placement.ownerNodeId().equals(leaderId)) {
                return handle.seriesKey();
            }
        }
        throw new AssertionError("não foi possível colocar uma série fora do nó líder em 10 tentativas");
    }

    /**
     * Desfecho já conhecido da migração, para a mensagem de falha: se o hook não foi alcançado porque
     * a migração terminou antes (ex.: {@code SKIPPED} por liderança perdida), é esse motivo que
     * explica a falha — sem ele a mensagem seria só um {@code false} sem causa.
     */
    private static String outcomeSoFar(CompletableFuture<MigrationCoordinator.MigrationResult> future) {
        MigrationCoordinator.MigrationResult result = future.getNow(null);
        return result == null
                ? " — migração ainda em andamento (hook nunca chamado)"
                : " — migração já terminou como " + result.outcome() + ": " + result.reason();
    }

    private String pickDestination(String leaderId, String src) {
        return harness.nodes().stream()
                .map(NgrrdStorageNode::nodeId)
                .filter(id -> !id.equals(leaderId) && !id.equals(src))
                .findFirst()
                .orElseThrow(() -> new AssertionError("nenhum terceiro nó disponível como destino"));
    }

    private List<NgrrdStorageNode> survivingNodes(String deadNodeId) {
        return harness.nodes().stream().filter(n -> !n.nodeId().equals(deadNodeId)).toList();
    }

    private NgrrdStorageNode anySurvivor(String deadNodeId) {
        return survivingNodes(deadNodeId).get(0);
    }

    /**
     * Como {@code node.catalog().placementStrong(seriesKey)}, mas absorve o "sem líder" transitório da
     * própria troca de liderança que estes testes provocam de propósito: {@code
     * DistributedMap.invokeLeader} (core) lança {@link IllegalStateException} quando esgota as
     * tentativas sem achar um líder — bem no instante em que o líder antigo cai e o novo ainda não se
     * afirmou, exatamente a janela que os {@code awaitTrue} abaixo estão esperando atravessar. Sem
     * isto, a exceção escapava o predicado inteiro (não só a rodada de poll), derrubando o {@code
     * awaitTrue} com um erro em vez de deixar a próxima rodada tentar de novo. {@link Optional#empty()}
     * é indistinguível de "catálogo ainda não convergiu" para quem chama — exatamente o que os
     * predicados já sabem esperar.
     */
    private static Optional<SeriesPlacement> placementStrongOrEmpty(NgrrdStorageNode node, String seriesKey) {
        long start = System.currentTimeMillis();
        try {
            return node.catalog().placementStrong(seriesKey);
        } catch (IllegalStateException | NgrrdClusterException e) {
            lastStrongReadFailure = String.format("%s em %s após %d ms: %s", e.getClass().getSimpleName(),
                    node.nodeId(), System.currentTimeMillis() - start, e.getMessage());
            return Optional.empty();
        } finally {
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > 5_000L) {
                slowStrongReads.add(node.nodeId() + ": " + elapsed + " ms");
            }
        }
    }

    /** Diagnóstico da leitura forte para a mensagem de falha (última exceção e leituras lentas). */
    private static volatile String lastStrongReadFailure = "nenhuma";
    private static final List<String> slowStrongReads = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Placement na cópia LOCAL (eventual) de cada nó vivo + diagnóstico da leitura forte. */
    private String catalogDiagnostics(String seriesKey) {
        StringBuilder sb = new StringBuilder(" — visão local por nó: ");
        for (NgrrdStorageNode node : harness.nodes()) {
            try {
                sb.append('[').append(node.nodeId()).append(" leader=").append(node.isLeader())
                        .append(" local=").append(node.catalog().placementsLocal().get(seriesKey)).append("] ");
            } catch (RuntimeException e) {
                sb.append('[').append(node.nodeId()).append(" indisponível: ").append(e.getMessage()).append("] ");
            }
        }
        sb.append("; última falha de leitura forte: ").append(lastStrongReadFailure)
                .append("; leituras fortes lentas (>5 s): ").append(slowStrongReads);
        return sb.toString();
    }

    /**
     * Imagem de {@code seriesKey} no volume do nó {@code nodeId} — só chamado com nós VIVOS (a origem,
     * o destino e o "sobrevivente" usado para checar o catálogo nunca são o líder derrubado, por
     * construção dos testes: {@link #pickDestination} exclui o líder, e {@code src} vem do placement
     * anterior à queda).
     */
    private Optional<byte[]> imageAt(String nodeId, String seriesKey) {
        return harness.nodes().stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("nó desconhecido: " + nodeId))
                .volume().storage().get(objectKey(seriesKey));
    }

    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
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

    private static void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        awaitTrue(description, condition, () -> "");
    }

    private static void awaitTrue(String description, BooleanSupplier condition, Supplier<String> diagnostics)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description + diagnostics.get());
        }
    }

    /**
     * Bloqueia a PRIMEIRA chamada ao ponto de interceptação escolhido (em qualquer nó — os 3 nós
     * compartilham a mesma instância) até ser interrompido pelo fechamento do nó; chamadas seguintes
     * (do novo líder, ao resolver via {@code resumeInFlight}) passam direto.
     */
    private static final class BlockOnceHook implements MigrationCoordinator.MigrationHooks {
        enum Point { BEFORE_START, BEFORE_COMPLETE }

        private final Point point;
        private final AtomicBoolean triggered = new AtomicBoolean(false);
        final CountDownLatch blocked = new CountDownLatch(1);

        BlockOnceHook(Point point) {
            this.point = point;
        }

        @Override
        public void beforeStart(String seriesKey, String migrationId) {
            if (point == Point.BEFORE_START && triggered.compareAndSet(false, true)) {
                blockForever();
            }
        }

        @Override
        public void beforeComplete(String migrationId) {
            if (point == Point.BEFORE_COMPLETE && triggered.compareAndSet(false, true)) {
                blockForever();
            }
        }

        private void blockForever() {
            blocked.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
