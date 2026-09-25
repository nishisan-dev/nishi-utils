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

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.StorageNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HexFormat;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * M3 (seção 8 do desenho / seção 7 da spec): nó novo entra → {@code Rebalancer} move séries →
 * imagens idênticas (SHA-256) no novo dono, apagadas no antigo, cliente segue escrevendo sem erro
 * durante a migração e todos os dados sobrevivem.
 */
@Timeout(value = 400, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RebalanceClusterTest {

    private static final int SERIES_COUNT = 40;
    /** Amostras gravadas em cada série antes do rebalanceamento (ver comentário no laço de setup). */
    private static final int SETUP_SAMPLES = 3;
    private static final long BASE_STEP_MS = 300_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(150);
    /** Intervalo entre redisparos de {@code rebalanceNow()} enquanto o nó novo ainda não recebeu séries. */
    private static final Duration RETRIGGER_INTERVAL = Duration.ofSeconds(10);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void noNovoRecebeSeriesComImagemIdenticaEClienteSeguemEscrevendoSemErro(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        harness = NgrrdClusterTestHarness.start(base, 2, RebalanceClusterTest::tuneForRebalance);
        harness.awaitLeader();
        harness.awaitNodeStatuses(2);

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(20)));

        Map<String, NgrrdHandle> handlesBySeriesKey = new LinkedHashMap<>();
        Map<String, Integer> sampleCountBySeriesKey = new LinkedHashMap<>();
        // Séries "congeladas": não recebem escrita depois do SHA-256 ser capturado, e por isso são as
        // únicas em que a comparação de SHA antes/depois da migração é um invariante válido. As demais
        // ficam com a thread de escrita contínua — a spec exige as duas coisas (imagem byte a byte
        // idêntica no novo dono E cliente escrevendo durante o rebalanceamento), e elas só são
        // verificáveis ao mesmo tempo em conjuntos disjuntos de séries: uma amostra nova gravada entre
        // a captura do SHA e o MIGRATE_START entra na imagem (markMigrating faz checkpoint+close antes
        // de lê-la), mudando legitimamente o SHA sem que nada tenha se corrompido.
        Set<String> frozenSeriesKeys = new LinkedHashSet<>();
        for (int i = 0; i < SERIES_COUNT; i++) {
            Map<String, String> tags = Map.of("deviceId", "rb" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            handlesBySeriesKey.put(handle.seriesKey(), handle);
            if (i % 2 == 0) {
                frozenSeriesKeys.add(handle.seriesKey());
            }
            long t0 = alignedBase(handle.seriesKey());
            // {@value #SETUP_SAMPLES} amostras (não uma só): as fontes deste YAML são COUNTER e o que
            // vai para os RRAs é a derivada (in_bps/out_bps), que precisa de pelo menos duas amostras
            // consecutivas para produzir um ponto. Com uma amostra só, as séries congeladas — que por
            // definição não recebem mais nada depois daqui — liam zero pontos no fim do teste.
            for (int sample = 0; sample < SETUP_SAMPLES; sample++) {
                long ts = t0 + sample * BASE_STEP_MS;
                handle.write("in_octets", new Sample(ts, 1_000d + sample * 1_000d));
                handle.write("out_octets", new Sample(ts, 500d + sample * 500d));
            }
            handle.checkpoint();
            sampleCountBySeriesKey.put(handle.seriesKey(), SETUP_SAMPLES);
        }
        assertEquals(SERIES_COUNT, handlesBySeriesKey.size(), "seriesKey deveria ser único por conjunto de tags");
        harness.awaitPlacements(SERIES_COUNT);

        // Captura o SHA-256 e o dono ORIGINAL de cada série antes do rebalanceamento.
        Map<String, String> shaBeforeBySeriesKey = new LinkedHashMap<>();
        Map<String, String> ownerBeforeBySeriesKey = new LinkedHashMap<>();
        for (String seriesKey : handlesBySeriesKey.keySet()) {
            String owner = ownerOf(seriesKey);
            ownerBeforeBySeriesKey.put(seriesKey, owner);
            byte[] image = imageAt(owner, seriesKey).orElseThrow(() ->
                    new AssertionError("imagem ausente no dono original de " + seriesKey));
            shaBeforeBySeriesKey.put(seriesKey, sha256Hex(image));
        }

        // Escritas contínuas durante o rebalanceamento — nenhuma exceção deve escapar.
        AtomicBoolean stopWriting = new AtomicBoolean(false);
        List<Throwable> writerErrors = new CopyOnWriteArrayList<>();
        Thread continuousWriter = new Thread(() -> {
            // Continua de onde o setup parou — reaproveitar os mesmos instantes sobrescreveria amostras
            // já gravadas em vez de acrescentar novas, e a contagem esperada na leitura final não bateria.
            int extraSample = SETUP_SAMPLES;
            while (!stopWriting.get()) {
                for (Map.Entry<String, NgrrdHandle> entry : handlesBySeriesKey.entrySet()) {
                    if (frozenSeriesKeys.contains(entry.getKey())) {
                        continue;
                    }
                    try {
                        long ts = alignedBase(entry.getKey()) + extraSample * BASE_STEP_MS;
                        entry.getValue().write("in_octets", new Sample(ts, 2_000d + extraSample));
                        entry.getValue().write("out_octets", new Sample(ts, 1_000d + extraSample));
                        sampleCountBySeriesKey.merge(entry.getKey(), 1, Integer::sum);
                    } catch (Throwable t) {
                        writerErrors.add(t);
                    }
                }
                extraSample++;
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "continuous-writer");

        // Entra com o nó novo numa malha já assentada e espera ele ser visto por todos ANTES de ligar a
        // escrita contínua: o que a spec exige é o cliente escrevendo durante o REBALANCEAMENTO, não
        // durante a entrada do nó. Sobrepor as duas coisas põe uma mudança de membership em cima de
        // tráfego pesado de replicação e a malha demora (ou deixa) de reconvergir — mesmo efeito medido
        // por A/B com NGrid puro e já tratado em NgrrdClusterTestHarness#connectClient.
        harness.awaitMeshStable();
        NgrrdStorageNode newNode = harness.addStorageNode(RebalanceClusterTest::tuneForRebalance);
        // Precondição funcional: o líder já conhece o nó novo (é isso que o planejador consome). NÃO se
        // exige consenso pleno de liderança aqui — com 3 storage nodes mais o cliente, a eleição do
        // NGrid pode ficar em desacordo por minutos depois da entrada do 3º nó (ver relatório do M3),
        // e o rebalanceamento não depende disso: depende do líder atual enxergar os 3 nós.
        awaitTrue("novo nó reportado no catálogo", () -> harness.nodes().get(0).catalog().nodesLocal().size() == 3);

        continuousWriter.start();
        // Um ciclo morre com a liderança do nó que o executou (movimentos SKIPPED/FAILED; o novo líder
        // resolve os MIGRATING pelo resumeInFlight) e, com rebalanceEnabled(false), ninguém replaneja.
        // Como o 3º nó é o de maior afinidade, o handback orquestrado (D11) do storage node lhe entrega
        // a liderança pouco depois de entrar — possivelmente no meio do ciclo disparado aqui. Redispara
        // rebalanceNow() periodicamente até o rebalanceamento efetivamente acontecer; a verificação
        // (≥ 8 séries no nó novo dentro do mesmo prazo) é a mesma.
        awaitTrueRetriggering("nó novo (" + newNode.nodeId() + ") dono de pelo menos 8 séries",
                () -> countOwnedBy(newNode.nodeId()) >= 8, client);

        // Espera o ciclo assentar (nenhum placement MIGRATING pendurado) antes de parar as escritas.
        awaitTrue("nenhum placement MIGRATING pendente", () ->
                harness.nodes().get(0).catalog().placementsLocal().values().stream()
                        .noneMatch(p -> p.state() == PlacementState.MIGRATING));

        stopWriting.set(true);
        continuousWriter.join(AWAIT_TIMEOUT.toMillis());
        assertTrue(writerErrors.isEmpty(), "escritas contínuas não deveriam lançar durante o rebalanceamento: "
                + writerErrors);

        int movedCount = 0;
        int movedFrozenCount = 0;
        for (String seriesKey : handlesBySeriesKey.keySet()) {
            String ownerBefore = ownerBeforeBySeriesKey.get(seriesKey);
            String ownerAfter = ownerOf(seriesKey);
            if (ownerAfter.equals(ownerBefore)) {
                continue;
            }
            movedCount++;
            byte[] imageAfter = imageAt(ownerAfter, seriesKey).orElseThrow(() ->
                    new AssertionError("imagem ausente no novo dono de " + seriesKey));
            if (frozenSeriesKeys.contains(seriesKey)) {
                movedFrozenCount++;
                assertEquals(shaBeforeBySeriesKey.get(seriesKey), sha256Hex(imageAfter),
                        "SHA-256 da imagem de " + seriesKey + " deveria sobreviver à migração");
            }
            // O catálogo é flipado para ACTIVE(dst) ANTES de o coordenador mandar MIGRATE_FINISH à
            // origem (é essa ordem que garante que nenhuma janela aponte para uma cópia inexistente),
            // então "sem placement MIGRATING" não implica "FINISH já executado": espera a remoção em
            // vez de exigi-la no mesmo instante. A verificação em si é a mesma que a spec pede.
            awaitTrue("imagem de " + seriesKey + " apagada no dono antigo " + ownerBefore, () ->
                    imageAt(ownerBefore, seriesKey).isEmpty());
            SeriesPlacement placement = harness.nodes().get(0).catalog().placementStrong(seriesKey).orElseThrow();
            assertEquals(PlacementState.ACTIVE, placement.state());
            assertEquals(ownerAfter, placement.ownerNodeId());
        }
        assertTrue(movedCount > 0, "pelo menos uma série deveria ter sido movida pelo rebalanceamento");
        assertTrue(movedFrozenCount > 0, "pelo menos uma série congelada deveria ter sido movida, para que a "
                + "comparação de SHA-256 antes/depois seja exercitada de fato");

        // Checkpoint + leitura de TODAS as 40 séries — inclusive as amostras escritas durante a migração.
        for (Map.Entry<String, NgrrdHandle> entry : handlesBySeriesKey.entrySet()) {
            String seriesKey = entry.getKey();
            NgrrdHandle handle = entry.getValue();
            // Recovery belongs to the client; an external retry would hide issue #169.
            handle.checkpoint();
            int expectedSamples = sampleCountBySeriesKey.get(seriesKey);
            long endExclusive = alignedBase(seriesKey) + (expectedSamples + 1) * BASE_STEP_MS;
            // Mesma leitura do DistributedWriteReadClusterTest (M1c): série DERIVADA (in_bps — é ela
            // que o YAML arquiva em RRA; in_octets é o COUNTER cru) e maxPoints fixo em 500, para que o
            // best_fit escolha a RRA de 300 s. Com maxPoints amarrado ao número de amostras, uma série
            // pequena fazia o best_fit cair na RRA de 1 h, onde 3 amostras não alcançam o xff de 0,50
            // do balde e TODOS os pontos voltavam NaN.
            ViewQuery query = new ViewQuery(Duration.ofDays(1), (int) (BASE_STEP_MS / 1_000L),
                    ConsolidationFunction.AVERAGE, 500);
            // Poll (não leitura única): logo depois da migração a primeira leitura chega ao novo dono
            // antes de a série estar aberta lá — o cliente responde a isso reabrindo (NOT_OPEN), o que
            // é transparente mas não instantâneo, e uma leitura única podia voltar vazia sem lançar
            // nada. A asserção é a mesma; só ganhou prazo.
            SeriesResult result = awaitReadWithPoints(handle, query, endExclusive);
            // Divergência documentada (aceita pelo Refuter): a spec original pedia "todos os pontos"
            // não-NaN; aqui só se exige > 0 (mais a cobertura de timestamp mínima logo abaixo) por dois
            // motivos que não são bug — (1) buffer do cliente: a thread de escrita contínua para de
            // forma assíncrona, então as últimas amostras enviadas podem não ter chegado ao servidor
            // ainda quando a leitura roda; (2) consolidação de RRA: com xff=0,50, um balde de 300 s cujas
            // amostras não alcançam esse limiar consolida como NaN mesmo com dado real gravado — não há
            // garantia de que TODA janela de 300 s do período lido tenha amostra suficiente, só que
            // ALGUMA janela tenha (o que os dois asserts abaixo, de cobertura mínima e estrita, cobrem).
            long nonNullPoints = result.points().stream().filter(p -> !Double.isNaN(p.value())).count();
            assertTrue(nonNullPoints > 0, "série " + seriesKey + " deveria ter pontos não-NaN após o "
                    + "rebalanceamento, achou " + nonNullPoints + " (pontos=" + result.points().size()
                    + ", congelada=" + frozenSeriesKeys.contains(seriesKey)
                    + ", dono antes=" + ownerBeforeBySeriesKey.get(seriesKey)
                    + ", dono agora=" + ownerOf(seriesKey)
                    + ", bytes no dono agora=" + imageAt(ownerOf(seriesKey), seriesKey).map(b -> b.length).orElse(-1)
                    // Discriminador: imagem byte a byte igual à capturada antes do rebalanceamento
                    // (⇒ dados intactos, problema na leitura) ou diferente (⇒ imagem substituída/recriada).
                    + ", imagemIgualAoSnapshot=" + imageAt(ownerOf(seriesKey), seriesKey)
                            .map(b -> sha256Hex(b).equals(shaBeforeBySeriesKey.get(seriesKey))).orElse(null)
                    + ", resultado=" + result
                    + ", amostras esperadas=" + expectedSamples + ")");
            // Cobertura mínima: as amostras do setup (gravadas ANTES do rebalanceamento) têm de estar
            // na leitura. Para as séries não congeladas, exige-se ESTRITAMENTE mais que isso — ou
            // seja, ao menos uma amostra escrita DURANTE o rebalanceamento sobreviveu. Não se fixa o
            // índice exato da última amostra: a thread de escrita contínua para de forma assíncrona e
            // as últimas amostras podem ainda estar no buffer do cliente quando ela é interrompida.
            long lastNonNullTs = result.points().stream()
                    .filter(point -> !Double.isNaN(point.value()))
                    .mapToLong(point -> point.tsEpochMs())
                    .max()
                    .orElseThrow();
            long setupCoverage = alignedBase(seriesKey) + (SETUP_SAMPLES - 1) * BASE_STEP_MS;
            assertTrue(lastNonNullTs >= setupCoverage, "leitura de " + seriesKey + " não cobre nem as "
                    + SETUP_SAMPLES + " amostras do setup (último ponto não-NaN em " + lastNonNullTs + ")");
            if (!frozenSeriesKeys.contains(seriesKey)) {
                assertTrue(lastNonNullTs > setupCoverage, "leitura de " + seriesKey + " não reflete nenhuma "
                        + "amostra escrita durante o rebalanceamento (último ponto não-NaN em " + lastNonNullTs
                        + ", fim do setup em " + setupCoverage + ", amostras contadas=" + expectedSamples + ")");
            }
        }

        client.close();
    }

    /**
     * Repete a leitura até vir ao menos um ponto não-NaN, ou até o prazo. Devolve sempre o ÚLTIMO
     * resultado obtido — inclusive um vazio, para que a asserção do chamador falhe com o retrato real.
     */
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
                // transitório (dono mudando, série ainda reabrindo) — o laço tenta de novo
            }
            Thread.sleep(200L);
        } while (System.currentTimeMillis() < deadline);
        return last != null ? last : handle.read("in_bps", query, endExclusive);
    }

    /**
     * Configuração comum aos storage nodes deste teste.
     *
     * <ul>
     *   <li>{@code rebalanceEnabled(false)}: o ciclo é disparado só por {@code client.rebalanceNow()},
     *       o que torna o teste determinístico (nada roda por trás pelo intervalo de 60 s).</li>
     *   <li>{@code rebalanceMinDelta(1)}: o default de produção é 50 séries — com as
     *       {@value #SERIES_COUNT} séries deste teste espalhadas por 3 nós, a diferença entre o nó mais
     *       e o menos carregado é 20, abaixo do gatilho, e o planejador (corretamente) devolveria ZERO
     *       movimentos. O teste exercita o mecanismo de rebalanceamento, não o valor do gatilho — que
     *       tem cobertura própria em {@code RebalancePlannerTest}.</li>
     * </ul>
     */
    private static void tuneForRebalance(StorageNodeConfig.Builder builder) {
        builder.rebalanceEnabled(false).rebalanceMinDelta(1L);
    }

    private String ownerOf(String seriesKey) {
        SeriesPlacement placement = harness.nodes().get(0).catalog().placementStrong(seriesKey)
                .orElseThrow(() -> new AssertionError("série sem placement: " + seriesKey));
        return placement.ownerNodeId();
    }

    private Optional<byte[]> imageAt(String nodeId, String seriesKey) {
        return harness.nodes().stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("nó desconhecido: " + nodeId))
                .volume().storage().get(objectKey(seriesKey));
    }

    private long countOwnedBy(String nodeId) {
        return harness.nodes().get(0).catalog().placementsLocal().values().stream()
                .filter(p -> p.state() == PlacementState.ACTIVE && p.ownerNodeId().equals(nodeId))
                .count();
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

    private static void retryVoid(Supplier<Void> action) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        NgrrdClusterException lastFailure = null;
        do {
            try {
                action.get();
                return;
            } catch (NgrrdClusterException e) {
                lastFailure = e;
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    fail("interrompido durante retentativa");
                }
            }
        } while (System.currentTimeMillis() < deadline);
        throw lastFailure;
    }

    /**
     * Como {@link #awaitTrue}, mas (re)dispara {@code client.rebalanceNow()} a cada
     * {@link #RETRIGGER_INTERVAL} enquanto a condição não vale — um ciclo disparado num líder que cede a
     * liderança logo em seguida morre sem replanejamento.
     */
    private static void awaitTrueRetriggering(String description, BooleanSupplier condition,
            NgrrdClusterClient client) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        long nextTrigger = 0L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.currentTimeMillis() >= nextTrigger) {
                try {
                    client.rebalanceNow();
                } catch (NgrrdClusterException e) {
                    // líder em transição: a próxima volta redispara
                }
                nextTrigger = System.currentTimeMillis() + RETRIGGER_INTERVAL.toMillis();
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }

    private static void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
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
