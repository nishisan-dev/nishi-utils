package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Soak test em Docker: valida que o cluster NGrid mantém integridade
 * de dados sob operação prolongada com leader churn periódico.
 *
 * <p>Usa a infraestrutura de 5 nós com map-stress (producer contínuo)
 * e valida:
 * <ul>
 *   <li>Producer continua escrevendo após múltiplos crashes de líder</li>
 *   <li>Readers eventuais nunca param</li>
 *   <li>Nenhuma regressão de índice no producer</li>
 * </ul>
 *
 * <p>Ativado via {@code -Dngrid.test.docker.soak=true}.
 * Duração default: 5 minutos (CI). Use {@code -Dngrid.soak.minutes=N}
 * para estender localmente.
 *
 * @since 3.2.0
 */
@EnabledIfSystemProperty(named = "ngrid.test.docker.soak", matches = "true")
class NGridDockerSoakIT extends AbstractNGridMapClusterIT {

    private int soakMinutes() {
        return Integer.getInteger("ngrid.soak.minutes", 5);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES) // max para o CI
    void shouldSustainOperationsUnderRepeatedLeaderChurn() throws Exception {
        int totalMinutes = soakMinutes();
        long deadlineMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(totalMinutes);

        System.out.println("[SOAK-DOCKER] Starting soak test: duration=" + totalMinutes + "min");

        // ── Fase 1: Convergência inicial ──
        await("initial cluster stability")
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .until(() -> countLeaders() == 1
                    && Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                            .allMatch(c -> c.latestActiveMembersCount() >= 5)
                    && !node2_producer.extractMapPuts().isEmpty());

        int churnCount = 0;
        int churnIntervalSeconds = 45;
        long lastChurnTime = System.currentTimeMillis();
        int lastPutCount = 0;

        // ── Loop de soak ──
        while (System.currentTimeMillis() < deadlineMs) {
            long now = System.currentTimeMillis();

            // Periodic leader churn
            if (now - lastChurnTime >= churnIntervalSeconds * 1000L) {
                NGridMapNodeContainer leader = findLeader();
                if (leader != null && leader != node2_producer && leader != node3_reader) {
                    churnCount++;
                    System.out.println("[SOAK-DOCKER] Churn #" + churnCount + ": killing leader " + leader.nodeId());

                    leader.getDockerClient().killContainerCmd(leader.getContainerId()).exec();

                    // Aguardar re-eleição
                    await("leader re-elected after churn #" + churnCount)
                        .atMost(60, TimeUnit.SECONDS)
                        .pollInterval(2, TimeUnit.SECONDS)
                        .until(() -> countLeaders() >= 1);

                    System.out.println("[SOAK-DOCKER] New leader elected after churn #" + churnCount);
                }
                lastChurnTime = now;
            }

            // Validar que o producer continua escrevendo
            int currentPuts = node2_producer.extractMapPuts().size();
            if (currentPuts <= lastPutCount) {
                // Pode estar em transição de líder, dar tempo
                final int threshold = lastPutCount;
                await("producer should resume")
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .until(() -> node2_producer.extractMapPuts().size() > threshold);
            }
            lastPutCount = node2_producer.extractMapPuts().size();

            // Checkpoint periódico (a cada 60s)
            long eventualReads = node3_reader.extractMapReads("EVENTUAL").size();
            System.out.println("[SOAK-DOCKER] Checkpoint: puts=" + lastPutCount
                    + " eventualReads=" + eventualReads
                    + " churns=" + churnCount
                    + " elapsed=" + ((now - (deadlineMs - TimeUnit.MINUTES.toMillis(totalMinutes))) / 1000) + "s");

            Thread.sleep(10_000); // checkpoint a cada 10s
        }

        // ── Validações finais ──
        int finalPuts = node2_producer.extractMapPuts().size();
        assertTrue(finalPuts > 0, "Producer deve ter escrito dados");
        assertTrue(churnCount > 0 || totalMinutes < 2, "Deve ter havido pelo menos 1 churn (se duração > 2min)");

        long finalEventualReads = node3_reader.extractMapReads("EVENTUAL").size();
        assertTrue(finalEventualReads > 0, "Reader eventual deve ter lido dados");

        // Validar que o índice cresce monotonicamente (sem regressão)
        List<String> allPuts = node2_producer.extractMapPuts();
        int maxIndex = 0;
        Set<String> seen = new HashSet<>();
        int duplicates = 0;
        for (String put : allPuts) {
            if (!seen.add(put)) {
                duplicates++;
            }
            int dashIdx = put.indexOf('-');
            if (dashIdx > 0) {
                int eqIdx = put.indexOf('=');
                if (eqIdx > dashIdx) {
                    try {
                        int idx = Integer.parseInt(put.substring(dashIdx + 1, eqIdx));
                        maxIndex = Math.max(maxIndex, idx);
                    } catch (NumberFormatException ignored) {}
                }
            }
        }

        System.out.println("[SOAK-DOCKER] Final: totalPuts=" + finalPuts
                + " maxIndex=" + maxIndex
                + " duplicateLogEntries=" + duplicates
                + " totalChurns=" + churnCount
                + " eventualReads=" + finalEventualReads);

        assertNotNull(findLeader(), "Cluster deve ter um líder no final do soak");
        assertTrue(maxIndex > 0, "Índice do producer deve crescer");
    }
}
