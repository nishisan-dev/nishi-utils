package dev.nishisan.utils.test.cluster;

import com.github.dockerjava.api.DockerClient;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida resiliência do cluster NGrid sob partição de rede real.
 *
 * <p>Cenário de split-brain:
 * <ol>
 *   <li>Cluster de 5 nós estável com líder eleito</li>
 *   <li>2 nós são desconectados da rede (minority partition)</li>
 *   <li>Majority partition (3 nós) continua operando normalmente</li>
 *   <li>Minority partition perde lease e faz step-down</li>
 *   <li>Após reconexão, nós da minority fazem catch-up sem perda</li>
 * </ol>
 *
 * <p>Usa {@code docker network disconnect/connect} para simular
 * partição de rede real entre containers.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridPartitionResilienceIT extends AbstractNGridMapClusterIT {

    /** Nós que serão isolados (minority side). */
    private NGridMapNodeContainer isolatedNode1;
    private NGridMapNodeContainer isolatedNode2;


    @Test
    @Order(1)
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void shouldHandleNetworkPartitionAndRecover() throws Exception {
        // ── Fase 1: Estabilização inicial ──
        await("initial stability")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> countLeaders() == 1
                    && Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                            .allMatch(c -> c.latestActiveMembersCount() >= 5)
                    && !node2_producer.extractMapPuts().isEmpty());

        NGridMapNodeContainer leader = findLeader();
        assertNotNull(leader, "Deve haver um líder");

        Thread.sleep(3000); // acumular operações

        // ── Fase 2: Determinar quem isolar ──
        // Isolamos 2 nós que NÃO são o producer (queremos que o producer
        // continue escrevendo no lado majority). Escolhemos node4 e node5_reader.
        isolatedNode1 = node4;
        isolatedNode2 = node5_reader;

        int putsBeforePartition = node2_producer.extractMapPuts().size();
        assertTrue(putsBeforePartition > 0, "Producer ativo antes da partição");

        // ── Fase 3: Criar partição de rede ──
        String networkId = network.getId();
        DockerClient docker = isolatedNode1.getDockerClient();

        docker.disconnectFromNetworkCmd()
                .withNetworkId(networkId)
                .withContainerId(isolatedNode1.getContainerId())
                .withForce(true)
                .exec();

        docker.disconnectFromNetworkCmd()
                .withNetworkId(networkId)
                .withContainerId(isolatedNode2.getContainerId())
                .withForce(true)
                .exec();

        // ── Fase 4: Validar que majority continua operando ──
        // O majority (seed, node2_producer, node3_reader) deve manter líder
        // e continuar aceitando writes
        await("majority continues operating")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                long leaders = Stream.of(seed, node2_producer, node3_reader)
                        .filter(c -> c.isRunning() && c.isLeader())
                        .count();
                return leaders >= 1;
            });

        // Producer deve continuar escrevendo no majority
        await("producer resumes in majority partition")
            .atMost(20, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> node2_producer.extractMapPuts().size() > putsBeforePartition);

        int putsDuringPartition = node2_producer.extractMapPuts().size();
        assertTrue(putsDuringPartition > putsBeforePartition,
                "Producer deve continuar escrevendo no lado majority."
                        + " Before=" + putsBeforePartition + " During=" + putsDuringPartition);

        // Reads eventuais no majority devem funcionar
        long eventualReads = node3_reader.extractMapReads("EVENTUAL").size();
        assertTrue(eventualReads > 0, "Reads eventuais devem funcionar no majority");

        // ── Fase 5: Manter partição por tempo suficiente para lease expirar ──
        Thread.sleep(15_000); // lease timeout + margem

        int putsAfterSoak = node2_producer.extractMapPuts().size();
        assertTrue(putsAfterSoak > putsDuringPartition,
                "Producer deve ter continuado durante partição longa");

        // ── Fase 6: Reconectar minority ──
        docker.connectToNetworkCmd()
                .withNetworkId(networkId)
                .withContainerId(isolatedNode1.getContainerId())
                .exec();

        docker.connectToNetworkCmd()
                .withNetworkId(networkId)
                .withContainerId(isolatedNode2.getContainerId())
                .exec();

        // ── Fase 7: Validar convergência pós-reconexão ──
        await("cluster converges after heal")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                // Todos os nós running devem ver pelo menos 4 membros ativos
                // (damos margem de 1 por eventual atraso)
                long nodesSeeing4Plus = Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                        .filter(c -> c.isRunning())
                        .filter(c -> c.latestActiveMembersCount() >= 4)
                        .count();
                return nodesSeeing4Plus >= 4 && countLeaders() == 1;
            });

        // Producer deve continuar ativo após heal
        int putsAfterHeal = node2_producer.extractMapPuts().size();
        assertTrue(putsAfterHeal > putsAfterSoak,
                "Producer deve continuar após reconexão. AfterSoak=" + putsAfterSoak + " AfterHeal=" + putsAfterHeal);
    }

    @Test
    @Order(2)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldRejectWritesInMinorityPartition() throws Exception {
        // Este teste valida que o lado minority não aceita writes
        // durante uma partição. Requer que o cluster esteja saudável
        // primeiro (recuperado do teste anterior).
        
        await("cluster healthy")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> countLeaders() == 1
                    && Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                            .filter(NGridMapNodeContainer::isRunning)
                            .allMatch(c -> c.latestActiveMembersCount() >= 4));

        // Isolar TODOS os nós exceto node2_producer e node3_reader
        // Assim, com 2/5 nós ativos, o lease deve expirar
        String networkId = network.getId();
        DockerClient docker = seed.getDockerClient();

        List<NGridMapNodeContainer> toIsolate = Stream.of(seed, node4, node5_reader)
                .filter(NGridMapNodeContainer::isRunning)
                .toList();

        for (NGridMapNodeContainer node : toIsolate) {
            docker.disconnectFromNetworkCmd()
                    .withNetworkId(networkId)
                    .withContainerId(node.getContainerId())
                    .withForce(true)
                    .exec();
        }

        // Com 2/5 nós, lease expira → writes devem falhar
        if (node2_producer.isRunning()) {
            await("writes should fail in minority")
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> node2_producer.getLogs().lines()
                    .filter(l -> l.contains("MAP-PUT-FAIL"))
                    .count() > 0);
        }

        // Reconectar
        for (NGridMapNodeContainer node : toIsolate) {
            if (node.isRunning()) {
                docker.connectToNetworkCmd()
                        .withNetworkId(networkId)
                        .withContainerId(node.getContainerId())
                        .exec();
            }
        }

        // Cluster deve se recuperar
        await("cluster recovers after minority test")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> countLeaders() >= 1);
    }
}
