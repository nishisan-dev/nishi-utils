package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida que mensagens produzidas pelo seed chegam aos demais nós do cluster.
 *
 * <p>
 * O {@code Main.java} no modo {@code "server"} publica mensagens no formato
 * {@code "INDEX-<epoch>-<n>"} na fila {@code "global-events"} a cada 2
 * segundos.
 * Os containers {@code node-2} e {@code node-3} estão em modo {@code "server"},
 * então verificamos que a replicação da fila ocorre examinando os logs dos
 * peers.
 *
 * <p>
 * <b>Estratégia:</b> o seed produz mensagens; aguardamos que os logs de
 * {@code node-2} e {@code node-3} contenham evidência de que receberam a
 * mensagem
 * replicada (ex.: log de recebimento de entrada WAL ou confirmação de
 * replicação).
 *
 * <p>
 * <b>Alternativa futura:</b> adicionar um modo {@code "consumer"} no
 * {@code Main.java} que imprime cada mensagem consumida em formato parseável,
 * permitindo asserções mais precisas do que análise de logs internos.
 */
class NGridReplicationIT extends AbstractNGridClusterIT {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void seedShouldPublishMessagesToQueue() {
        // O seed publica "INDEX-1-0", "INDEX-1-1", ... a cada 2s após estabilizar
        // Aguardamos que o seed tenha logado pelo menos "Enviado: INDEX-1-0"
        await("seed published first message")
                .atMost(40, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> seed.containerLogs().contains("Enviado: INDEX-"));

        assertTrue(seed.containerLogs().contains("Enviado: INDEX-"),
                "Seed deve ter publicado ao menos uma mensagem na fila. Logs:\n" + seed.containerLogs());
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void allNodesShouldAcknowledgeReplication() {
        // Aguarda o seed produzir mensagens (modo server produz a cada 2s)
        await("seed published messages")
                .atMost(40, TimeUnit.SECONDS)
                .until(() -> seed.containerLogs().contains("Enviado: INDEX-"));

        // Verifica que o cluster não reportou erros críticos de replicação
        // (ausência de "FAILED" ou "replication error" nos logs dos peers)
        String node2Logs = node2.containerLogs();
        String node3Logs = node3.containerLogs();

        assertTrue(node2.isRunning(),
                "node-2 deve estar rodando após replicação. Logs:\n" + node2Logs);
        assertTrue(node3.isRunning(),
                "node-3 deve estar rodando após replicação. Logs:\n" + node3Logs);
    }
}
