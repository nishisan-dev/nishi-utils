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

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regressão do bug de intermitência do failover (issue #117, integração tevent-cardinal): o
 * {@code broadcast} de heartbeat NÃO pode bloquear na discagem de um peer morto.
 *
 * <p>
 * {@code send()} faz um {@code connect} bloqueante (até {@code connectTimeout}, e novamente no
 * fallback de proxy) para qualquer peer sem conexão viva. Quando o {@code broadcast} era síncrono,
 * a discagem de ~10s para um peer recém-morto travava a thread do heartbeat, atrasando o heartbeat
 * para TODOS os peers vivos iterados depois — o que, num failover, faminava os heartbeats dos
 * sobreviventes além da janela de eviction e derrubava o quórum de forma intermitente (dependente
 * da ordem do HashMap + jitter). O fan-out passou a ser assíncrono (uma virtual thread por peer),
 * então um peer lento/morto nunca atrasa o heartbeat de um peer vivo.
 */
class BroadcastNonBlockingTest {

    @Test
    void broadcastDoesNotBlockWhenAPeerDialIsStuck() throws Exception {
        int localPort = freePort(Set.of());
        int stuckPort = freePort(Set.of(localPort));

        NodeInfo local = new NodeInfo(NodeId.of("local-node"), "127.0.0.1", localPort);
        NodeInfo stuckPeer = new NodeInfo(NodeId.of("stuck-peer"), "127.0.0.1", stuckPort);

        TcpTransport transport = new TcpTransport(config(local, stuckPeer));
        CountDownLatch dialReached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // Faz a discagem para o peer "morto" travar (como um connect sem resposta SYN).
        transport.setBeforeDialHook(id -> {
            if (id.equals(stuckPeer.nodeId())) {
                dialReached.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        transport.start();
        try {
            ClusterMessage heartbeat = ClusterMessage.lightweight(
                    MessageType.HEARTBEAT, "hb", local.nodeId(), null, HeartbeatPayload.now());

            long t0 = System.nanoTime();
            transport.broadcast(heartbeat);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            assertTrue(elapsedMs < 1000,
                    "broadcast() travou " + elapsedMs + "ms na discagem de um peer preso — "
                            + "o fan-out deveria ser assíncrono (1 virtual thread por peer)");

            // Garante que o teste exercitou de fato o caminho de discagem (e não passou por vácuo).
            assertTrue(dialReached.await(3, TimeUnit.SECONDS),
                    "a discagem para o peer preso nunca foi tentada — o teste não exercitou o caminho");
        } finally {
            release.countDown();
            transport.close();
        }
    }

    @Test
    void broadcastIsBestEffortAfterClose() throws Exception {
        int localPort = freePort(Set.of());
        int peerPort = freePort(Set.of(localPort));
        NodeInfo local = new NodeInfo(NodeId.of("local-node"), "127.0.0.1", localPort);
        NodeInfo peer = new NodeInfo(NodeId.of("peer-node"), "127.0.0.1", peerPort);

        TcpTransport transport = new TcpTransport(config(local, peer));
        transport.start();
        transport.close();

        ClusterMessage heartbeat = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", local.nodeId(), null, HeartbeatPayload.now());

        // Após close(), o workerPool está encerrado; o broadcast deve ser best-effort e NÃO lançar
        // RejectedExecutionException (que cancelaria a task agendada de um caller como o
        // LeaderReelectionService.tick()).
        assertDoesNotThrow(() -> transport.broadcast(heartbeat));
    }

    private static TcpTransportConfig config(NodeInfo local, NodeInfo... peers) {
        TcpTransportConfig.Builder b = TcpTransportConfig.builder(local)
                .reconnectInterval(Duration.ofSeconds(30))   // longo: não queremos re-dials interferindo
                .routeProbeInterval(Duration.ofSeconds(30))
                .connectTimeout(Duration.ofSeconds(5));
        for (NodeInfo p : peers) {
            b.addPeer(p);
        }
        return b.build();
    }

    private static int freePort(Set<Integer> avoid) throws java.io.IOException {
        for (int i = 0; i < 50; i++) {
            try (java.net.ServerSocket s = new java.net.ServerSocket()) {
                s.setReuseAddress(true);
                s.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                int p = s.getLocalPort();
                if (p > 0 && !avoid.contains(p)) {
                    return p;
                }
            }
        }
        throw new java.io.IOException("no free port");
    }
}
