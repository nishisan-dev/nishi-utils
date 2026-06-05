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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Guarda de regressão para o invariante de malha determinística introduzido pela
 * correção da issue #117: peers iniciados concorrentemente com configuração
 * full-mesh convergem para uma malha <b>direta</b> e <b>estável</b> (sem proxy,
 * sem flapping), independente da ordem de boot.
 *
 * <p>
 * <b>Escopo:</b> em loopback (localhost) o <i>simultaneous open</i> completa sem
 * {@code RST}/half-open, então este teste valida o <i>invariante</i> garantido
 * por {@code registerLiveConnection} (reconciliação determinística por NodeId) —
 * ele não dispara a manifestação dependente de rede real do bug. A reprodução
 * fiel do failover quebrado (rede com latência, hub como SPOF) é coberta pelo IT
 * Docker {@code NGridConcurrentMeshFailoverIT}.
 */
class TcpTransportConcurrentMeshTest {

    /**
     * Três transports full-mesh iniciados o mais simultaneamente possível devem
     * convergir para uma malha totalmente direta (6 conexões direcionais) e
     * mantê-la estável, sem recorrer a proxy.
     */
    @Test
    void concurrentlyStartedPeersFormFullDirectMesh() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));

        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "127.0.0.1", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "127.0.0.1", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "127.0.0.1", portC);

        // Full-mesh: cada nó conhece os outros dois desde o boot.
        TcpTransport transA = new TcpTransport(meshConfig(infoA, infoB, infoC));
        TcpTransport transB = new TcpTransport(meshConfig(infoB, infoA, infoC));
        TcpTransport transC = new TcpTransport(meshConfig(infoC, infoA, infoB));

        try {
            startConcurrently(transA, transB, transC);

            // 1) Descoberta mútua (sanity)
            awaitDiscovery(transA, infoB.nodeId(), infoC.nodeId());
            awaitDiscovery(transB, infoA.nodeId(), infoC.nodeId());
            awaitDiscovery(transC, infoA.nodeId(), infoB.nodeId());

            // 2) Malha direta total e estável
            awaitFullDirectMesh(
                    List.of(transA, transB, transC),
                    List.of(infoA, infoB, infoC),
                    Duration.ofSeconds(20));

            // 3) Rotas devem ser DIRECT (nextHop == destino), nunca via proxy
            assertDirectRoute(transA, infoB.nodeId());
            assertDirectRoute(transA, infoC.nodeId());
            assertDirectRoute(transB, infoA.nodeId());
            assertDirectRoute(transB, infoC.nodeId());
            assertDirectRoute(transC, infoA.nodeId());
            assertDirectRoute(transC, infoB.nodeId());
        } finally {
            closeQuietly(transA, transB, transC);
        }
    }

    private static TcpTransportConfig meshConfig(NodeInfo local, NodeInfo... peers) {
        TcpTransportConfig.Builder builder = TcpTransportConfig.builder(local)
                .reconnectInterval(Duration.ofMillis(500))
                .routeProbeInterval(Duration.ofSeconds(1))
                .connectTimeout(Duration.ofSeconds(1));
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        return builder.build();
    }

    /** Inicia os transports o mais simultaneamente possível para forçar o simultaneous open. */
    private static void startConcurrently(TcpTransport... transports) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(transports.length);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(transports.length);
        for (TcpTransport t : transports) {
            Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    go.await();
                    t.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS), "threads de start não ficaram prontas");
        go.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "transports não iniciaram a tempo");
    }

    private static void awaitDiscovery(TcpTransport transport, NodeId... targets) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (NodeId target : targets) {
                if (transport.peers().stream().noneMatch(p -> p.nodeId().equals(target))) {
                    all = false;
                    break;
                }
            }
            if (all) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Descoberta incompleta em " + transport.local().nodeId());
    }

    /**
     * Aguarda a malha direta total e exige que ela permaneça estável por uma
     * janela, para descartar estados transitórios.
     */
    private static void awaitFullDirectMesh(List<TcpTransport> transports,
                                            List<NodeInfo> infos,
                                            Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            // Exige malha direta total E que ela permaneça estável por ~2s, para
            // descartar estados transitórios (flapping) de convergência.
            if (allDirectlyConnected(transports, infos)
                    && stableFor(transports, infos, Duration.ofSeconds(2))) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Malha direta total não convergiu/estabilizou dentro de " + timeout
                + ".\nEstado final:\n  " + meshState(transports, infos));
    }

    private static boolean stableFor(List<TcpTransport> transports,
                                     List<NodeInfo> infos,
                                     Duration window) throws InterruptedException {
        long end = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < end) {
            if (!allDirectlyConnected(transports, infos)) {
                return false;
            }
            Thread.sleep(50);
        }
        return true;
    }

    /** Verdadeiro quando todos os pares direcionais possuem conexão direta aberta. */
    private static boolean allDirectlyConnected(List<TcpTransport> transports, List<NodeInfo> infos) {
        for (int i = 0; i < transports.size(); i++) {
            for (int j = 0; j < infos.size(); j++) {
                if (i != j && !transports.get(i).isConnected(infos.get(j).nodeId())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String meshState(List<TcpTransport> transports, List<NodeInfo> infos) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < transports.size(); i++) {
            for (int j = 0; j < infos.size(); j++) {
                if (i == j) {
                    continue;
                }
                boolean connected = transports.get(i).isConnected(infos.get(j).nodeId());
                sb.append(infos.get(i).nodeId().value())
                        .append(" -> ")
                        .append(infos.get(j).nodeId().value())
                        .append(connected ? " [direct] " : " [MISSING] ");
            }
        }
        return sb.toString();
    }

    private static void assertDirectRoute(TcpTransport transport, NodeId target) {
        Optional<NodeId> hop = transport.getRouter().nextHop(target);
        assertTrue(hop.isPresent(),
                "Sem rota de " + transport.local().nodeId() + " para " + target);
        assertEquals(target, hop.get(),
                "Rota de " + transport.local().nodeId() + " para " + target
                        + " deveria ser DIRECT, mas é via " + hop.get());
    }

    private static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws java.io.IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (java.net.ServerSocket socket = new java.net.ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new java.io.IOException("Unable to allocate a free local port");
    }
}
