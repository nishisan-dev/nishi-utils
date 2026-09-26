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
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.LeavePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Esquecimento de membros efêmeros (clientes inelegíveis a líder) no transporte: um id esquecido sai
 * de {@code knownPeers} de vez, não é mais discado, fica protegido por um tombstone contra readmissão
 * de segunda mão (gossip, mensagem retransmitida, socket sem handshake) e volta só por um handshake
 * direto (nova encarnação).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class TcpTransportLeaveTest {

    private static final Duration RECONNECT = Duration.ofMillis(150);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final Set<Integer> usedPorts = new HashSet<>();

    @AfterEach
    void tearDown() {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    @Test
    void forgottenPeerIsNotDialedNorReAddedByGossipAndComesBackByDirectHandshake() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT));
        RecordingListener events = listen(storage);
        // Listening port nobody binds: the storage (lower id) is the designated dialer of the client.
        NodeInfo client = info("z-client", freePort(), true);

        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "cliente não conectou");

        assertTrue(storage.forget(client.nodeId()));
        AtomicInteger dialsToClient = countDials(storage, client.nodeId());

        assertFalse(knows(storage, client.nodeId()), "o id esquecido deveria sair de peers()");
        assertTrue(storage.isDeparted(client.nodeId()));
        assertFalse(storage.isConnected(client.nodeId()));

        // A third node that still lists the client: in its handshake and in a PEER_UPDATE.
        NodeInfo third = info("m-storage", freePort(), false);
        RawPeer thirdLink = raw(storage);
        thirdLink.send(handshake(third, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(third.nodeId()), "terceiro nó não conectou");
        thirdLink.send(ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", third.nodeId(), null,
                new PeerUpdatePayload(Set.of(client, third), Map.of(client.nodeId(), 1.0))));

        Thread.sleep(RECONNECT.toMillis() * 6);
        assertFalse(knows(storage, client.nodeId()), "gossip de terceiros não pode readmitir o id esquecido");
        assertEquals(0, dialsToClient.get(), "o id esquecido não pode ser discado de novo");
        assertEquals(List.of("left:z-client"), events.peerEvents(client.nodeId()),
                "a saída é reportada uma vez, como onPeerLeft, sem disconnect repetido");

        // New incarnation: direct handshake with the same id.
        RawPeer reborn = raw(storage);
        reborn.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "a nova encarnação deveria ser aceita");
        assertTrue(knows(storage, client.nodeId()));
        assertFalse(storage.isDeparted(client.nodeId()), "o handshake direto limpa o tombstone");
    }

    @Test
    void relayedMessagesAndHandshakelessSocketsOfAForgottenPeerAreDropped() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        RecordingListener events = listen(storage);
        NodeInfo client = info("z-client", 0, true);
        NodeInfo relay = info("m-storage", freePort(), false);

        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        RawPeer relayLink = raw(storage);
        relayLink.send(handshake(relay, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(client.nodeId()) && storage.isConnected(relay.nodeId()),
                "peers não conectaram");

        storage.forget(client.nodeId());

        relayLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "relayed-from-departed",
                client.nodeId(), storage.local().nodeId(), "late"));
        RawPeer handshakeless = raw(storage);
        handshakeless.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "inferred-departed",
                client.nodeId(), storage.local().nodeId(), "late"));
        relayLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "from-relay",
                relay.nodeId(), storage.local().nodeId(), "control"));

        awaitTrue(() -> events.qualifiers().contains("from-relay"), "mensagem de controle não chegou");
        Thread.sleep(300);
        assertFalse(events.qualifiers().contains("relayed-from-departed"),
                "mensagem retransmitida de um id esquecido deveria ser descartada");
        assertFalse(events.qualifiers().contains("inferred-departed"),
                "socket sem handshake identificado como id esquecido não pode ser publicado");
        assertFalse(storage.isConnected(client.nodeId()));
        assertFalse(knows(storage, client.nodeId()));
    }

    @Test
    void forgettingAPeerFailsItsPendingResponses() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .requestTimeout(Duration.ofSeconds(30)));
        NodeInfo client = info("z-client", 0, true);
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "cliente não conectou");

        CompletableFuture<ClusterMessage> pending = storage.sendAndAwait(ClusterMessage.request(
                MessageType.CLIENT_REQUEST, "never-answered", storage.local().nodeId(), client.nodeId(), "x"));
        storage.forget(client.nodeId());

        ExecutionException failure = assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
        assertInstanceOf(PeerDisconnectedException.class, failure.getCause());
    }

    /**
     * Envio para um id esquecido (ex.: resposta ou notificação atrasada de um serviço para o cliente que
     * saiu): falha rápido, sem procurar rota, sem log de "No connection available" e sem recriar estado
     * de roteamento para o id.
     */
    @Test
    void sendingToAForgottenPeerFailsFastWithoutRecreatingItsRoute() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        NodeInfo client = info("z-client", freePort(), true);
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "cliente não conectou");
        storage.forget(client.nodeId());
        List<String> warnings = new CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                if (record.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue()) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(TcpTransport.class.getName());
        logger.addHandler(capture);
        try {
            storage.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "late-notify", storage.local().nodeId(),
                    client.nodeId(), "x"));
            CompletableFuture<ClusterMessage> request = storage.sendAndAwait(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "late-request", storage.local().nodeId(), client.nodeId(), "x"));

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> request.get(1, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, failure.getCause());
            assertTrue(warnings.isEmpty(), "envio para um id esquecido não é falha de conexão: " + warnings);
            assertFalse(storage.getRouter().routesSnapshot().containsKey(client.nodeId()),
                    "o envio não pode recriar rota para o id esquecido");
        } finally {
            logger.removeHandler(capture);
        }
    }

    /**
     * Malha parcial: um cliente VIVO que o storage não consegue discar (firewall, link parcial) mas que
     * segue falando com ele por relay não pode ser esquecido pelo gatilho lento — senão todo o tráfego
     * retransmitido dele passa a ser descartado e não há recuperação (ele nunca faz handshake direto).
     */
    @Test
    void liveEphemeralPeerReachableOnlyThroughARelayIsNotForgotten() throws Exception {
        Duration forgetAfter = Duration.ofMillis(600);
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT)
                .connectTimeout(Duration.ofMillis(200))
                .departedPeerForgetAfter(forgetAfter));
        RecordingListener events = listen(storage);
        NodeInfo client = info("z-client", freePort(), true); // nobody listens: the storage cannot dial it
        NodeInfo relay = info("m-storage", freePort(), false);
        RawPeer relayLink = raw(storage);
        relayLink.send(handshake(relay, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(relay.nodeId()) && knows(storage, client.nodeId()),
                "cliente não foi aprendido pelo gossip do relay");

        int sent = 0;
        long until = System.currentTimeMillis() + forgetAfter.toMillis() * 4;
        while (System.currentTimeMillis() < until) {
            relayLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "via-relay-" + sent, client.nodeId(),
                    storage.local().nodeId(), "hb"));
            sent++;
            Thread.sleep(100);
        }
        int expected = sent;
        awaitTrue(() -> events.qualifiers().size() >= expected, "tráfego retransmitido não chegou");

        assertTrue(knows(storage, client.nodeId()), "cliente vivo alcançável só por relay foi esquecido");
        assertFalse(storage.isDeparted(client.nodeId()));
        assertEquals(sent, events.qualifiers().stream().filter(q -> q.startsWith("via-relay-")).count(),
                "todo o tráfego retransmitido do cliente vivo deveria ser entregue");

        // Once its relayed traffic stops as well, the backstop applies again.
        awaitTrue(() -> !knows(storage, client.nodeId()), "cliente silencioso e sem conexão não foi esquecido");
    }

    /**
     * Um socket que chega durante o flush do LEAVE (transporte saindo) é fechado em silêncio: não é
     * erro do accept loop.
     */
    @Test
    void socketArrivingWhileLeavingIsClosedQuietly() throws Exception {
        TcpTransport closing = start(TcpTransportConfig.builder(info("z-client", freePort(), true))
                .compressionEnabled(false)
                .leaveFlushTimeout(Duration.ofSeconds(3)));
        NodeInfo stuck = info("a-storage", freePort(), false);
        // A peer that announced LEAVE support but never reads: its socket buffers fill up, so the LEAVE
        // queued behind the backlog cannot be flushed and close() waits out the flush timeout.
        java.net.Socket stuckSocket = new java.net.Socket();
        closeables.add(stuckSocket);
        stuckSocket.setReceiveBufferSize(4096);
        stuckSocket.connect(new InetSocketAddress(closing.local().host(), closing.local().port()), 5_000);
        byte[] hs = new dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec()
                .encode(handshake(stuck, closing.local(), Set.of()));
        java.io.DataOutputStream out = new java.io.DataOutputStream(stuckSocket.getOutputStream());
        out.writeInt(hs.length);
        out.write(hs);
        out.flush();
        awaitTrue(() -> closing.announcesLeaveTo(stuck.nodeId()), "handshake não concluiu");
        String chunk = "x".repeat(64 * 1024);
        for (int i = 0; i < 400; i++) {
            closing.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "backlog", closing.local().nodeId(),
                    stuck.nodeId(), chunk + i));
        }

        List<String> warnings = new CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                if (record.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue()) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(TcpTransport.class.getName());
        logger.addHandler(capture);
        try {
            Thread closer = Thread.ofVirtual().start(() -> {
                try {
                    closing.close();
                } catch (IOException ignored) {
                    // best-effort
                }
            });
            Thread.sleep(300); // close() is now waiting for the LEAVE flush
            assertTrue(closer.isAlive(), "precondição: close() deveria estar esperando o flush do LEAVE");
            try (java.net.Socket late = new java.net.Socket()) {
                late.connect(new InetSocketAddress(closing.local().host(), closing.local().port()), 1_000);
                Thread.sleep(200);
            }
            closer.join(10_000);
            assertTrue(warnings.stream().noneMatch(w -> w.startsWith("Error accepting connection")),
                    "socket que chega durante o LEAVE não é erro do accept loop: " + warnings);
        } finally {
            logger.removeHandler(capture);
        }
    }

    /**
     * Saída sem LEAVE (kill -9, OOM, perda de rede): o peer efêmero é esquecido depois de
     * {@code departedPeerForgetAfter} sem conexão; um peer elegível a líder nunca é esquecido assim, e
     * um efêmero ainda conectado também não.
     */
    @Test
    void ephemeralPeerDisconnectedBeyondTheForgetWindowIsForgottenButVotersAreKept() throws Exception {
        Duration forgetAfter = Duration.ofMillis(600);
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT)
                .connectTimeout(Duration.ofMillis(200))
                .departedPeerForgetAfter(forgetAfter));
        RecordingListener events = listen(storage);
        NodeInfo killedClient = info("z-client", freePort(), true);
        NodeInfo liveClient = info("y-client", 0, true);
        NodeInfo voter = info("m-storage", freePort(), false);

        RawPeer killed = raw(storage);
        killed.send(handshake(killedClient, storage.local(), Set.of()));
        RawPeer live = raw(storage);
        live.send(handshake(liveClient, storage.local(), Set.of()));
        RawPeer voterLink = raw(storage);
        voterLink.send(handshake(voter, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(killedClient.nodeId()) && storage.isConnected(liveClient.nodeId())
                && storage.isConnected(voter.nodeId()), "peers não conectaram");

        long killedAt = System.currentTimeMillis();
        killed.close();
        voterLink.close();

        awaitTrue(() -> !knows(storage, killedClient.nodeId()), "cliente morto sem LEAVE não foi esquecido");
        long forgottenAfterMs = System.currentTimeMillis() - killedAt;
        assertTrue(forgottenAfterMs >= forgetAfter.toMillis(),
                "esquecido cedo demais: " + forgottenAfterMs + " ms < " + forgetAfter);
        assertTrue(storage.isDeparted(killedClient.nodeId()));
        assertTrue(events.peerEvents(killedClient.nodeId()).contains("left:z-client"));

        Thread.sleep(forgetAfter.toMillis() * 2);
        assertTrue(knows(storage, voter.nodeId()), "um peer elegível a líder nunca é esquecido por timeout");
        assertFalse(storage.isDeparted(voter.nodeId()));
        assertTrue(knows(storage, liveClient.nodeId()), "um efêmero conectado não pode ser esquecido");
    }

    /**
     * Fechamento gracioso de um cliente: o LEAVE faz o storage esquecê-lo na hora (sem esperar o prazo
     * de desconexão), não discá-lo mais e aceitá-lo de volta quando religar com o mesmo id.
     */
    @Test
    void closingAnIneligibleClientMakesTheStorageForgetItAtOnce() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT));
        RecordingListener events = listen(storage);
        int clientPort = freePort();
        TcpTransport client = client("z-client", clientPort, storage);
        NodeId clientId = client.local().nodeId();
        awaitTrue(() -> storage.isConnected(clientId) && client.isConnected(storage.local().nodeId()),
                "cliente não conectou");

        client.close();
        AtomicInteger dialsToClient = countDials(storage, clientId);

        // Well below departedPeerForgetAfter (1 min by default): only the LEAVE explains it.
        awaitTrue(() -> !knows(storage, clientId), "o storage não esqueceu o cliente que saiu com LEAVE");
        assertTrue(storage.isDeparted(clientId));
        Thread.sleep(RECONNECT.toMillis() * 6);
        assertEquals(0, dialsToClient.get(), "o cliente que saiu não pode ser discado");
        assertEquals(List.of("left:z-client"), events.peerEvents(clientId),
                "a saída é reportada uma vez, como onPeerLeft");

        TcpTransport reborn = client("z-client", clientPort, storage);
        awaitTrue(() -> storage.isConnected(clientId) && knows(storage, clientId),
                "o mesmo id religando deveria ser aceito");
        assertFalse(storage.isDeparted(clientId));
        reborn.close();
        awaitTrue(() -> !knows(storage, clientId), "a nova encarnação também sai com LEAVE");
    }

    @Test
    void closingALeaderEligibleNodeKeepsItAsAKnownVoter() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT));
        TcpTransport voter = start(TcpTransportConfig.builder(info("m-storage", freePort(), false))
                .reconnectInterval(RECONNECT)
                .addPeer(storage.local()));
        NodeId voterId = voter.local().nodeId();
        awaitHandshaked(voter, storage);

        RecordingListener events = listen(storage);
        voter.close();
        awaitTrue(() -> !storage.isConnected(voterId), "desconexão não percebida");
        awaitTrue(() -> events.peerEvents(voterId).contains("leaving:m-storage"),
                "a saída anunciada de um votante é reportada (onPeerLeaving) para o caminho rápido");
        Thread.sleep(RECONNECT.toMillis() * 4);
        assertTrue(knows(storage, voterId), "um membro elegível a líder nunca é esquecido pelo LEAVE");
        assertFalse(storage.isDeparted(voterId));
    }

    @Test
    void leaveIsSentOnlyToPeersThatAnnouncedSupport() throws Exception {
        TcpTransport closing = start(TcpTransportConfig.builder(info("z-client", freePort(), true)));
        NodeInfo modern = info("a-storage", freePort(), false);
        NodeInfo legacy = info("b-storage", freePort(), false);
        RawPeer modernLink = raw(closing);
        modernLink.send(handshake(modern, closing.local(), Set.of()));
        RawPeer legacyLink = raw(closing);
        legacyLink.send(legacyHandshake(legacy, closing.local()));
        awaitTrue(() -> closing.isConnected(modern.nodeId()) && closing.isConnected(legacy.nodeId()),
                "peers não conectaram");

        closing.close();

        awaitTrue(() -> modernLink.received().stream().anyMatch(m -> m.type() == MessageType.LEAVE),
                "o peer que anunciou suporte deveria receber o LEAVE");
        LeavePayload payload = modernLink.received().stream().filter(m -> m.type() == MessageType.LEAVE)
                .findFirst().orElseThrow().payload(LeavePayload.class);
        assertEquals(closing.local().nodeId(), payload.node().nodeId());
        Thread.sleep(200);
        assertTrue(legacyLink.received().stream().noneMatch(m -> m.type() == MessageType.LEAVE),
                "um peer antigo (sem supportsLeave) nunca recebe LEAVE: " + legacyLink.received());
    }

    /** O LEAVE só vale de primeira mão: na conexão rastreada para aquele peer, com a identidade dela. */
    @Test
    void leaveThatIsNotFirstHandIsIgnored() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        // Distinct listen addresses: two portless peers on one host would collide by address.
        NodeInfo client = info("z-client", freePort(), true);
        NodeInfo other = info("y-client", freePort(), true);
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        RawPeer otherLink = raw(storage);
        otherLink.send(handshake(other, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(client.nodeId()) && storage.isConnected(other.nodeId()),
                "peers não conectaram");

        // Spoofed: announced on another peer's connection (both as the client and as the sender).
        otherLink.send(leave(client.nodeId(), client));
        otherLink.send(leave(other.nodeId(), client));
        // Announced on a socket that never identified itself with a handshake.
        RawPeer anonymous = raw(storage);
        anonymous.send(leave(client.nodeId(), client));
        Thread.sleep(400);
        assertTrue(knows(storage, client.nodeId()) && storage.isConnected(client.nodeId()),
                "LEAVE que não é de primeira mão não pode esquecer o peer");
        assertFalse(storage.isDeparted(client.nodeId()));

        clientLink.send(leave(client.nodeId(), client));
        awaitTrue(() -> !knows(storage, client.nodeId()), "o LEAVE de primeira mão deveria ser honrado");
        assertTrue(knows(storage, other.nodeId()));
    }

    /** A primeira recepção de um LEAVE é disseminada no PEER_UPDATE ({@code departed}), nunca repassada. */
    @Test
    void firstLeaveIsDisseminatedAsDepartedInPeerUpdate() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        NodeInfo client = info("z-client", freePort(), true);
        NodeInfo other = info("m-storage", freePort(), false);
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        RawPeer otherLink = raw(storage);
        otherLink.send(handshake(other, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()) && storage.isConnected(other.nodeId()),
                "peers não conectaram");

        clientLink.send(leave(client.nodeId(), client));

        awaitTrue(() -> otherLink.received().stream()
                        .filter(m -> m.type() == MessageType.PEER_UPDATE)
                        .map(m -> m.payload(PeerUpdatePayload.class))
                        .anyMatch(u -> u.departed().containsKey(client.nodeId())
                                && u.peers().stream().noneMatch(p -> p.nodeId().equals(client.nodeId()))),
                "os outros peers deveriam saber da saída pelo PEER_UPDATE");
        assertTrue(otherLink.received().stream().noneMatch(m -> m.type() == MessageType.LEAVE),
                "o LEAVE nunca é repassado");
    }

    /** Quem nunca alcançou o cliente que saiu também o esquece, pela notícia de segunda mão. */
    @Test
    void departureLearnedSecondHandForgetsAPeerKnownOnlyByGossip() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT)
                .connectTimeout(Duration.ofMillis(200)));
        RecordingListener events = listen(storage);
        NodeInfo client = info("z-client", freePort(), true); // nobody listens: never connected
        NodeInfo reporter = info("m-storage", freePort(), false);
        RawPeer reporterLink = raw(storage);
        reporterLink.send(handshake(reporter, storage.local(), Set.of(client)));
        awaitTrue(() -> knows(storage, client.nodeId()), "cliente não foi aprendido por gossip");

        reporterLink.send(peerUpdate(reporter, Map.of(client.nodeId(), 60_000L)));

        awaitTrue(() -> !knows(storage, client.nodeId()), "a saída de segunda mão deveria esquecer o cliente");
        assertTrue(storage.isDeparted(client.nodeId()));
        assertTrue(events.peerEvents(client.nodeId()).contains("left:z-client"));
    }

    /**
     * Notícia de saída atrasada: o mesmo id já religou (handshake direto) quando o PEER_UPDATE com
     * {@code departed} chega. Evidência de primeira mão vence: o peer segue conhecido e conectado.
     */
    @Test
    void delayedSecondHandDepartureDoesNotEvictAReconnectedPeer() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        NodeInfo client = info("z-client", freePort(), true);
        NodeInfo reporter = info("m-storage", freePort(), false);
        NodeInfo voter = info("n-storage", freePort(), false);
        RawPeer reporterLink = raw(storage);
        reporterLink.send(handshake(reporter, storage.local(), Set.of(voter)));
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()) && storage.isConnected(reporter.nodeId())
                && knows(storage, voter.nodeId()), "peers não conectaram");

        RecordingListener events = listen(storage);
        reporterLink.send(peerUpdate(reporter, Map.of(client.nodeId(), 60_000L, voter.nodeId(), 60_000L,
                storage.local().nodeId(), 60_000L)));
        reporterLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "after-update", reporter.nodeId(),
                storage.local().nodeId(), "x"));
        awaitTrue(() -> events.qualifiers().contains("after-update"), "PEER_UPDATE não foi processado");

        assertTrue(knows(storage, client.nodeId()) && storage.isConnected(client.nodeId()),
                "notícia de segunda mão não pode derrubar um peer com conexão handshaked");
        assertFalse(storage.isDeparted(client.nodeId()), "nem bloquear o id dele");
        assertTrue(knows(storage, voter.nodeId()), "um peer elegível a líder nunca é esquecido de segunda mão");
        assertFalse(storage.isDeparted(voter.nodeId()));
        assertFalse(storage.isDeparted(storage.local().nodeId()), "um nó ignora a própria saída reportada");
    }

    // ---- helpers ----

    static ClusterMessage peerUpdate(NodeInfo source, Map<NodeId, Long> departed) {
        return ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", source.nodeId(), null,
                new PeerUpdatePayload(Set.of(source), Map.of(), departed));
    }

    static NodeInfo info(String id, int port, boolean ephemeral) {
        Set<String> roles = ephemeral ? Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE) : Set.of("storage");
        return new NodeInfo(NodeId.of(id), "127.0.0.1", port, roles, 0);
    }

    static ClusterMessage handshake(NodeInfo self, NodeInfo target, Set<NodeInfo> peers) {
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", self.nodeId(), target.nodeId(),
                new HandshakePayload(self, peers, Map.of(), false, true, true));
    }

    /** Handshake of a node that predates LEAVE: the {@code supportsLeave} field is absent. */
    static ClusterMessage legacyHandshake(NodeInfo self, NodeInfo target) {
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", self.nodeId(), target.nodeId(),
                new HandshakePayload(self, Set.of(), Map.of(), false, true));
    }

    static ClusterMessage leave(NodeId source, NodeInfo announced) {
        return ClusterMessage.request(MessageType.LEAVE, "leave", source, null, new LeavePayload(announced, "test"));
    }

    /** Starts an ineligible client seeded with {@code seed} and waits until both handshakes completed. */
    private TcpTransport client(String id, int port, TcpTransport seed) throws InterruptedException {
        TcpTransport client = start(TcpTransportConfig.builder(info(id, port, true))
                .reconnectInterval(RECONNECT)
                .addPeer(seed.local()));
        awaitHandshaked(client, seed);
        return client;
    }

    /** Both sides processed the other's handshake on their tracked connection (LEAVE is negotiated). */
    static void awaitHandshaked(TcpTransport a, TcpTransport b) throws InterruptedException {
        awaitTrue(() -> a.announcesLeaveTo(b.local().nodeId()) && b.announcesLeaveTo(a.local().nodeId()),
                "handshake entre " + a.local().nodeId() + " e " + b.local().nodeId() + " não concluiu");
    }

    private TcpTransport start(TcpTransportConfig.Builder builder) {
        TcpTransport transport = new TcpTransport(builder.build());
        closeables.add(transport);
        transport.start();
        return transport;
    }

    private RawPeer raw(TcpTransport target) throws IOException {
        RawPeer peer = new RawPeer(target.local().host(), target.local().port());
        closeables.add(peer);
        return peer;
    }

    private static RecordingListener listen(TcpTransport transport) {
        RecordingListener listener = new RecordingListener();
        transport.addListener(listener);
        return listener;
    }

    private static AtomicInteger countDials(TcpTransport transport, NodeId target) {
        AtomicInteger dials = new AtomicInteger();
        transport.setBeforeDialHook(id -> {
            if (id.equals(target)) {
                dials.incrementAndGet();
            }
        });
        return dials;
    }

    static boolean knows(Transport transport, NodeId id) {
        return transport.peers().stream().anyMatch(p -> p.nodeId().equals(id));
    }

    static void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError(message);
    }

    private int freePort() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (usedPorts.add(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }

    static final class RecordingListener implements TransportListener {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final List<String> qualifiers = new CopyOnWriteArrayList<>();

        @Override
        public void onPeerConnected(NodeInfo peer) {
            events.add("connected:" + peer.nodeId().value());
        }

        @Override
        public void onPeerDisconnected(NodeId peerId) {
            events.add("disconnected:" + peerId.value());
        }

        @Override
        public void onPeerLeft(NodeId peerId) {
            events.add("left:" + peerId.value());
        }

        @Override
        public void onPeerLeaving(NodeId peerId) {
            events.add("leaving:" + peerId.value());
        }

        @Override
        public void onMessage(ClusterMessage message) {
            qualifiers.add(message.qualifier());
        }

        /** Connection-lifecycle events of {@code id} after its first connection. */
        List<String> peerEvents(NodeId id) {
            List<String> matching = new ArrayList<>();
            for (String event : events) {
                if (event.endsWith(":" + id.value()) && !event.startsWith("connected:")) {
                    matching.add(event);
                }
            }
            return matching;
        }

        List<String> qualifiers() {
            return qualifiers;
        }
    }
}
