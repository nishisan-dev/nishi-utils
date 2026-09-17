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

package dev.nishisan.utils.oss.cluster.api;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Configuração imutável de um {@link NgrrdClusterClient}: parâmetros de
 * membership do {@code NGridNode} subjacente (sem volume, papel
 * cliente/inelegível a líder) e das políticas de batching/retry do
 * {@code WriteDispatcher}. Construída via {@link #builder()}; a validação
 * acontece no construtor compacto, ou seja, em {@link Builder#build()}.
 *
 * @param clientId                  identificador do cliente no cluster NGrid
 * @param host                      endereço de bind do transporte
 * @param port                      porta de bind; {@code 0} = porta efêmera
 * @param seed                      endereço {@code host:port} de um nó semente para autodiscovery; {@code null} = nenhum
 * @param peers                     endereços {@code host:port} de peers explícitos; nunca {@code null} (vazio quando ausente)
 * @param dataDir                   diretório de dados do NGrid; {@code null} = criado como diretório
 *                                  temporário por {@code connect()}, e apagado no {@code close()} do cliente
 * @param batchMaxSamples           tamanho máximo de um lote de escrita por nó antes do flush automático
 * @param batchMaxDelay             tempo máximo de espera antes do flush automático de um nó com pendências
 * @param maxBufferedSamplesPerNode capacidade do buffer de escrita de cada nó de destino
 * @param bufferFullPolicy          política aplicada quando o buffer de um nó atinge a capacidade
 * @param requestTimeout            prazo de espera por resposta a um RPC síncrono do cluster
 * @param retryTimeout              prazo total de retentativa (ex.: série em migração) antes de desistir
 * @param retryBackoffMin           backoff mínimo entre retentativas
 * @param retryBackoffMax           teto do backoff exponencial entre retentativas
 * @param leaderWaitTimeout         prazo de espera por um líder eleito (conexão e resolução de placement)
 * @param closeTimeout              orçamento TOTAL (não por handle) para {@code close()} drenar os
 *                                  buffers de escrita antes de desistir; handles que não couberem no
 *                                  prazo fecham sem flush, e a amostra descartada é logada em ERROR
 */
public record NgrrdClusterConfig(
        String clientId,
        String host,
        int port,
        String seed,
        List<String> peers,
        Path dataDir,
        int batchMaxSamples,
        Duration batchMaxDelay,
        long maxBufferedSamplesPerNode,
        BufferFullPolicy bufferFullPolicy,
        Duration requestTimeout,
        Duration retryTimeout,
        Duration retryBackoffMin,
        Duration retryBackoffMax,
        Duration leaderWaitTimeout,
        Duration closeTimeout) {

    public NgrrdClusterConfig {
        Objects.requireNonNull(clientId, "clientId é obrigatório");
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId não pode ser vazio");
        }
        Objects.requireNonNull(host, "host é obrigatório");
        if (port < 0) {
            throw new IllegalArgumentException("port deve ser >= 0 (0 = efêmera): " + port);
        }
        peers = List.copyOf(Objects.requireNonNullElse(peers, List.of()));
        if ((seed == null || seed.isBlank()) && peers.isEmpty()) {
            throw new IllegalArgumentException("ao menos um de seed/peers é obrigatório");
        }
        if (batchMaxSamples <= 0) {
            throw new IllegalArgumentException("batchMaxSamples deve ser > 0: " + batchMaxSamples);
        }
        Objects.requireNonNull(batchMaxDelay, "batchMaxDelay é obrigatório");
        if (batchMaxDelay.isNegative() || batchMaxDelay.isZero()) {
            throw new IllegalArgumentException("batchMaxDelay deve ser > 0");
        }
        if (maxBufferedSamplesPerNode <= 0) {
            throw new IllegalArgumentException("maxBufferedSamplesPerNode deve ser > 0: " + maxBufferedSamplesPerNode);
        }
        Objects.requireNonNull(bufferFullPolicy, "bufferFullPolicy é obrigatório");
        Objects.requireNonNull(requestTimeout, "requestTimeout é obrigatório");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout deve ser > 0");
        }
        Objects.requireNonNull(retryTimeout, "retryTimeout é obrigatório");
        if (retryTimeout.isNegative() || retryTimeout.isZero()) {
            throw new IllegalArgumentException("retryTimeout deve ser > 0");
        }
        Objects.requireNonNull(retryBackoffMin, "retryBackoffMin é obrigatório");
        if (retryBackoffMin.isNegative() || retryBackoffMin.isZero()) {
            throw new IllegalArgumentException("retryBackoffMin deve ser > 0");
        }
        Objects.requireNonNull(retryBackoffMax, "retryBackoffMax é obrigatório");
        if (retryBackoffMax.compareTo(retryBackoffMin) < 0) {
            throw new IllegalArgumentException("retryBackoffMax deve ser >= retryBackoffMin");
        }
        Objects.requireNonNull(leaderWaitTimeout, "leaderWaitTimeout é obrigatório");
        if (leaderWaitTimeout.isNegative() || leaderWaitTimeout.isZero()) {
            throw new IllegalArgumentException("leaderWaitTimeout deve ser > 0");
        }
        Objects.requireNonNull(closeTimeout, "closeTimeout é obrigatório");
        if (closeTimeout.isNegative() || closeTimeout.isZero()) {
            throw new IllegalArgumentException("closeTimeout deve ser > 0");
        }
    }

    /** Política aplicada quando o buffer de escrita de um nó atinge a capacidade configurada. */
    public enum BufferFullPolicy {
        /** Bloqueia o chamador de {@code write} até que o flush libere espaço. */
        BLOCK,
        /** Lança {@link NgrrdClusterException} com {@link ErrorCode#BUFFER_FULL} imediatamente. */
        FAIL
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String defaultClientId() {
        return "ngrrd-client-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Builder fluente de {@link NgrrdClusterConfig}. */
    public static final class Builder {

        private String clientId = defaultClientId();
        private String host = "127.0.0.1";
        private int port = 0;
        private String seed;
        private List<String> peers = List.of();
        private Path dataDir;
        private int batchMaxSamples = 500;
        private Duration batchMaxDelay = Duration.ofMillis(200);
        private long maxBufferedSamplesPerNode = 100_000L;
        private BufferFullPolicy bufferFullPolicy = BufferFullPolicy.BLOCK;
        private Duration requestTimeout = Duration.ofSeconds(20);
        private Duration retryTimeout = Duration.ofMinutes(5);
        private Duration retryBackoffMin = Duration.ofMillis(100);
        private Duration retryBackoffMax = Duration.ofSeconds(2);
        private Duration leaderWaitTimeout = Duration.ofSeconds(30);
        private Duration closeTimeout = Duration.ofSeconds(30);

        private Builder() {
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder seed(String seed) {
            this.seed = seed;
            return this;
        }

        public Builder peers(List<String> peers) {
            this.peers = peers;
            return this;
        }

        public Builder peers(String... peers) {
            this.peers = List.of(peers);
            return this;
        }

        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder batchMaxSamples(int batchMaxSamples) {
            this.batchMaxSamples = batchMaxSamples;
            return this;
        }

        public Builder batchMaxDelay(Duration batchMaxDelay) {
            this.batchMaxDelay = batchMaxDelay;
            return this;
        }

        public Builder maxBufferedSamplesPerNode(long maxBufferedSamplesPerNode) {
            this.maxBufferedSamplesPerNode = maxBufferedSamplesPerNode;
            return this;
        }

        public Builder bufferFullPolicy(BufferFullPolicy bufferFullPolicy) {
            this.bufferFullPolicy = bufferFullPolicy;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder retryTimeout(Duration retryTimeout) {
            this.retryTimeout = retryTimeout;
            return this;
        }

        public Builder retryBackoffMin(Duration retryBackoffMin) {
            this.retryBackoffMin = retryBackoffMin;
            return this;
        }

        public Builder retryBackoffMax(Duration retryBackoffMax) {
            this.retryBackoffMax = retryBackoffMax;
            return this;
        }

        public Builder leaderWaitTimeout(Duration leaderWaitTimeout) {
            this.leaderWaitTimeout = leaderWaitTimeout;
            return this;
        }

        /** Orçamento TOTAL de {@code close()} (compartilhado entre todos os handles), não por handle. */
        public Builder closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public NgrrdClusterConfig build() {
            return new NgrrdClusterConfig(clientId, host, port, seed, peers, dataDir, batchMaxSamples,
                    batchMaxDelay, maxBufferedSamplesPerNode, bufferFullPolicy, requestTimeout, retryTimeout,
                    retryBackoffMin, retryBackoffMax, leaderWaitTimeout, closeTimeout);
        }
    }
}
