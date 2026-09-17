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

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.blob.BlobVolumeConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Configuração imutável de um {@link NgrrdStorageNode}: parâmetros do
 * {@code NGridNode} subjacente (membership) e do {@code BlobVolume} local
 * (armazenamento das séries). Construída via {@link #builder()}; a validação
 * acontece no construtor compacto, ou seja, em {@link Builder#build()}.
 *
 * @param nodeId                    identificador do nó no cluster NGrid
 * @param host                      endereço de bind do transporte
 * @param port                      porta de bind; {@code 0} = porta efêmera
 * @param seed                       endereço {@code host:port} de um nó semente para autodiscovery; {@code null} = nenhum
 * @param peers                     endereços {@code host:port} de peers explícitos; nunca {@code null} (vazio quando ausente)
 * @param dataDir                   diretório de dados do NGrid (WAL/snapshots das estruturas distribuídas)
 * @param priority                  prioridade de liderança (afinidade) deste nó
 * @param volumeDir                 diretório base do blob volume local
 * @param volumeName                nome lógico do blob volume
 * @param shardCount                número de shards do volume
 * @param segmentBytes              tamanho do segmento mmap de cada shard
 * @param initialShardCapacityBytes capacidade inicial (sparse) de cada shard
 * @param capacityBytes             capacidade total declarada do nó, em bytes; {@code <= 0} = desconhecida
 * @param statusReportInterval      intervalo entre publicações de {@code StorageNodeStatus}
 * @param handleIdleTtl             tempo de ociosidade após o qual um handle de série é fechado
 * @param maxOpenHandles            número máximo de handles de série simultaneamente abertos
 * @param requestTimeout            prazo de espera por resposta a um RPC do cluster
 * @param defaultDurability         durabilidade default aplicada quando o pedido de abertura não especifica uma
 * @param defaultOnGeometryChange   política de mudança de geometria default quando o pedido não especifica uma
 */
public record StorageNodeConfig(
        String nodeId,
        String host,
        int port,
        String seed,
        List<String> peers,
        Path dataDir,
        int priority,
        Path volumeDir,
        String volumeName,
        int shardCount,
        long segmentBytes,
        long initialShardCapacityBytes,
        long capacityBytes,
        Duration statusReportInterval,
        Duration handleIdleTtl,
        int maxOpenHandles,
        Duration requestTimeout,
        Durability defaultDurability,
        OnGeometryChange defaultOnGeometryChange) {

    public StorageNodeConfig {
        Objects.requireNonNull(nodeId, "nodeId é obrigatório");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId não pode ser vazio");
        }
        Objects.requireNonNull(host, "host é obrigatório");
        if (port < 0) {
            throw new IllegalArgumentException("port deve ser >= 0 (0 = efêmera): " + port);
        }
        peers = List.copyOf(Objects.requireNonNullElse(peers, List.of()));
        Objects.requireNonNull(dataDir, "dataDir é obrigatório");
        Objects.requireNonNull(volumeDir, "volumeDir é obrigatório");
        Objects.requireNonNull(volumeName, "volumeName é obrigatório");
        if (volumeName.isBlank()) {
            throw new IllegalArgumentException("volumeName não pode ser vazio");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount deve ser > 0: " + shardCount);
        }
        if (segmentBytes <= 0) {
            throw new IllegalArgumentException("segmentBytes deve ser > 0: " + segmentBytes);
        }
        if (initialShardCapacityBytes <= 0) {
            throw new IllegalArgumentException("initialShardCapacityBytes deve ser > 0: " + initialShardCapacityBytes);
        }
        Objects.requireNonNull(statusReportInterval, "statusReportInterval é obrigatório");
        if (statusReportInterval.isNegative() || statusReportInterval.isZero()) {
            throw new IllegalArgumentException("statusReportInterval deve ser > 0");
        }
        Objects.requireNonNull(handleIdleTtl, "handleIdleTtl é obrigatório");
        if (handleIdleTtl.isNegative() || handleIdleTtl.isZero()) {
            throw new IllegalArgumentException("handleIdleTtl deve ser > 0");
        }
        if (maxOpenHandles <= 0) {
            throw new IllegalArgumentException("maxOpenHandles deve ser > 0: " + maxOpenHandles);
        }
        Objects.requireNonNull(requestTimeout, "requestTimeout é obrigatório");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout deve ser > 0");
        }
        Objects.requireNonNull(defaultDurability, "defaultDurability é obrigatório");
        Objects.requireNonNull(defaultOnGeometryChange, "defaultOnGeometryChange é obrigatório");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder fluente de {@link StorageNodeConfig}. */
    public static final class Builder {

        private String nodeId;
        private String host = "127.0.0.1";
        private int port = 0;
        private String seed;
        private List<String> peers = List.of();
        private Path dataDir;
        private int priority = 100;
        private Path volumeDir;
        private String volumeName = "ngrrd";
        private int shardCount = BlobVolumeConfig.DEFAULT_SHARD_COUNT;
        private long segmentBytes = BlobVolumeConfig.DEFAULT_SEGMENT_BYTES;
        private long initialShardCapacityBytes = BlobVolumeConfig.DEFAULT_SEGMENT_BYTES;
        private long capacityBytes = 0L;
        private Duration statusReportInterval = Duration.ofSeconds(10);
        private Duration handleIdleTtl = Duration.ofMinutes(15);
        private int maxOpenHandles = 10_000;
        private Duration requestTimeout = Duration.ofSeconds(20);
        private Durability defaultDurability = Durability.FSYNC;
        private OnGeometryChange defaultOnGeometryChange = OnGeometryChange.FAIL;

        private Builder() {
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
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

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder volumeDir(Path volumeDir) {
            this.volumeDir = volumeDir;
            return this;
        }

        public Builder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public Builder shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder segmentBytes(long segmentBytes) {
            this.segmentBytes = segmentBytes;
            return this;
        }

        public Builder initialShardCapacityBytes(long initialShardCapacityBytes) {
            this.initialShardCapacityBytes = initialShardCapacityBytes;
            return this;
        }

        public Builder capacityBytes(long capacityBytes) {
            this.capacityBytes = capacityBytes;
            return this;
        }

        public Builder statusReportInterval(Duration statusReportInterval) {
            this.statusReportInterval = statusReportInterval;
            return this;
        }

        public Builder handleIdleTtl(Duration handleIdleTtl) {
            this.handleIdleTtl = handleIdleTtl;
            return this;
        }

        public Builder maxOpenHandles(int maxOpenHandles) {
            this.maxOpenHandles = maxOpenHandles;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder defaultDurability(Durability defaultDurability) {
            this.defaultDurability = defaultDurability;
            return this;
        }

        public Builder defaultOnGeometryChange(OnGeometryChange defaultOnGeometryChange) {
            this.defaultOnGeometryChange = defaultOnGeometryChange;
            return this;
        }

        public StorageNodeConfig build() {
            return new StorageNodeConfig(nodeId, host, port, seed, peers, dataDir, priority, volumeDir,
                    volumeName, shardCount, segmentBytes, initialShardCapacityBytes, capacityBytes,
                    statusReportInterval, handleIdleTtl, maxOpenHandles, requestTimeout,
                    defaultDurability, defaultOnGeometryChange);
        }
    }
}
