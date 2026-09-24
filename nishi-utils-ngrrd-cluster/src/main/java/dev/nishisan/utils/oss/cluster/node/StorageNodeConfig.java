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

import dev.nishisan.utils.oss.cluster.placement.DistributionMode;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.blob.BlobVolumeConfig;
import dev.nishisan.utils.oss.cluster.config.NgrrdYamlSupport;
import dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener;
import dev.nishisan.utils.oss.definition.ObjectNaming;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Configuração imutável de um {@link NgrrdStorageNode}: parâmetros do
 * {@code NGridNode} subjacente (membership) e do {@code BlobVolume} local
 * (armazenamento das séries). Construída via {@link #builder()}; a validação
 * acontece no construtor compacto, ou seja, em {@link Builder#build()}.
 *
 * <p><b>Dimensionamento do cluster.</b> A liderança do NGrid exige maioria entre os
 * membros elegíveis a líder (os storage nodes; clientes {@code leader-ineligible} não
 * contam). Com apenas <b>2</b> storage nodes, a queda de um deles deixa o cluster sem
 * maioria e, portanto, sem líder: os comandos que dependem do líder (placement de séries
 * novas, {@code admin.status}, {@code admin.drain}/{@code activate}, rebalanceamento) param
 * até o nó voltar — {@code admin.metrics} não entra nessa lista: é atendido por qualquer
 * storage node, líder ou não, com métricas puramente locais. Escrita e leitura nas séries
 * do nó sobrevivente continuam funcionando. Tolerar
 * a falha de um nó sem indisponibilidade de coordenação exige <b>≥ 3</b> storage nodes.
 * O {@code bootDiscoveryWindow} (default 3 s) faz um nó recém-iniciado esperar por seus
 * peers configurados antes de se eleger, evitando que um nó que volta atrasado assuma a
 * liderança com estado desatualizado.</p>
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
 * @param nodeStatusStaleAfter      prazo completo após o qual o líder considera "velho" o último
 *                                  status reportado por um nó, para fins de placement
 *                                  ({@code LeastLoadedPlacementPolicy}) — default
 *                                  {@code max(5 × statusReportInterval, 15s)}, generoso o bastante
 *                                  para sobreviver a um handoff de liderança sem descartar nós ativos
 * @param handleIdleTtl             tempo de ociosidade após o qual um handle de série é fechado
 * @param maxOpenHandles            número máximo de handles de série simultaneamente abertos
 * @param requestTimeout            prazo de espera por resposta a um RPC do cluster
 * @param defaultDurability         durabilidade default aplicada quando o pedido de abertura não especifica uma
 * @param defaultOnGeometryChange   política de mudança de geometria default quando o pedido não especifica uma
 * @param metricsListener           integração opcional de métricas (ver {@link NgrrdClusterMetricsListener});
 *                                  {@code null} = nenhuma (só o log marker {@code NGRRD_NODE_STATUS})
 * @param affinityHandbackMode      liga o handback orquestrado de liderança do NGrid (D11), repassado ao
 *                                  {@code NGridNodeBuilder} (default {@code true}): um nó de maior afinidade
 *                                  que volta à malha NUNCA reassume a liderança pelo gate de watermark —
 *                                  sob carga, esse handoff sobrepõe dois líderes produzindo e o D10c
 *                                  descarta a cauda do perdedor, o que reverteu flips do catálogo já
 *                                  confirmados (imagem migrada e apagada na origem, catálogo apontando
 *                                  para a origem → série recriada vazia); com o handback, o incumbente
 *                                  congela a produção, entrega um snapshot e só então cede. Custo aceito
 *                                  pelo default {@code true}: se o handback é abortado (o candidato cai,
 *                                  o snapshot não fecha a tempo, etc.), o core aplica um cooldown de 60 s
 *                                  ({@code reclaimQuiesceCooldown}) antes de deixar outro handback ser
 *                                  tentado — um nó de maior afinidade que reinicia pode passar até esse
 *                                  período inteiro sem conseguir assumir a liderança de volta, mesmo já
 *                                  saudável e alcançável (a malha continua servindo normalmente pelo
 *                                  incumbente atual nesse meio-tempo; só a REASSUNÇÃO de liderança fica
 *                                  represada). Aceito porque o alternativo — reclaim imediato por
 *                                  watermark — é exatamente o cenário de perda de dados acima.
 * @param bootDiscoveryWindow       janela de boot-discovery repassada ao {@code NGridNodeBuilder}
 *                                  ({@code NGridConfig.Builder#bootDiscoveryWindow}): um nó recém-subido
 *                                  adia a auto-eleição por essa janela enquanto descobre peers e seus
 *                                  watermarks de replicação antes de decidir se reivindica a liderança —
 *                                  mitiga a deferência mútua a três (D9/D10c) observada em
 *                                  {@code RebalanceClusterTest}/{@code PlacementUnderLeaderChurnClusterTest}
 *                                  quando um 3º nó entra na malha
 * @param placementGraceAfterLeadership janela após este nó assumir a liderança durante a qual
 *                                  {@code PlacementRequestHandler} recusa criar placements NOVOS
 *                                  (responde {@code NOT_LEADER}, o cliente retenta) — dá tempo da
 *                                  réplica local do catálogo convergir antes de decidir sobre séries
 *                                  que já existem (ver seção 0 da spec do M3)
 * @param rebalanceEnabled          liga/desliga o agendamento automático do {@code Rebalancer}
 *                                  ({@code ngrrd.admin.rebalance} continua funcionando mesmo desligado)
 * @param rebalanceInterval         intervalo entre ciclos automáticos de rebalanceamento
 * @param rebalanceMinDelta         diferença mínima absoluta de séries entre o nó mais e o menos
 *                                  carregado para disparar um movimento
 * @param rebalanceTolerance        diferença mínima relativa (fração da média) entre o nó mais e o
 *                                  menos carregado para disparar um movimento
 * @param maxConcurrentMigrations   migrações em curso simultaneamente, no cluster inteiro, permitidas
 *                                  pelo {@code MigrationCoordinator}
 * @param maxMovesPerCycle          teto de movimentos planejados por ciclo de rebalanceamento
 * @param migrationTimeout          prazo total de uma migração (do {@code MIGRATE_START} até
 *                                  {@code COMMITTED} no destino) antes do coordenador desistir e abortar
 * @param migrationChunkBytes       tamanho de cada {@code MIGRATE_CHUNK} enviado pela origem
 * @param maxSeriesBytes            tamanho máximo de imagem de série aceito para migração
 * @param migrationStatusPollInterval intervalo entre consultas {@code MIGRATE_STATUS} ao destino
 * @param reconcileInterval        intervalo entre ciclos automáticos do {@code LocalReconciler}
 * @param orphanGrace              prazo mínimo (desde {@code SeriesPlacement#updatedAtEpochMs}) antes do
 *                                 {@code LocalReconciler} apagar uma cópia local órfã (placement
 *                                 {@code ACTIVE} confirmado noutro dono) — evita apagar durante um
 *                                 {@code MIGRATE_FINISH} atrasado
 * @param seriesObjectPrefix       prefixo do objeto único da série no {@code BlobStorage}
 *                                 ({@code {prefix}/{seriesKey}.ngrr}), usado pelo {@code LocalReconciler}
 *                                 para listar/decodificar chaves do volume e pelo
 *                                 {@code StorageRequestHandler} para validar, no {@code OPEN}, que a
 *                                 definição da série usa o mesmo prefixo. Default: o default do próprio
 *                                 {@code nishi-utils-oss} ({@code ObjectNaming.seriesPrefixOrDefault()},
 *                                 obtido programaticamente, nunca hardcoded aqui). <b>Todas as definições
 *                                 servidas por um mesmo cluster ngrrd devem declarar (ou herdar por
 *                                 omissão) o mesmo {@code storage.objectNaming.seriesPrefix}</b> — o
 *                                 {@code LocalReconciler} não tem como saber, a partir só da chave física
 *                                 no volume, qual prefixo uma definição customizada usaria antes de a
 *                                 abrir; um {@code OPEN} com prefixo divergente do configurado neste nó é
 *                                 rejeitado com {@code SeriesStatus#ERROR}.
 * @param distributionMode strategy shared by placement and rebalance; defaults to COUNT
 * @param weight positive finite relative weight used in WEIGHT mode
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
        Duration nodeStatusStaleAfter,
        Duration handleIdleTtl,
        int maxOpenHandles,
        Duration requestTimeout,
        Durability defaultDurability,
        OnGeometryChange defaultOnGeometryChange,
        NgrrdClusterMetricsListener metricsListener,
        Duration bootDiscoveryWindow,
        boolean affinityHandbackMode,
        Duration placementGraceAfterLeadership,
        boolean rebalanceEnabled,
        Duration rebalanceInterval,
        long rebalanceMinDelta,
        double rebalanceTolerance,
        int maxConcurrentMigrations,
        int maxMovesPerCycle,
        Duration migrationTimeout,
        long migrationChunkBytes,
        long maxSeriesBytes,
        Duration migrationStatusPollInterval,
        Duration reconcileInterval,
        Duration orphanGrace,
        String seriesObjectPrefix,
        DistributionMode distributionMode,
        double weight) {

    /** Compatibility constructor with equal-count distribution. */
    public StorageNodeConfig(
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
        Duration nodeStatusStaleAfter,
        Duration handleIdleTtl,
        int maxOpenHandles,
        Duration requestTimeout,
        Durability defaultDurability,
        OnGeometryChange defaultOnGeometryChange,
        NgrrdClusterMetricsListener metricsListener,
        Duration bootDiscoveryWindow,
        boolean affinityHandbackMode,
        Duration placementGraceAfterLeadership,
        boolean rebalanceEnabled,
        Duration rebalanceInterval,
        long rebalanceMinDelta,
        double rebalanceTolerance,
        int maxConcurrentMigrations,
        int maxMovesPerCycle,
        Duration migrationTimeout,
        long migrationChunkBytes,
        long maxSeriesBytes,
        Duration migrationStatusPollInterval,
        Duration reconcileInterval,
        Duration orphanGrace,
        String seriesObjectPrefix) {
        this(nodeId, host, port, seed, peers, dataDir, priority, volumeDir, volumeName, shardCount, segmentBytes,
                initialShardCapacityBytes, capacityBytes, statusReportInterval, nodeStatusStaleAfter,
                handleIdleTtl, maxOpenHandles, requestTimeout, defaultDurability, defaultOnGeometryChange,
                metricsListener, bootDiscoveryWindow, affinityHandbackMode, placementGraceAfterLeadership,
                rebalanceEnabled, rebalanceInterval, rebalanceMinDelta, rebalanceTolerance,
                maxConcurrentMigrations, maxMovesPerCycle, migrationTimeout, migrationChunkBytes, maxSeriesBytes,
                migrationStatusPollInterval, reconcileInterval, orphanGrace, seriesObjectPrefix,
                DistributionMode.COUNT, 1.0);
    }

    public StorageNodeConfig {
        Objects.requireNonNull(distributionMode, "distributionMode");
        if (!Double.isFinite(weight) || weight <= 0) {
            throw new IllegalArgumentException("weight must be positive and finite");
        }
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
        Objects.requireNonNull(nodeStatusStaleAfter, "nodeStatusStaleAfter é obrigatório");
        if (nodeStatusStaleAfter.isNegative() || nodeStatusStaleAfter.isZero()) {
            throw new IllegalArgumentException("nodeStatusStaleAfter deve ser > 0");
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
        Objects.requireNonNull(bootDiscoveryWindow, "bootDiscoveryWindow é obrigatório");
        if (bootDiscoveryWindow.isNegative()) {
            throw new IllegalArgumentException("bootDiscoveryWindow deve ser >= 0");
        }
        Objects.requireNonNull(placementGraceAfterLeadership, "placementGraceAfterLeadership é obrigatório");
        if (placementGraceAfterLeadership.isNegative()) {
            throw new IllegalArgumentException("placementGraceAfterLeadership deve ser >= 0");
        }
        Objects.requireNonNull(rebalanceInterval, "rebalanceInterval é obrigatório");
        if (rebalanceInterval.isNegative() || rebalanceInterval.isZero()) {
            throw new IllegalArgumentException("rebalanceInterval deve ser > 0");
        }
        if (rebalanceMinDelta < 0) {
            throw new IllegalArgumentException("rebalanceMinDelta deve ser >= 0: " + rebalanceMinDelta);
        }
        if (!Double.isFinite(rebalanceTolerance) || rebalanceTolerance < 0) {
            throw new IllegalArgumentException("rebalanceTolerance deve ser >= 0: " + rebalanceTolerance);
        }
        if (maxConcurrentMigrations <= 0) {
            throw new IllegalArgumentException("maxConcurrentMigrations deve ser > 0: " + maxConcurrentMigrations);
        }
        if (maxMovesPerCycle <= 0) {
            throw new IllegalArgumentException("maxMovesPerCycle deve ser > 0: " + maxMovesPerCycle);
        }
        Objects.requireNonNull(migrationTimeout, "migrationTimeout é obrigatório");
        if (migrationTimeout.isNegative() || migrationTimeout.isZero()) {
            throw new IllegalArgumentException("migrationTimeout deve ser > 0");
        }
        if (migrationChunkBytes <= 0) {
            throw new IllegalArgumentException("migrationChunkBytes deve ser > 0: " + migrationChunkBytes);
        }
        if (maxSeriesBytes <= 0) {
            throw new IllegalArgumentException("maxSeriesBytes deve ser > 0: " + maxSeriesBytes);
        }
        Objects.requireNonNull(migrationStatusPollInterval, "migrationStatusPollInterval é obrigatório");
        if (migrationStatusPollInterval.isNegative() || migrationStatusPollInterval.isZero()) {
            throw new IllegalArgumentException("migrationStatusPollInterval deve ser > 0");
        }
        Objects.requireNonNull(reconcileInterval, "reconcileInterval é obrigatório");
        if (reconcileInterval.isNegative() || reconcileInterval.isZero()) {
            throw new IllegalArgumentException("reconcileInterval deve ser > 0");
        }
        Objects.requireNonNull(orphanGrace, "orphanGrace é obrigatório");
        if (orphanGrace.isNegative()) {
            throw new IllegalArgumentException("orphanGrace deve ser >= 0");
        }
        Objects.requireNonNull(seriesObjectPrefix, "seriesObjectPrefix é obrigatório");
        // BAIXO-D do Refuter: normaliza (remove barras iniciais/finais) já aqui — "series", "series/" e
        // "/series/" são o mesmo prefixo; reaproveita a mesma normalização de SeriesObjectKeys (mesmo
        // pacote), em vez de duplicar a lógica.
        seriesObjectPrefix = SeriesObjectKeys.normalize(seriesObjectPrefix);
        if (seriesObjectPrefix.isBlank()) {
            throw new IllegalArgumentException("seriesObjectPrefix não pode ser vazio");
        }
    }

    /**
     * Default de {@code seriesObjectPrefix}: o default do próprio {@code nishi-utils-oss}
     * ({@link ObjectNaming#seriesPrefixOrDefault()}), obtido programaticamente — nunca um literal
     * duplicado aqui, para nunca divergir se o default de lá mudar.
     */
    private static final String DEFAULT_SERIES_OBJECT_PREFIX = new ObjectNaming(null, null, null)
            .seriesPrefixOrDefault();

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Carrega a configuração de um storage node a partir de um arquivo YAML, com interpolação
     * {@code ${VAR}}/{@code ${VAR:default}} via {@code envResolver} (tipicamente {@code System::getenv}).
     * Ver o Javadoc da seção 3 da spec do M4 para o esquema completo esperado.
     *
     * @throws java.io.UncheckedIOException se o arquivo não puder ser lido
     * @throws IllegalArgumentException      se o YAML for inválido ou algum campo obrigatório estiver ausente
     */
    public static StorageNodeConfig fromYaml(Path path, Function<String, String> envResolver) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(envResolver, "envResolver");
        String raw = Files.readString(path, StandardCharsets.UTF_8);
        return fromYaml(raw, envResolver);
    }

    /** Como {@link #fromYaml(Path, Function)}, mas a partir do texto YAML já em memória. */
    public static StorageNodeConfig fromYaml(String yaml, Function<String, String> envResolver) {
        Objects.requireNonNull(yaml, "yaml");
        Objects.requireNonNull(envResolver, "envResolver");
        String interpolated = NgrrdYamlSupport.interpolate(yaml, envResolver);
        YamlModel model;
        try {
            model = NgrrdYamlSupport.mapper().readValue(interpolated, YamlModel.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "YAML de configuração do storage node inválido: " + e.getOriginalMessage(), e);
        }
        return model.toConfig();
    }

    // ---------------------------------------------------------------- YAML DTOs (fromYaml)

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class YamlModel {
        public NodeSection node;
        public NgrrdSection ngrrd;

        StorageNodeConfig toConfig() {
            require(node != null, "seção 'node' é obrigatória");
            require(node.id != null && !node.id.isBlank(), "node.id é obrigatório");
            require(node.host != null && !node.host.isBlank(), "node.host é obrigatório");
            require(node.port != null, "node.port é obrigatório");
            require(node.dataDir != null && !node.dataDir.isBlank(), "node.dataDir é obrigatório");
            require(ngrrd != null && ngrrd.volume != null, "seção 'ngrrd.volume' é obrigatória");
            VolumeSection volume = ngrrd.volume;
            require(volume.dir != null && !volume.dir.isBlank(), "ngrrd.volume.dir é obrigatório");
            require(volume.name != null && !volume.name.isBlank(), "ngrrd.volume.name é obrigatório");

            Builder builder = builder()
                    .nodeId(node.id)
                    .host(node.host)
                    .port(node.port)
                    .dataDir(Path.of(node.dataDir))
                    .volumeDir(Path.of(volume.dir))
                    .volumeName(volume.name);
            if (node.priority != null) {
                builder.priority(node.priority);
            }
            if (node.seed != null && !node.seed.isBlank()) {
                builder.seed(node.seed);
            }
            if (node.peers != null) {
                builder.peers(node.peers);
            }
            if (volume.shardCount != null) {
                builder.shardCount(volume.shardCount);
            }
            if (volume.segmentBytes != null) {
                builder.segmentBytes(volume.segmentBytes);
            }
            if (volume.initialShardCapacityBytes != null) {
                builder.initialShardCapacityBytes(volume.initialShardCapacityBytes);
            }
            if (volume.capacityBytes != null) {
                builder.capacityBytes(volume.capacityBytes);
            }
            applyDuration(ngrrd.statusReportInterval, "ngrrd.statusReportInterval", builder::statusReportInterval);
            applyDuration(ngrrd.nodeStatusStaleAfter, "ngrrd.nodeStatusStaleAfter", builder::nodeStatusStaleAfter);
            applyDuration(ngrrd.handleIdleTtl, "ngrrd.handleIdleTtl", builder::handleIdleTtl);
            if (ngrrd.maxOpenHandles != null) {
                builder.maxOpenHandles(ngrrd.maxOpenHandles);
            }
            applyDuration(ngrrd.requestTimeout, "ngrrd.requestTimeout", builder::requestTimeout);
            if (ngrrd.defaultDurability != null) {
                builder.defaultDurability(Durability.from(ngrrd.defaultDurability));
            }
            if (ngrrd.defaultOnGeometryChange != null) {
                builder.defaultOnGeometryChange(OnGeometryChange.from(ngrrd.defaultOnGeometryChange));
            }
            if (ngrrd.seriesObjectPrefix != null && !ngrrd.seriesObjectPrefix.isBlank()) {
                builder.seriesObjectPrefix(ngrrd.seriesObjectPrefix);
            }
            if (ngrrd.distribution != null && ngrrd.distribution.mode != null) {
                builder.distributionMode(ngrrd.distribution.mode);
            }
            if (ngrrd.weight != null) { builder.weight(ngrrd.weight); }
            RebalanceSection rebalance = ngrrd.rebalance;
            if (rebalance != null) {
                if (rebalance.enabled != null) {
                    builder.rebalanceEnabled(rebalance.enabled);
                }
                applyDuration(rebalance.interval, "ngrrd.rebalance.interval", builder::rebalanceInterval);
                if (rebalance.minDelta != null) {
                    builder.rebalanceMinDelta(rebalance.minDelta);
                }
                if (rebalance.tolerance != null) {
                    builder.rebalanceTolerance(rebalance.tolerance);
                }
                if (rebalance.maxConcurrentMigrations != null) {
                    builder.maxConcurrentMigrations(rebalance.maxConcurrentMigrations);
                }
                if (rebalance.maxMovesPerCycle != null) {
                    builder.maxMovesPerCycle(rebalance.maxMovesPerCycle);
                }
                applyDuration(rebalance.migrationTimeout, "ngrrd.rebalance.migrationTimeout",
                        builder::migrationTimeout);
                if (rebalance.chunkBytes != null) {
                    builder.migrationChunkBytes(rebalance.chunkBytes);
                }
                if (rebalance.maxSeriesBytes != null) {
                    builder.maxSeriesBytes(rebalance.maxSeriesBytes);
                }
            }
            ReconcileSection reconcile = ngrrd.reconcile;
            if (reconcile != null) {
                applyDuration(reconcile.interval, "ngrrd.reconcile.interval", builder::reconcileInterval);
                applyDuration(reconcile.orphanGrace, "ngrrd.reconcile.orphanGrace", builder::orphanGrace);
            }
            return builder.build();
        }

        private static void applyDuration(String raw, String fieldName, java.util.function.Consumer<Duration> setter) {
            Duration parsed = NgrrdYamlSupport.duration(raw, fieldName);
            if (parsed != null) {
                setter.accept(parsed);
            }
        }

        private static void require(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException(message);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class NodeSection {
        public String id;
        public String host;
        public Integer port;
        public Integer priority;
        public String dataDir;
        public String seed;
        public List<String> peers;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class NgrrdSection {
        public VolumeSection volume;
        public String statusReportInterval;
        public String nodeStatusStaleAfter;
        public String handleIdleTtl;
        public Integer maxOpenHandles;
        public String requestTimeout;
        public String defaultDurability;
        public String defaultOnGeometryChange;
        public String seriesObjectPrefix;
        public RebalanceSection rebalance;
        public DistributionSection distribution;
        public Double weight;
        public ReconcileSection reconcile;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class VolumeSection {
        public String dir;
        public String name;
        public Integer shardCount;
        public Long segmentBytes;
        public Long initialShardCapacityBytes;
        public Long capacityBytes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class DistributionSection {
        public DistributionMode mode;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class RebalanceSection {
        public Boolean enabled;
        public String interval;
        public Long minDelta;
        public Double tolerance;
        public Integer maxConcurrentMigrations;
        public Integer maxMovesPerCycle;
        public String migrationTimeout;
        public Long chunkBytes;
        public Long maxSeriesBytes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class ReconcileSection {
        public String interval;
        public String orphanGrace;
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
        private DistributionMode distributionMode = DistributionMode.COUNT;
        private double weight = 1.0;
        private Duration statusReportInterval = Duration.ofSeconds(10);
        /** {@code null} = calculado em {@link #build()} a partir de {@link #statusReportInterval}. */
        private Duration nodeStatusStaleAfter;
        private Duration handleIdleTtl = Duration.ofMinutes(15);
        private int maxOpenHandles = 10_000;
        private Duration requestTimeout = Duration.ofSeconds(20);
        private Durability defaultDurability = Durability.FSYNC;
        private OnGeometryChange defaultOnGeometryChange = OnGeometryChange.FAIL;
        private NgrrdClusterMetricsListener metricsListener;
        private Duration bootDiscoveryWindow = Duration.ofSeconds(3);
        private boolean affinityHandbackMode = true;
        private Duration placementGraceAfterLeadership = Duration.ofSeconds(3);
        private boolean rebalanceEnabled = true;
        private Duration rebalanceInterval = Duration.ofSeconds(60);
        private long rebalanceMinDelta = 50L;
        private double rebalanceTolerance = 0.10;
        private int maxConcurrentMigrations = 2;
        private int maxMovesPerCycle = 50;
        private Duration migrationTimeout = Duration.ofMinutes(10);
        private long migrationChunkBytes = 256L * 1024L;
        private long maxSeriesBytes = 64L * 1024L * 1024L;
        private Duration migrationStatusPollInterval = Duration.ofMillis(500);
        private Duration reconcileInterval = Duration.ofMinutes(10);
        private Duration orphanGrace = Duration.ofMinutes(5);
        private String seriesObjectPrefix = DEFAULT_SERIES_OBJECT_PREFIX;

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

        /** Sobrescreve o prazo de "status velho" para placement; sem chamar isto, {@link #build()} calcula o default. */
        public Builder nodeStatusStaleAfter(Duration nodeStatusStaleAfter) {
            this.nodeStatusStaleAfter = nodeStatusStaleAfter;
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

        /** Integração opcional de métricas; {@code null} (default) = nenhuma. */
        public Builder metricsListener(NgrrdClusterMetricsListener metricsListener) {
            this.metricsListener = metricsListener;
            return this;
        }

        /**
         * Janela de boot-discovery repassada ao {@code NGridNodeBuilder} (default 3 s) — ver Javadoc do
         * record. {@code ZERO} desliga a deferral (eleição imediata, comportamento legado do NGrid).
         */
        public Builder bootDiscoveryWindow(Duration bootDiscoveryWindow) {
            this.bootDiscoveryWindow = bootDiscoveryWindow;
            return this;
        }

        /**
         * Handback orquestrado de liderança do NGrid (D11), repassado ao {@code NGridNodeBuilder}
         * (default {@code true}) — ver Javadoc do record, inclusive o custo aceito: um handback
         * abortado entra num cooldown de 60 s antes do próximo, período em que um nó de maior afinidade
         * que acabou de voltar não reassume a liderança mesmo saudável. {@code false} volta ao reclaim
         * por watermark (comportamento legado do NGrid, sujeito a perda da cauda do incumbente sob carga).
         */
        public Builder affinityHandbackMode(boolean affinityHandbackMode) {
            this.affinityHandbackMode = affinityHandbackMode;
            return this;
        }

        /** Janela após assumir a liderança durante a qual novos placements não são criados (default 3 s). */
        public Builder placementGraceAfterLeadership(Duration placementGraceAfterLeadership) {
            this.placementGraceAfterLeadership = placementGraceAfterLeadership;
            return this;
        }

        public Builder rebalanceEnabled(boolean rebalanceEnabled) {
            this.rebalanceEnabled = rebalanceEnabled;
            return this;
        }

        public Builder rebalanceInterval(Duration rebalanceInterval) {
            this.rebalanceInterval = rebalanceInterval;
            return this;
        }

        public Builder rebalanceMinDelta(long rebalanceMinDelta) {
            this.rebalanceMinDelta = rebalanceMinDelta;
            return this;
        }

        public Builder rebalanceTolerance(double rebalanceTolerance) {
            this.rebalanceTolerance = rebalanceTolerance;
            return this;
        }

        public Builder maxConcurrentMigrations(int maxConcurrentMigrations) {
            this.maxConcurrentMigrations = maxConcurrentMigrations;
            return this;
        }

        public Builder maxMovesPerCycle(int maxMovesPerCycle) {
            this.maxMovesPerCycle = maxMovesPerCycle;
            return this;
        }

        public Builder migrationTimeout(Duration migrationTimeout) {
            this.migrationTimeout = migrationTimeout;
            return this;
        }

        public Builder migrationChunkBytes(long migrationChunkBytes) {
            this.migrationChunkBytes = migrationChunkBytes;
            return this;
        }

        public Builder maxSeriesBytes(long maxSeriesBytes) {
            this.maxSeriesBytes = maxSeriesBytes;
            return this;
        }

        public Builder migrationStatusPollInterval(Duration migrationStatusPollInterval) {
            this.migrationStatusPollInterval = migrationStatusPollInterval;
            return this;
        }

        /** Intervalo entre ciclos automáticos do {@code LocalReconciler} (default 10 min). */
        public Builder reconcileInterval(Duration reconcileInterval) {
            this.reconcileInterval = reconcileInterval;
            return this;
        }

        /** Prazo mínimo antes de apagar uma cópia local órfã (default 5 min). */
        public Builder orphanGrace(Duration orphanGrace) {
            this.orphanGrace = orphanGrace;
            return this;
        }

        /**
         * Prefixo do objeto único da série no {@code BlobStorage} (default: o default do
         * {@code nishi-utils-oss}) — ver Javadoc do record. Todas as definições servidas por este nó
         * devem usar o mesmo prefixo.
         */
        public Builder seriesObjectPrefix(String seriesObjectPrefix) {
            this.seriesObjectPrefix = seriesObjectPrefix;
            return this;
        }

        private static final Duration MIN_NODE_STATUS_STALE_AFTER = Duration.ofSeconds(15);

        /** Distribution mode, shared by placement and rebalance (default COUNT). */
        public Builder distributionMode(DistributionMode mode) { this.distributionMode = mode; return this; }
        /** Explicit relative node weight (default 1). */
        public Builder weight(double weight) { this.weight = weight; return this; }

        public StorageNodeConfig build() {
            Duration resolvedStaleAfter = nodeStatusStaleAfter != null
                    ? nodeStatusStaleAfter
                    : maxDuration(statusReportInterval.multipliedBy(5), MIN_NODE_STATUS_STALE_AFTER);
            return new StorageNodeConfig(nodeId, host, port, seed, peers, dataDir, priority, volumeDir,
                    volumeName, shardCount, segmentBytes, initialShardCapacityBytes, capacityBytes,
                    statusReportInterval, resolvedStaleAfter, handleIdleTtl, maxOpenHandles, requestTimeout,
                    defaultDurability, defaultOnGeometryChange, metricsListener, bootDiscoveryWindow,
                    affinityHandbackMode, placementGraceAfterLeadership,
                    rebalanceEnabled, rebalanceInterval, rebalanceMinDelta, rebalanceTolerance,
                    maxConcurrentMigrations, maxMovesPerCycle, migrationTimeout, migrationChunkBytes,
                    maxSeriesBytes, migrationStatusPollInterval, reconcileInterval, orphanGrace,
                    seriesObjectPrefix, distributionMode, weight);
        }

        private static Duration maxDuration(Duration a, Duration b) {
            return a.compareTo(b) >= 0 ? a : b;
        }
    }
}
