# Spec M1a — módulo `nishi-utils-ngrrd-cluster`: esqueleto, catálogo, protocolo, placement

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`.
Desenho completo: `planning/ngrrd-cluster.md` (leia as seções 1, 3, 4, 7 e "M1").
O core já contém `NodeInfo.ROLE_LEADER_INELIGIBLE`, `NodeInfo.isLeaderEligible()` e `NGridNodeBuilder.roles(String...)`, instalados no `~/.m2` na versão 8.2.0.

## Regras (obrigatórias)
- Identificadores em inglês; Javadoc/comentários em PT-BR; nomes de `@Test` em PT-BR; helpers de teste em inglês.
- Nunca `git add -A`/`git add .`. **Não commite** — o orquestrador commita após revisão.
- Imports limpos. Sem `Object`/`Map<String,Object>` onde um record cabe. Sem fallback/legado.
- Não altere `nishi-utils-core` nem `nishi-utils-oss` neste marco. Se precisar de algo deles que não é público, reporte em vez de contornar.
- A versão do reactor permanece 8.2.0 neste marco (bump vai no M5).

## 1. Módulo Maven
Crie `nishi-utils-ngrrd-cluster/pom.xml` espelhando `nishi-utils-oss/pom.xml` (parent `dev.nishisan:nishi-utils-parent:8.2.0`, `relativePath ../pom.xml`, packaging jar, description PT-BR "Cluster de armazenamento distribuído para o formato ngrrd (coordenador + storage nodes sobre o NGrid)").
Dependências compile: `dev.nishisan:nishi-utils-core:${project.version}`, `dev.nishisan:nishi-utils-oss:${project.version}`, `jackson-databind`, `jackson-dataformat-yaml`, `jackson-datatype-jsr310`, e os mesmos `spring-boot-*` provided do oss se o core/oss os exigirem para logging (confira o que o oss usa para SLF4J; siga o mesmo). Test: `junit-jupiter`, `logback-classic` (mesma versão do oss). Sem AWS SDK, sem testcontainers.
Surefire no módulo: por padrão exclui `**/*ClusterTest.java`; profile `ngrrd-cluster` (ativado por `-Pngrrd-cluster`) inclui `**/*ClusterTest.java` (padrão de `-Presilience` no root). Deixe um comentário XML em PT-BR explicando.
Registre `<module>nishi-utils-ngrrd-cluster</module>` no `pom.xml` raiz **após** `nishi-utils-oss` e antes de `ngrid-test`. Verifique que o profile `exclude-ngrid` do root (exclui `**/ngrid/**`) não afeta o módulo (pacote é `dev.nishisan.utils.oss.cluster`, ok).
`src/test/resources/logback-test.xml` igual ao do oss.

## 2. Pacote `dev.nishisan.utils.oss.cluster.catalog`
- `enum PlacementState { ACTIVE, MIGRATING }`
- `enum NodeState { ACTIVE, DRAINING, DRAINED }`
- `record SeriesPlacement(String ownerNodeId, String targetNodeId, PlacementState state, String migrationId, long createdAtEpochMs, long updatedAtEpochMs)` com validação no compact constructor (`ownerNodeId` obrigatório; `MIGRATING` exige `targetNodeId` e `migrationId`; `ACTIVE` exige ambos nulos) e factories estáticas `active(String owner, long now)`, `migrating(SeriesPlacement current, String target, String migrationId, long now)`, `completed(SeriesPlacement migrating, long now)` (owner := target, ACTIVE), `aborted(SeriesPlacement migrating, long now)` (volta a ACTIVE no owner). Método `boolean isOwnedBy(String nodeId)`.
- `record StorageNodeStatus(String nodeId, NodeState state, long seriesCount, long usedBytes, long capacityBytes, long reportedAtEpochMs)` com factory `active(nodeId, now)` e `withLoad(seriesCount, usedBytes, capacityBytes, now)`, `withState(NodeState, now)`, `double fillRatio()` (0 se capacity <= 0), `boolean isFresh(long now, Duration interval)` (`now - reportedAt <= 2 × interval`).
- `final class CatalogService`: constantes `CATALOG_MAP = "ngrrd.catalog"`, `NODES_MAP = "ngrrd.nodes"`; construtor `(DistributedMap<String, SeriesPlacement> catalog, DistributedMap<String, StorageNodeStatus> nodes)`; métodos:
  - `Optional<SeriesPlacement> placementLocal(String seriesKey)` → `catalog.getOptional(key, Consistency.EVENTUAL)`
  - `Optional<SeriesPlacement> placementStrong(String seriesKey)` → `Consistency.STRONG`
  - `void putPlacement(String seriesKey, SeriesPlacement p)` → `catalog.put`
  - `void removePlacement(String seriesKey)`
  - `Map<String, SeriesPlacement> placementsLocal()` → cópia imutável de `entrySet()`
  - `Map<String, List<String>> seriesByOwnerLocal()` (só `ACTIVE`) — útil ao Rebalancer
  - `Optional<StorageNodeStatus> nodeStatusLocal(String nodeId)`, `Collection<StorageNodeStatus> nodesLocal()`, `void putNodeStatus(StorageNodeStatus s)`.
  Javadoc PT-BR deixa claro: escrita sempre via líder (comportamento do `DistributedMap`), leitura local eventual.
  Helper estático `static void declareMaps(NGridNodeBuilder b)` → `b.map(CATALOG_MAP).map(NODES_MAP)`; e `static CatalogService from(NGridNode node)` → `node.getMap(CATALOG_MAP, String.class, SeriesPlacement.class)` etc.

## 3. Pacote `dev.nishisan.utils.oss.cluster.protocol`
- `final class Commands` com constantes String exatamente como na tabela da seção 4 do plano: `PLACE="ngrrd.place"`, `OPEN="ngrrd.open"`, `WRITE_BATCH="ngrrd.writeBatch"`, `CHECKPOINT="ngrrd.checkpoint"`, `FLUSH="ngrrd.flush"`, `READ="ngrrd.read"`, `READ_PRESET="ngrrd.readPreset"`, `CLOSE="ngrrd.close"`, `MIGRATE_START="ngrrd.migrate.start"`, `MIGRATE_CHUNK`, `MIGRATE_COMMIT`, `MIGRATE_ABORT`, `MIGRATE_FINISH`, `MIGRATE_STATUS`, `ADMIN_DRAIN="ngrrd.admin.drain"`, `ADMIN_ACTIVATE`, `ADMIN_STATUS`, `ADMIN_METRICS`, `ADMIN_REBALANCE`; e `Set<String> LEADER_COMMANDS` (place, admin.*), `Set<String> OWNER_COMMANDS` (open…close), `Set<String> MIGRATION_COMMANDS`.
- `enum SeriesStatus { OK, WRONG_OWNER, MIGRATING, NOT_OPEN, NOT_LEADER, NO_STORAGE_NODE_AVAILABLE, ERROR }` (`NOT_OPEN`: o dono não tem handle/definição em memória para a série, ex.: após reinício; o cliente deve reenviar `ngrrd.open` e repetir).
- Payloads (records públicos, sem `Duration`/`Instant` — use `long` epoch ms; sem anotações Jackson salvo se o round-trip exigir):
  - `PlaceRequest(String seriesKey, String definitionHashHex, String preferredOwnerNodeId)`
  - `PlaceResponse(SeriesStatus status, SeriesPlacement placement, String message)`
  - `OpenRequest(String seriesKey, String yaml, Map<String,String> tags, Durability durability, OnGeometryChange onGeometryChange, SeriesPlacement placementHint)` (`Durability`/`OnGeometryChange` são enums do oss `api/`, já têm `@JsonCreator`)
  - `SeriesStatusResponse(SeriesStatus status, String ownerNodeId, String message)` — usada por open/checkpoint/flush/close
  - `SeriesWrite(String seriesKey, String dsName, long tsEpochMs, double value)`
  - `WriteBatchRequest(List<SeriesWrite> writes)`
  - `WriteBatchResponse(Map<String, SeriesStatus> statusBySeries, Map<String, String> ownerBySeries, Map<String, String> errorBySeries)` (mapas podem ser vazios, nunca null)
  - `SeriesCommandRequest(String seriesKey)` — checkpoint/flush/close
  - `ReadRequest(String seriesKey, String dsName, long windowMs, int targetStepSec, ConsolidationFunction cf, int maxPoints, Long endExclusiveEpochMs)` + método `ViewQuery toViewQuery()` e factory `of(seriesKey, dsName, ViewQuery, Long end)`
  - `ReadResponse(SeriesStatus status, String ownerNodeId, SeriesResult result, String message)`
  - `ReadPresetRequest(String seriesKey, String presetName, Long endExclusiveEpochMs)`
  - `ReadPresetResponse(SeriesStatus status, String ownerNodeId, Map<String, SeriesResult> results, String message)`
  - Migração (só os tipos, sem handlers ainda): `MigrateStartRequest(String seriesKey, String migrationId, String targetNodeId)`, `MigrateChunkRequest(String seriesKey, String migrationId, int seq, int total, byte[] data)`, `MigrateCommitRequest(String seriesKey, String migrationId, String sha256Hex, long totalBytes)`, `MigrateControlRequest(String seriesKey, String migrationId)` (abort/finish/status), `MigrateResponse(MigrateStatus status, String message)`, `enum MigrateStatus { OK, COMMITTED, PARTIAL, UNKNOWN, HASH_MISMATCH, ERROR }`.
  - Admin: `AdminNodeRequest(String nodeId)`, `AdminStatusResponse(String leaderNodeId, List<StorageNodeStatus> nodes, int migrationsInFlight, Map<String, Long> seriesCountByNode)`.
- `ProtocolCodecTest`: para CADA record acima, monte `ClusterMessage.request(MessageType.CLIENT_REQUEST, cmd, NodeId.of("a"), NodeId.of("b"), new ClientRequestPayload(UUID, cmd, body))` e faça round-trip por `dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec` (ou `CompositeMessageCodec`, o que for público — verifique) e compare `payload(ClientRequestPayload.class).body()` com `equals` do record. Inclua `SeriesResult` com pontos e enum, `byte[]` de 300 KiB no `MigrateChunkRequest`, mapas vazios, `Long` nulo. Faça o mesmo com `ClientResponsePayload` para as respostas. Se algum record falhar (ex.: Jackson do core sem suporte a record, ou `byte[]`), corrija **no record** (anotações `@JsonCreator/@JsonProperty`) e reporte; se a causa estiver no codec do core, PARE e reporte sem contornar.

## 4. Pacote `dev.nishisan.utils.oss.cluster.placement`
- `interface PlacementPolicy { Optional<String> choose(PlacementContext ctx); }`
- `record PlacementContext(Collection<StorageNodeStatus> nodes, Set<String> reachableNodeIds, Map<String, Long> pendingSeriesByNode, long nowEpochMs, Duration statusReportInterval, String preferredOwnerNodeId)` — `pendingSeriesByNode` = séries já colocadas pelo líder num nó desde o último `reportedAt` daquele nó (o status é reportado a cada ~10 s; sem esse ajuste, todas as séries novas de uma rajada cairiam no mesmo nó). Carga efetiva = `seriesCount + pending`.
- `final class LeastLoadedPlacementPolicy implements PlacementPolicy`: candidatos = `state == ACTIVE && reachable && isFresh`; se `preferredOwnerNodeId` estiver entre os candidatos, retorna ele; senão menor `fillRatio()` **quando ambos têm `capacityBytes > 0`**, depois menor carga efetiva (`seriesCount + pending`), depois menor `nodeId` (ordem total, determinística). Vazio → `Optional.empty()`.
- `LeastLoadedPlacementPolicyTest`: cobre cada critério e os filtros (DRAINING, inalcançável, status velho, preferido presente/ausente, empate total → nodeId, rajada de 10 placements com status parado alterna entre dois nós graças ao `pending`).

## 5. Pacote `dev.nishisan.utils.oss.cluster.api`
- `enum ErrorCode { NO_LEADER, NO_STORAGE_NODE_AVAILABLE, SERIES_UNAVAILABLE, WRONG_OWNER, MIGRATING, REMOTE_ERROR, TIMEOUT, BUFFER_FULL, CLOSED }`
- `class NgrrdClusterException extends RuntimeException` com `ErrorCode code()`, construtores `(code, message)` e `(code, message, cause)`.

## 6. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster -q compile
mvn -pl nishi-utils-ngrrd-cluster test
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster     # deve rodar sem *ClusterTest ainda; só confirmar que o profile existe
```
Confirme também que `mvn -q -pl nishi-utils-oss test -Dtest=NgrrdBlobFacadeTest` continua verde (sanidade; oss não deve ter sido tocado).

## Relatório
Arquivos criados com `path:linha` dos pontos-chave; comandos e contagem real Tests run/Failures/Errors por classe; decisões divergentes; NÃO VERIFICADO.

## Concorrência na árvore (IMPORTANTE)
Outro agente está revisando mudanças NÃO commitadas em `nishi-utils-core` (arquivos `M`/`??` sob `nishi-utils-core/` no `git status`). Não toque neles, não rode `git stash`, não rode maven na raiz nem com `-am` (o core já está instalado no `~/.m2` em 8.2.0). Use sempre `mvn -pl nishi-utils-ngrrd-cluster ...`. Para a sanidade do oss, `mvn -pl nishi-utils-oss ...` é permitido.
