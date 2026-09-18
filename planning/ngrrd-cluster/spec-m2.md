# Spec M2 — métricas por nó, `admin.status` e `admin.metrics`

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`, módulo `nishi-utils-ngrrd-cluster`.
Desenho: `planning/ngrrd-cluster.md` (seções 4 e 10). Histórico: `planning/ngrrd-cluster/checkpoint.md`.
Motivação: o gargalo de disco do ngrrd-consumer nunca foi medido; o cluster precisa expor números por nó
(latência de escrita/checkpoint/leitura, séries, bytes, buffers) para confirmar onde o tempo vai.

## Regras (obrigatórias)
- Identificadores em inglês; Javadoc/comentários em PT-BR; `@Test` em PT-BR; cabeçalho GPL em todo `.java` novo.
- Sem commit, sem `git add -A`, sem `git stash`. Maven só `-pl nishi-utils-ngrrd-cluster`, nunca raiz nem `-am`.
- Não alterar `nishi-utils-core` nem `nishi-utils-oss`. Sem `Thread.sleep` fixo em testes.
- Operações que passam por `withHandleSelfHealing` nunca devolvem `null`.
- O gate JaCoCo (40% de linha) precisa continuar passando em `mvn -pl nishi-utils-ngrrd-cluster verify`.

## 1. Pacote `metrics` — `dev.nishisan.utils.oss.cluster.metrics`
- `LatencyHistogram` (thread-safe, sem dependência externa): `record(long nanos)`; `snapshot()` → `LatencySnapshot(long count, long p50Micros, long p99Micros, long maxMicros)`. Implementação simples: reservatório circular de N=1024 amostras (ou buckets log2); documentar a aproximação. Reset opcional por `snapshotAndReset()` não é necessário.
- `NodeMetricsSnapshot` (record, serializável pelo codec do NGrid — sem `Duration`): `nodeId`, `capturedAtEpochMs`, `leader` (boolean), `seriesCount`, `usedBytes`, `capacityBytes`, `openHandles`, `writeBatches`, `samplesWritten`, `samplesFailed`, `checkpoints`, `flushes`, `reads`, `writeBatchLatency` (LatencySnapshot), `checkpointLatency`, `readLatency`, `errorsByStatus` (Map<SeriesStatus, Long>), `blobStats` (record próprio `BlobVolumeSummary(int shardCount, long usedBytes, long capacityBytes, double maxFillRatio, int catalogEntryCount, long walBytes)` derivado de `BlobVolumeStats`), `migrationsIn`, `migrationsOut` (zeros neste marco).
- `NgrrdClusterMetricsListener` (interface, métodos `default` no-op): `onNodeMetrics(NodeMetricsSnapshot)`, `onClientMetrics(ClientMetricsSnapshot)`. `StorageNodeConfig` e `NgrrdClusterConfig` ganham `metricsListener` opcional. O nó chama `onNodeMetrics` a cada tick do `NodeStatusReporter`; o cliente chama `onClientMetrics` a cada `batchMaxDelay × 50` (≈10 s) numa thread do dispatcher.
- Log marker: a cada tick, o nó loga em INFO uma linha `NGRRD_NODE_STATUS nodeId=... leader=... series=... usedBytes=... openHandles=... samples/s=... writeBatchP99us=... checkpointP99us=... readP99us=...` (padrão de marker do projeto; Docker ITs futuros podem depender dele — não renomear depois). Taxa `samples/s` calculada entre ticks.

## 2. Instrumentação existente
- `StorageRequestHandler`: além dos contadores atuais, medir latência de `writeBatch`, `checkpoint` e `read`/`readPreset` com `LatencyHistogram`; expor `metricsSnapshot()` → `StorageHandlerMetrics` ampliado (ou substituir por `NodeMetricsSnapshot` parcial). `samplesFailed` = amostras descartadas por `ERROR`.
- `NgrrdStorageNode.metricsSnapshot()` monta o `NodeMetricsSnapshot` completo (handler + registry + volume + liderança).
- Cliente: `ClientMetricsSnapshot` já existe; garantir `retriesByStatus`, `bufferedSamples` por nó, `samplesFailed`, `openHandles` e adicionar `rpcLatency` (LatencySnapshot) por comando agregado e `placeCount`.

## 3. Comandos admin (protocolo já tem `Commands.ADMIN_STATUS`, `ADMIN_METRICS`, `AdminStatusResponse`, `AdminNodeRequest`)
- `node/AdminRequestHandler extends RequestHandlerSupport`:
  - `ADMIN_METRICS` (qualquer nó): corpo `AdminNodeRequest(nodeId)`; se `nodeId` é o próprio ou nulo → `NodeMetricsSnapshot` local; se é outro nó → encaminha por `rpc.call(target, ADMIN_METRICS, ...)` e devolve (um salto no máximo: flag `forwarded` no request para não encadear).
  - `ADMIN_STATUS` (só líder; senão `NOT_LEADER` na resposta): `AdminStatusResponse(leaderNodeId, nodes (StorageNodeStatus de todos, marcando inalcançáveis com um campo novo `reachable` — adicione ao record ou devolva `List<NodeStatusView(StorageNodeStatus status, boolean reachable)>`), migrationsInFlight=0, seriesCountByNode do catálogo local)`.
- `api/NgrrdClusterClient` ganha `AdminStatusResponse clusterStatus()` e `NodeMetricsSnapshot nodeMetrics(String nodeId)` (delegam ao RPC; `clusterStatus` vai ao líder com re-resolução em `NOT_LEADER`).

## 4. Testes
- Unitários: `LatencyHistogramTest` (p50/p99 em distribuição conhecida; vazio → zeros; concorrência simples), `AdminRequestHandlerTest` com fakes (`NOT_LEADER`, status com nó inalcançável, metrics local e encaminhado uma vez só), `StorageRequestHandler` latências preenchidas após operações, snapshot do nó com blob stats reais em `@TempDir`.
- Cluster (`AdminStatusClusterTest`, profile `ngrrd-cluster`): 2 nós + cliente via harness; após 20 séries escritas e checkpoint: `clusterStatus()` lista 2 nós alcançáveis e `seriesCountByNode` soma 20; `nodeMetrics(ownerId)` traz `samplesWritten > 0`, `checkpointLatency.count > 0`, `openHandles > 0`; derrubar um nó → `clusterStatus()` marca `reachable=false` para ele em ≤ 10 s; o log do nó contém `NGRRD_NODE_STATUS`. `@Timeout(SEPARATE_THREAD)`, timeouts curtos.

## 5. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster verify
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster -Dtest='AdminStatusClusterTest,DistributedWriteReadClusterTest,NodeRestartClusterTest' -Dsurefire.failIfNoSpecifiedTests=false
```
Rodar `AdminStatusClusterTest` 3×. Relatório: arquivos com `path:linha`, contagens reais por classe, JaCoCo, divergências, NÃO VERIFICADO.
