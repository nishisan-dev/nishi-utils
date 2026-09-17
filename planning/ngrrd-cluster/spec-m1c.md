# Spec M1c — cliente transparente (`NgrrdClusterClient` + `RemoteSeriesHandle`) e harness fim a fim

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`, módulo `nishi-utils-ngrrd-cluster`. Já existem: `catalog/`, `protocol/`, `placement/`, `api/`, `rpc/`, `node/` (leia `rpc/ClusterRpc`, `node/NgrrdStorageNode`, `node/StorageRequestHandler`, `protocol/*` e `StorageNodeClusterTest` antes de começar — o cliente é o espelho do que o nó já atende).
Desenho completo: `planning/ngrrd-cluster.md` (seções 2, 5).

## Regras (obrigatórias)
- Identificadores em inglês; Javadoc/comentários em PT-BR; nomes de `@Test` em PT-BR; helpers de teste em inglês.
- Nunca `git add -A`/`git add .`. **Não commite.**
- Não altere `nishi-utils-core` nem `nishi-utils-oss`.
- Sem `Thread.sleep` fixo em testes: polling com prazo. Sem fallback/legado.
- `NgrrdHandle` do oss é o contrato: `RemoteSeriesHandle implements dev.nishisan.utils.oss.NgrrdHandle` sem métodos extras públicos além do necessário.

## 1. Pacote `api`
- `NgrrdClusterConfig` (record + `Builder`): `clientId` (default `"ngrrd-client-" + UUID curto`), `host` (default "127.0.0.1"), `port` (0 efêmera), `seed` (opcional), `peers` (opcional; ao menos um dos dois obrigatório), `dataDir` (Path; default `Files.createTempDirectory("ngrrd-client")` criado no `connect`), `batchMaxSamples` (500), `batchMaxDelay` (200 ms), `maxBufferedSamplesPerNode` (100_000), `bufferFullPolicy` (`enum BufferFullPolicy { BLOCK, FAIL }`, default BLOCK), `requestTimeout` (20 s), `retryTimeout` (5 min), `retryBackoffMin` (100 ms), `retryBackoffMax` (2 s), `leaderWaitTimeout` (30 s).
- `NgrrdClusterClient` (interface pública, `Closeable`): `NgrrdHandle open(String yaml, Map<String,String> tags)`, `NgrrdHandle open(String yaml, Map<String,String> tags, Ngrrd.OpenOptions options)`, `NgrrdHandle open(Path yamlFile, Map<String,String> tags)`, `void flushAll()`, `ClientMetricsSnapshot metrics()`, `NodeId clientNodeId()`, `close()`.
- `NgrrdCluster` (façade final, raiz `dev.nishisan.utils.oss.cluster`): `static NgrrdClusterClient connect(NgrrdClusterConfig cfg)`; `static NgrrdStorageNode startStorageNode(StorageNodeConfig cfg)` (delegações).
- `ClientMetricsSnapshot` (record): `samplesEnqueued`, `samplesSent`, `samplesFailed`, `batchesSent`, `retriesByStatus` (Map<SeriesStatus, Long>), `bufferedSamples` (Map<nodeId, Long>), `openHandles`.

## 2. Pacote `client`
### `SeriesKeyTemplate`
- `static String resolve(String template, Map<String,String> tags)` com **exatamente** a semântica de `Ngrrd.resolveSeriesKey` (`nishi-utils-oss/.../Ngrrd.java:247+`, package-private — leia e replique; mesmas exceções para tag ausente). Teste com os mesmos casos que o oss cobre (procure testes de `resolveSeriesKey`/`seriesKeyTemplate` no oss e espelhe).
- `static String templateOf(String yaml)`: `NgrrdYamlLoader.parse(yaml, System::getenv).spec().identity().seriesKeyTemplate()` (confira a assinatura de `parse`).

### `PlacementResolver`
- Construtor `(CatalogService catalog, ClusterRpc rpc, RetryPolicy retry, Clock)`.
- `SeriesPlacement resolve(String seriesKey, String definitionHashHex)`: 1) cache local de overrides (`ConcurrentHashMap<String, SeriesPlacement>`, preenchido por respostas de `PLACE`); 2) `catalog.placementLocal(key)` se `ACTIVE`; 3) `PLACE` no líder (`rpc.leaderId()`; sem líder → espera até `leaderWaitTimeout` via polling curto, depois `NgrrdClusterException(NO_LEADER)`); `NOT_LEADER` → re-resolve o líder e repete (≤ 5 vezes, backoff); `NO_STORAGE_NODE_AVAILABLE` → exceção com esse código; `OK` → grava override e devolve.
- `void invalidate(String seriesKey)`; `void noteOwner(String seriesKey, String ownerNodeId)` (quando uma resposta `WRONG_OWNER` traz o dono, atualiza override para `ACTIVE(owner)` sem RPC).

### `RetryPolicy`
- Record `(Duration timeout, Duration backoffMin, Duration backoffMax)`, método `Duration backoffFor(int attempt)` (exponencial com teto) e `boolean exhausted(long startedAtMs, long nowMs)`.

### `WriteDispatcher` (`Closeable`)
- Um `NodeBuffer` por `ownerNodeId`: `ArrayDeque<SeriesWrite>` + `ReentrantLock`/`Condition`, capacidade `maxBufferedSamplesPerNode`. `enqueue(owner, write)`: se cheio → `BLOCK` espera espaço (interrompível → `NgrrdClusterException(CLOSED)` se fechado), `FAIL` → `NgrrdClusterException(BUFFER_FULL)`. Se após enfileirar `size >= batchMaxSamples` → agenda flush imediato do nó.
- Thread única de tick (`ngrrd-write-dispatcher`, daemon) a cada `batchMaxDelay` agenda flush dos nós com pendências; flushes executam num pool pequeno (`min(4, nós)`), no máximo **um flush em voo por nó** (flag `inFlight`).
- `flush(owner)`: retira até `batchMaxSamples` (mantendo ordem), envia `WRITE_BATCH`; resposta por série: `OK` → conta `samplesSent`; `WRONG_OWNER` → `resolver.noteOwner`/`invalidate` e re-enfileira essas amostras **na frente** do buffer do novo dono (ordem preservada); `NOT_OPEN` → chama `reopen` do handle correspondente (callback `Function<String, Boolean> reopener` fornecido pelo cliente) e re-enfileira no mesmo nó; `MIGRATING` → re-enfileira no mesmo nó com backoff (não reenvia antes de `backoffFor(n)`); `ERROR` → log WARN + `samplesFailed` (descarta essas amostras do lote — o servidor rejeitou); falha de transporte (`TIMEOUT`/`REMOTE_ERROR`) → devolve o lote inteiro à frente do buffer e marca o nó com backoff. Contadores em `LongAdder`.
- `void flushNodeSync(owner)`: força flush do nó e espera (usado por `flush()`/`checkpoint()` do handle); `void flushAllSync()`.
- `close()`: `flushAllSync()` com prazo `requestTimeout`, depois para threads; pendências que não couberam viram `samplesFailed` com log ERROR (contagem no relatório).

### `RemoteSeriesHandle implements NgrrdHandle`
- Estado: `seriesKey`, `yaml`, `definitionHashHex`, `tags`, `options`, `volatile String owner`, `closed`.
- `open()` (chamado por `client.open`): `resolver.resolve` → `OPEN` no dono com `placementHint`; `WRONG_OWNER` → `noteOwner`/`invalidate`, repete (≤ 5); `MIGRATING` → backoff até `retryTimeout`; `ERROR` → `NgrrdClusterException(REMOTE_ERROR, msg)`.
- `write(ds, sample)` → `dispatcher.enqueue(owner, new SeriesWrite(seriesKey, ds, ts, value))`; `closed` → `CLOSED`.
- `flush()`: `dispatcher.flushNodeSync(owner)` → `FLUSH` no dono (mesmo tratamento de status de `checkpoint`).
- `checkpoint()`: `flushNodeSync(owner)` → `CHECKPOINT`; `WRONG_OWNER` → atualiza dono e repete uma vez; `NOT_OPEN` → `reopen()` e repete uma vez; `MIGRATING` → backoff até `retryTimeout`; senão exceção.
- `read(...)` (4 sobrecargas): `READ`/`READ_PRESET` com o mesmo tratamento de status; retorna `SeriesResult`/`Map`.
- `reopen()`: reexecuta `open()` (usado pelo dispatcher em `NOT_OPEN`).
- `close()`: `flushNodeSync(owner)`, `CLOSE` best-effort (erro só loga), remove do registro do cliente, `closed=true`.
- `seriesKey()`.

### `DefaultNgrrdClusterClient implements NgrrdClusterClient`
- `connect(cfg)`: `NGrid.node(host, port).id(clientId).priority(0).roles("client", NodeInfo.ROLE_LEADER_INELIGIBLE).dataDir(dataDir)` + seed/peers + `CatalogService.declareMaps` → `start()`; espera `coordinator.leaderInfo()` presente até `leaderWaitTimeout` (polling 50 ms) senão `NO_LEADER`; monta `TransportClusterRpc`, `CatalogService.from(node)`, `PlacementResolver`, `WriteDispatcher`, `Map<String, RemoteSeriesHandle> handles`.
- `open(yaml, tags[, options])`: `seriesKey = SeriesKeyTemplate.resolve(templateOf(yaml), tags)`; se já existe handle aberto para a chave, devolve o mesmo (contagem de referências não é necessária: `close()` fecha para todos — documente); senão cria `RemoteSeriesHandle`, `open()`, registra.
- `close()`: fecha handles (flush), `dispatcher.close()`, `node.close()`; apaga `dataDir` se foi temporário criado pelo cliente.

## 3. Testes
Unitários (default):
- `SeriesKeyTemplateTest`, `RetryPolicyTest`.
- `WriteDispatcherTest` com `ClusterRpc` fake (interface já existe): batching por tamanho e por tempo (Clock/tick manual ou delay curto com polling), ordem preservada, `WRONG_OWNER` re-roteia para o novo dono, `NOT_OPEN` chama reopener e reenvia, `ERROR` conta `samplesFailed`, `BLOCK` bloqueia até flush liberar, `FAIL` lança `BUFFER_FULL`, falha de transporte devolve o lote ao buffer, `close()` drena.
- `PlacementResolverTest` com fakes: override cache, `NOT_LEADER` re-resolve, `NO_STORAGE_NODE_AVAILABLE`, `noteOwner`.
- `RemoteSeriesHandleTest` com `ClusterRpc` fake: `open` com `WRONG_OWNER` → segundo dono; `checkpoint` com `NOT_OPEN` → reopen + repete; `MIGRATING` além do timeout → exceção `MIGRATING`; `closed` → `CLOSED`.
Cluster (`*ClusterTest`, profile `ngrrd-cluster`):
- `NgrrdClusterTestHarness` (test scope, reutilizável nos marcos seguintes): `static Harness start(int storageNodes, Consumer<StorageNodeConfig.Builder> customize)` — portas pré-alocadas, malha completa, `@TempDir`-friendly (recebe `Path base`); `NgrrdClusterClient connectClient(Consumer<NgrrdClusterConfig.Builder>)`; `awaitLeader()`, `awaitNodeStatuses(n)`, `awaitPlacements(n)`, `leaderNode()`, `nodes()`, `addStorageNode(...)` (para M3), `close()`.
- `DistributedWriteReadClusterTest`: 2 storage nodes + cliente. Abre 40 séries (tags distintas) com o YAML `iface-traffic-blob.yaml`; escreve rampa de 60 amostras por série (mesmo padrão de `writeRamp` do oss `NgrrdBlobFacadeTest`); `checkpoint()` em todas; `read("daily")` (preset do YAML) em todas e valida pontos não vazios e coerentes com o que o teste single-node do oss valida; `handle.read(ds, ViewQuery)` também. Asserções de distribuição: catálogo com 40 `ACTIVE`, cada nó dono de 20 ± 2; para cada série, `owner.volume().storage().exists(seriesKey)` é true e no outro nó é false. Reabertura: `client.close()`, novo `connect`, `open` das mesmas séries, `read` devolve os dados. Reinício de nó: fechar um storage node e subir outro com o MESMO `nodeId`, `dataDir` e `volumeDir`; cliente faz `write`+`checkpoint` nas séries dele: a primeira tentativa recebe `NOT_OPEN` (dispatcher reabre sozinho) e o `checkpoint` termina sem exceção; `read` devolve dados antigos e novos. Cliente nunca vira líder: `client` tem `ROLE_LEADER_INELIGIBLE` (asserção em `coordinator().leaderInfo()` dos nós ≠ clientId durante todo o teste).
- Timeout total ≤ 120 s; polling com prazo.

## 4. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster test
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster
```
Rode o `DistributedWriteReadClusterTest` 3 vezes e reporte cada contagem.

## Relatório
Arquivos criados com `path:linha`; comandos e contagens reais; divergências; NÃO VERIFICADO.
