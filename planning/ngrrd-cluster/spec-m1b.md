# Spec M1b — storage node, placement no líder e RPC

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`, módulo `nishi-utils-ngrrd-cluster` (já existe: pom, `catalog/`, `protocol/`, `placement/`, `api/` — leia esses pacotes antes de começar; NÃO os reescreva, apenas complemente se a spec pedir).
Desenho completo: `planning/ngrrd-cluster.md` (seções 2, 3, 4, 6, 7).

## Regras (obrigatórias)
- Identificadores em inglês; Javadoc/comentários em PT-BR (módulo novo); nomes de `@Test` em PT-BR; helpers de teste em inglês.
- Nunca `git add -A`/`git add .`. **Não commite.**
- Não altere `nishi-utils-core` nem `nishi-utils-oss`. Se algo não for público, reporte.
- Toda exceção dentro de um `TransportListener.onMessage` é capturada e vira resposta `ERROR` — nunca escapa para o worker do transporte.
- Sem fallback/legado. Sem `Thread.sleep` fixo em testes: use polling com prazo.

## Referências de API (já verificadas; confira as linhas antes de usar)
- `Transport` (`ngrid/cluster/transport/Transport.java`): `addListener`, `send`, `sendAndAwait` → `CompletableFuture<ClusterMessage>`, `local()`, `isReachable(NodeId)`.
- `TransportListener`: `onPeerConnected(NodeInfo)`, `onPeerDisconnected(NodeId)`, `onMessage(ClusterMessage)` — os três obrigatórios.
- Request: `ClusterMessage.request(MessageType.CLIENT_REQUEST, command, localId, targetId, new ClientRequestPayload(UUID.randomUUID(), command, body))`; resposta: `transport.send(ClusterMessage.response(request, new ClientResponsePayload(payload.requestId(), success, body, error)))`. Exemplo canônico: `structures/DistributedQueue.java:332-362` e `:475-503`.
- `NGridNode`: `transport()`, `coordinator()` (`leaderInfo()`, `isLeader()`, `activeMembers()`, `addLeadershipListener`, `addMembershipListener`), `getMap(name, K.class, V.class)`, `close()`. Id local: `node.transport().local().nodeId()`.
- `NGrid.node(host, port)` → `NGridNodeBuilder`: `id`, `priority`, `roles`, `seed`, `peers`, `map`, `dataDir`, `replication`, `strictConsistency`, `start()`.
- `NGridConfig.Builder.requestTimeout(Duration)` existe; no `NGridNodeBuilder` verifique se há passthrough; se não houver, use o default (20 s) e reporte.
- ngrrd: `Ngrrd.open(BlobVolume, NgrrdUri.of(volumeName, seriesKey), yaml, OpenOptions)`; `NgrrdBlob.registry().basePath(..).shardCount(..).segmentBytes(..).initialShardCapacityBytes(..).volume(name).build()` → `BlobVolumeRegistry.require(name)` → `BlobVolume` (`storage()` → `BlobStorage` com `get/put/delete/list/exists/stats`, `stats()`, `close()`). `NgrrdHandle`: `write(ds, Sample)`, `flush()`, `checkpoint()`, `read(ds, ViewQuery[, end])`, `read(preset[, end])`, `close()`. `DefinitionHash.hex(yaml)`.
- **Verifique** a semântica de `NgrrdHandle.close()` no oss (`writer/NgrrdWriter.java`): drena a fila e força durabilidade? Se não, o registry deve chamar `checkpoint()` antes de `close()`.

## 1. Pacote `rpc` — `dev.nishisan.utils.oss.cluster.rpc`
- `interface ClusterRpc { <R> R call(NodeId target, String command, Object body, Class<R> responseType); NodeId localId(); Optional<NodeId> leaderId(); }`
- `final class TransportClusterRpc implements ClusterRpc` (construtor `(Transport, ClusterCoordinator, Duration requestTimeout)`): monta request como acima, `future.get(timeout)`; `TimeoutException` → `NgrrdClusterException(TIMEOUT)`; outras falhas de transporte → `NgrrdClusterException(REMOTE_ERROR, msg, cause)`; `ClientResponsePayload.success()==false` → `NgrrdClusterException(REMOTE_ERROR, error)`; body com tipo errado → `REMOTE_ERROR`. Se `target` é o nó local, ainda assim passa pelo transporte? NÃO: chame diretamente os listeners locais é complexo; em vez disso, garanta que `TcpTransport.send` a si mesmo funciona — **verifique** em `TcpTransport` se mensagens com destination == local são entregues aos listeners locais (loopback). Se não forem, implemente em `TransportClusterRpc` um `LocalDispatch` opcional: interface `LocalRequestHandler { Optional<Object> handleLocal(String command, Object body); }` registrada pelos handlers do nó, usada quando `target.equals(localId)`. Reporte qual caminho foi necessário.
- `RequestHandlerSupport` (classe base abstrata, ou utilitário) para os listeners do módulo: dado um `Set<String>` de comandos que atende, filtra `CLIENT_REQUEST`, extrai `ClientRequestPayload`, chama `protected abstract Object handle(String command, Object body, NodeId source)` dentro de try/catch e envia a resposta (`success=true` com body; exceção → `success=false` + `error` = classe+mensagem, log WARN). `onPeerConnected/Disconnected` no-op por padrão.

## 2. Pacote `node` — `dev.nishisan.utils.oss.cluster.node`

### `StorageNodeConfig` (record + `Builder`)
Campos: `nodeId` (String, obrigatório), `host` (default "127.0.0.1"), `port` (0 = efêmera), `seed` (String host:port, opcional), `peers` (List<String>, opcional), `dataDir` (Path, obrigatório — diretório do NGrid), `priority` (default 100), `volumeDir` (Path, obrigatório), `volumeName` (default "ngrrd"), `shardCount` (default 64), `segmentBytes` (default 1 GiB), `initialShardCapacityBytes` (default: o mesmo default do oss `BlobVolumeConfig`), `capacityBytes` (default 0 = desconhecido), `statusReportInterval` (default 10 s), `handleIdleTtl` (default 15 min), `maxOpenHandles` (default 10_000), `requestTimeout` (default 20 s), `defaultDurability` (`Durability`, default FSYNC), `defaultOnGeometryChange` (default FAIL). Validação no build. `Builder` fluente; `static Builder builder()`.

### `SeriesHandleRegistry` (thread-safe, `Closeable`)
- Construtor `(BlobVolume volume, String volumeName, Duration idleTtl, int maxOpenHandles, Clock clock)`.
- `NgrrdHandle open(String seriesKey, String yaml, Ngrrd.OpenOptions options)`: se já aberto, atualiza `lastAccess` e retorna; senão abre via `Ngrrd.open(...)`, guarda `definitionHashHex → yaml` num cache interno (`DefinitionCache`: `Map<hash, yaml>` + `Map<seriesKey, hash>`) e retorna. Abertura por chave serializada (`computeIfAbsent` ou lock por chave).
- `Optional<NgrrdHandle> existing(String seriesKey)` (atualiza `lastAccess`); `Optional<NgrrdHandle> reopenIfKnown(String seriesKey)`: se a série foi fechada por ociosidade mas o `DefinitionCache` ainda conhece seu hash/yaml, reabre; senão `empty()`.
- `void close(String seriesKey)` (checkpoint + close); `int closeIdle()` (fecha handles com `lastAccess` > ttl; retorna quantos); `void evictIfOverLimit()` (fecha LRU até ficar ≤ `maxOpenHandles`, chamado após cada `open`); `int openCount()`; `Set<String> openSeries()`; `close()` fecha todos.
- Estado local de migração (preparação para M3): `void markMigrating(String seriesKey)` / `void clearMigrating(String seriesKey)` / `boolean isMigrating(String seriesKey)` — `markMigrating` faz checkpoint+close do handle e impede `open`/`existing` de reabrir (retornam `empty`/lançam `IllegalStateException("MIGRATING")`). Só a API; sem handlers de migração neste marco.

### `StorageRequestHandler extends RequestHandlerSupport` (comandos `Commands.OWNER_COMMANDS`)
- Dependências: `CatalogService`, `SeriesHandleRegistry`, `NodeId self`, `StorageNodeConfig`.
- Checagem de dono por requisição (`ownership(seriesKey, placementHint)`):
  1. `registry.isMigrating(key)` → `MIGRATING`.
  2. `catalog.placementLocal(key)`: presente e `state == MIGRATING` → `MIGRATING`; presente e `!isOwnedBy(self)` → `WRONG_OWNER(owner)`; presente e dono → OK.
  3. ausente: se `placementHint != null && placementHint.isOwnedBy(self)` → OK (corrida líder→dono); senão se `registry.existing(key)` presente → OK (já aberta localmente, catálogo local atrasado); senão `WRONG_OWNER(owner=null)`.
- `OPEN`: ownership; OK → `registry.open(key, yaml, OpenOptions.of(durability ?: default, ogc ?: default))` → `SeriesStatusResponse(OK, self, null)`; falha do oss (ex.: geometria incompatível) → `SeriesStatusResponse(ERROR, self, msg)`.
- `WRITE_BATCH`: agrupa por `seriesKey`; por série: ownership → se não OK, status correspondente; senão `registry.existing(key)` ou `reopenIfKnown(key)`; ausente → `NOT_OPEN`; presente → `handle.write(ds, new Sample(ts, value))` por amostra (na ordem recebida); exceção → `ERROR` com mensagem. Resposta `WriteBatchResponse` com status para TODAS as séries do lote.
- `CHECKPOINT` / `FLUSH`: ownership → handle (`existing`/`reopenIfKnown`) → `checkpoint()`/`flush()` → `SeriesStatusResponse`.
- `READ`: ownership → handle → `handle.read(ds, req.toViewQuery(), end)` (ou sem `end` quando nulo) → `ReadResponse(OK, self, result, null)`; `NOT_OPEN` se sem handle.
- `READ_PRESET`: idem com `handle.read(preset[, end])`.
- `CLOSE`: `registry.close(key)` se aberto → OK (idempotente).
- Métricas mínimas (LongAdder): `writeBatches`, `samplesWritten`, `reads`, `checkpoints`, `errorsByStatus` — expostas por `StorageRequestHandler.metricsSnapshot()` (record `StorageHandlerMetrics`). O M2 amplia.

### `PlacementRequestHandler extends RequestHandlerSupport` (comando `Commands.PLACE`)
- Dependências: `CatalogService`, `ClusterCoordinator`, `Transport`, `PlacementPolicy`, `Duration statusReportInterval`, `Clock`.
- Se `!coordinator.isLeader()` → `PlaceResponse(NOT_LEADER, null, leaderId ou "no leader")`.
- Senão, sob lock por `seriesKey` (striping, 64 locks): `catalog.placementStrong(key)` presente → `PlaceResponse(OK, placement, null)` (idempotente). Ausente → `PlacementContext(nodes = catalog.nodesLocal(), reachable = ids de `coordinator.activeMembers()` filtrados por `transport.isReachable` (o próprio líder conta como alcançável), pendingSeriesByNode, now, interval, preferredOwner)` → `policy.choose` → vazio → `NO_STORAGE_NODE_AVAILABLE`; escolhido → `SeriesPlacement.active(owner, now)` → `catalog.putPlacement` → incrementa `pending[owner]` → `PlaceResponse(OK, placement, null)`.
- `pendingSeriesByNode`: `Map<String, PendingCounter{long reportedAtSeen; long count}>`; ao montar o contexto, para cada nó cujo `reportedAt` mudou desde `reportedAtSeen`, zera `count`.
- Ao perder liderança (`LeadershipListener`), zera os pendings.

### `NodeStatusReporter` (`Closeable`)
- `ScheduledExecutorService` (daemon, nome `ngrrd-status-reporter`), a cada `statusReportInterval`, e uma vez imediatamente no start: lê `volume.stats()` → `seriesCount = catalogEntryCount`, `usedBytes = Σ shardUsedBytes`; `state` = o estado já registrado em `catalog.nodeStatusLocal(self)` se houver (preserva DRAINING/DRAINED), senão `ACTIVE`; `catalog.putNodeStatus(...)`. Sem líder/exceção → log WARN e tenta no próximo tick. Também agenda `registry.closeIdle()` no mesmo tick.

### `NgrrdStorageNode` (`Closeable`)
- `static NgrrdStorageNode start(StorageNodeConfig cfg)`: cria `BlobVolumeRegistry` + `BlobVolume`; `NGrid.node(host, port).id(nodeId).priority(priority).roles("storage").dataDir(dataDir)` + `seed`/`peers` + `CatalogService.declareMaps(...)` → `start()`; `CatalogService.from(node)`; `TransportClusterRpc`; `SeriesHandleRegistry`; `StorageRequestHandler`, `PlacementRequestHandler` registrados via `node.transport().addListener`; `NodeStatusReporter`. Ordem de shutdown em `close()`: reporter → handlers (removeListener) → registry (checkpoint+close de tudo) → node → volume registry. Erros de close logados, não propagados.
- Getters: `nodeId()`, `node()`, `catalog()`, `volume()`, `registry()`, `rpc()`, `isLeader()`, `config()`.

## 3. Testes
Unitários (rodam no `mvn test` padrão):
- `SeriesHandleRegistryTest` (volume real em `@TempDir`, YAML de `src/test/resources/iface-traffic-blob.yaml` — copie do oss se ainda não existir no módulo): abre, `existing`, `closeIdle` com `Clock` controlável, `evictIfOverLimit` (limite 2, abre 3 → 2 abertos, o LRU fechado), `reopenIfKnown` após close por ociosidade, `markMigrating` bloqueia `open`, `close()` fecha tudo; dados escritos antes do close são lidos após reabrir.
- `StorageRequestHandlerTest`: sem rede — instancie o handler com um `CatalogService` sobre `DistributedMap`s de um `NGrid.local(1)`? Se for pesado, crie uma pequena interface interna `PlacementLookup` que o handler consome (com implementação sobre `CatalogService`) e teste com fake. Cobre: dono OK; WRONG_OWNER com e sem hint; NOT_OPEN; MIGRATING; writeBatch multi-série com statuses mistos; exceção vira ERROR.
- `PlacementRequestHandlerTest`: com fakes de `ClusterCoordinator`? A classe é final/complexa — abstraia por interface interna `LeaderView { boolean isLeader(); Optional<String> leaderId(); Set<String> reachableNodeIds(); }` com implementação sobre coordinator+transport, e teste o handler com fake: idempotência, NOT_LEADER, pending zera quando `reportedAt` avança, rajada alterna nós.
Cluster (`*ClusterTest`, profile `ngrrd-cluster`):
- `StorageNodeClusterTest`: 2 `NgrrdStorageNode` (portas pré-alocadas via `ServerSocket(0)`, `peers` em malha) + 1 `NGridNode` "cliente cru" (`roles("client", NodeInfo.ROLE_LEADER_INELIGIBLE)`, `priority(0)`, mapas declarados) + `TransportClusterRpc` a partir dele. Espera líder e 2 `StorageNodeStatus` no catálogo. Para 20 seriesKeys: `PLACE` no líder → `OPEN` no dono (com hint) → `WRITE_BATCH` com 10 amostras → `CHECKPOINT` → `READ` retorna pontos coerentes. Asserções: 20 placements ACTIVE; ambos os nós receberam séries (com `pending` a distribuição deve ser 10/10); `WRITE_BATCH` enviado ao nó errado → `WRONG_OWNER` com `ownerBySeries` correto; `PLACE` enviado a não-líder → `NOT_LEADER`; `READ` de série nunca aberta no dono (após `CLOSE`) → `NOT_OPEN`, e após novo `OPEN` volta a funcionar; segundo `PLACE` da mesma chave devolve o mesmo placement.
- Timeout total do teste ≤ 90 s; nada de sleeps fixos.

## 4. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster -q compile
mvn -pl nishi-utils-ngrrd-cluster test
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster -Dtest=StorageNodeClusterTest -Dsurefire.failIfNoSpecifiedTests=false
```
Rode o `StorageNodeClusterTest` 3 vezes seguidas e reporte cada contagem.

## Relatório
Arquivos criados com `path:linha` dos pontos-chave; qual caminho foi necessário para RPC a si mesmo (loopback ou LocalDispatch); semântica verificada de `NgrrdHandle.close()`; comandos e contagens reais; divergências; NÃO VERIFICADO.
