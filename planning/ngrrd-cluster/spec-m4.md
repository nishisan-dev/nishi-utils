# Spec M4 — drenagem, reconciliação local e administração (estágio 3)

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`, módulo `nishi-utils-ngrrd-cluster`.
Desenho: `planning/ngrrd-cluster.md` (seções 6 e 9). Histórico: `planning/ngrrd-cluster/checkpoint.md`. Pré-requisito: M3 commitado.

## Regras (obrigatórias)
Mesmas dos marcos anteriores: identificadores em inglês, Javadoc/comentários PT-BR, `@Test` PT-BR, cabeçalho GPL, sem commit,
sem `git add -A`, sem `git stash`, Maven só `-pl nishi-utils-ngrrd-cluster` (e `-pl nishi-utils-core` apenas se a spec pedir),
sem sleep fixo em testes, `@Timeout(SEPARATE_THREAD)` nos testes de cluster, gate JaCoCo passando.

## 1. Estados de nó e drenagem (líder)
- `admin/AdminService` (líder): `drain(nodeId)` → `catalog.putNodeStatus(status.withState(DRAINING, now))` (erro se nó desconhecido); `activate(nodeId)` → `ACTIVE`; ambos idempotentes e disparam um ciclo do `Rebalancer`. Após cada ciclo, o `Rebalancer` promove `DRAINING` → `DRAINED` quando `seriesByOwnerLocal().get(nodeId)` está vazio e não há migração em curso com ele como origem.
- `NodeStatusReporter` preserva `DRAINING`/`DRAINED` ao republicar (já faz). Um nó `DRAINED` que reinicia continua `DRAINED` (o operador decide `activate`).
- `PlacementPolicy` já exclui não-`ACTIVE`. `LocalReconciler` (abaixo) não adota séries num nó `DRAINING`/`DRAINED`: reporta como órfãs para o líder decidir (log WARN + contagem).
- Comandos: `ADMIN_DRAIN`/`ADMIN_ACTIVATE` no `AdminRequestHandler` (só líder; `NOT_LEADER` com `leaderNodeId`), resposta `StorageNodeStatus`. Cliente: `NgrrdClusterClient.drainNode(id)`, `activateNode(id)`, `rebalanceNow()` (já no M3), `clusterStatus()` mostra estado, `reachable`, séries e migrações em curso.

## 2. Reconciliação local (`node/LocalReconciler`)
Executa no start do storage node (após o catálogo convergir: espera `placementsLocal()` estável por 2 ticks ou até 30 s) e a cada `reconcileInterval` (default 10 min), e ao virar líder (só para o próprio volume).
Para cada chave de `volume.storage().list("")` que seja uma série (ignorar prefixos internos, se houver):
- Ausente no catálogo → **adoção**: `rpc.call(leader, PLACE, new PlaceRequest(key, definitionHashHex=null, preferredOwnerNodeId=self))`. O `PlacementRequestHandler` já honra `preferredOwner` quando ele é candidato; se o nó não for candidato (DRAINING), a série vai para outro nó e a cópia local vira órfã de migração → NÃO apagar automaticamente neste caso: log WARN `RECONCILE_UNPLACED` e contagem (o operador ativa o nó ou move manualmente). Adoção em lote: até 200 `PLACE` por ciclo, com backoff em `NOT_LEADER`.
- Presente com `ACTIVE` em outro dono → **órfã**: apagar (`storage().delete`) somente se `placement.updatedAt` tem mais de `orphanGrace` (default 5 min) — evita apagar durante um `finish` atrasado.
- Presente `ACTIVE` em self → nada.
- Presente `MIGRATING` (self como origem ou destino) → nada (o coordenador resolve).
- Catálogo `ACTIVE` em self mas ausente no volume → log ERROR `MISSING_SERIES` + contagem em métricas; não altera o catálogo.
Resultado por ciclo: `ReconcileReport(adopted, orphansDeleted, unplaced, missing, durationMs)` exposto em `NodeMetricsSnapshot` e logado `NGRRD_RECONCILE ...`.
Esse é também o caminho de **migração do ngrrd single-node**: apontar um storage node novo para um volume blob existente e subir o cluster adota todas as séries.

## 3. Configuração por YAML e processo do storage node
- `node/StorageNodeConfig.fromYaml(Path|String, Function<String,String> envResolver)`: Jackson YAML + `VariableInterpolator` do oss (`${VAR}`/`${VAR:default}`). Estrutura:
  ```yaml
  node: { id, host, port, priority, dataDir, seed, peers: [host:port] }
  ngrrd:
    volume: { dir, name, shardCount, segmentBytes, initialShardCapacityBytes, capacityBytes }
    statusReportInterval, nodeStatusStaleAfter, handleIdleTtl, maxOpenHandles, requestTimeout,
    defaultDurability, defaultOnGeometryChange
    rebalance: { enabled, interval, minDelta, tolerance, maxConcurrentMigrations, maxMovesPerCycle, migrationTimeout, chunkBytes, maxSeriesBytes }
    reconcile: { interval, orphanGrace }
  ```
  Validação com mensagens claras; teste com YAML completo e com `${VAR:default}`.
- `NgrrdStorageNodeMain` (classe com `main(String[] args)`): `--config <yaml>`; sobe o nó, registra shutdown hook (close ordenado), loga `NGRRD_STORAGE_NODE_STARTED nodeId=... port=...` (marker para Docker ITs futuros). Sem Spring.
- Cliente: `NgrrdClusterConfig.fromYaml(...)` com a mesma abordagem (`client: { id, host, port, dataDir, seed, peers, batchMaxSamples, batchMaxDelay, maxBufferedSamplesPerNode, bufferFullPolicy, requestTimeout, retryTimeout, closeTimeout, leaderWaitTimeout }`).

## 4. CLI de administração (`admin/NgrrdClusterAdminCli`)
`java -cp ... dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli --seed host:port [--client-id x] <status|metrics <nodeId>|drain <nodeId>|activate <nodeId>|rebalance>`
- Entra no cluster como cliente (roles `client`+`leader-ineligible`, `dataDir` temporário apagado ao sair), executa um comando, imprime resultado em texto tabular legível (sem dependência de lib de CLI), sai com código 0/1. `status` imprime líder, nós (id, estado, reachable, séries, bytes, fill%), migrações em curso. Testável: método `int run(String[] args, PrintStream out, PrintStream err)`; teste de cluster chama `run` contra o harness.

## 5. Testes
Unitários: `AdminServiceTest` (drain/activate/idempotência/nó desconhecido/dispara rebalance), `LocalReconcilerTest` (volume real em `@TempDir` + `PlacementLookup`/`ClusterRpc` fakes: adoção, órfã dentro e fora do grace, missing, MIGRATING ignorado, DRAINING não adota), `StorageNodeConfigYamlTest`, `NgrrdClusterConfigYamlTest`, `NgrrdClusterAdminCliTest` (parse de args, saída, código de retorno com `ClusterRpc` fake).
Cluster: `DrainClusterTest` (3 nós + cliente escrevendo continuamente; `drainNode(n2)` → em ≤ 90 s n2 fica `DRAINED` com 0 séries, todas as imagens preservadas por SHA nos novos donos, cliente sem exceção; `activateNode(n2)` + `rebalanceNow()` → n2 volta a receber séries); `AdoptExistingVolumeClusterTest` (cria 20 séries com o oss single-node `Ngrrd.open` num volume; sobe um storage node apontando para esse volume dentro de um cluster de 2 nós; após reconciliação o catálogo tem as 20 séries `ACTIVE` nesse nó e o cliente lê os dados); `AdminCliClusterTest` (`status` e `drain` via `run(...)` contra o harness, saída contém os nós).

## 6. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster verify
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster
```
`DrainClusterTest` 3×, `AdoptExistingVolumeClusterTest` 3×. Relatório: arquivos com `path:linha`, contagens por classe, JaCoCo, divergências, NÃO VERIFICADO.
