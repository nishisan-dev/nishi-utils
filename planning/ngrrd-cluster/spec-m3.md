# Spec M3 — migração de série entre nós e rebalanceamento (estágio 2)

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`, módulo `nishi-utils-ngrrd-cluster`.
Desenho: `planning/ngrrd-cluster.md` (seções 4 e 8). Histórico e notas para o M3: `planning/ngrrd-cluster/checkpoint.md`.
Pré-requisito: M2 commitado (`metrics/`, `AdminRequestHandler`, `NodeMetricsSnapshot`).

## Regras (obrigatórias)
- Identificadores em inglês; Javadoc/comentários em PT-BR; `@Test` em PT-BR; cabeçalho GPL em todo `.java` novo.
- Sem commit, sem `git add -A`, sem `git stash`. Maven só `-pl nishi-utils-ngrrd-cluster`, nunca raiz nem `-am`.
- Não alterar `nishi-utils-core` nem `nishi-utils-oss`. Sem `Thread.sleep` fixo em testes. `@Timeout(SEPARATE_THREAD)` nos testes de cluster.
- Nunca segurar dois locks de entrada do `SeriesHandleRegistry` ao mesmo tempo (ver desenho do M1b). Toda exceção em handler vira resposta de erro.
- Gate JaCoCo (40%) continua passando em `verify`.

## 0. Pré-requisito obrigatório — placement nunca recria uma série existente (defeito pré-existente, risco de dados)
Refuter do M2 reproduziu (A/B, também sem o M2): durante churn de liderança do NGrid, `PlacementRequestHandler.handlePlace`
pode não encontrar no catálogo uma série já colocada (réplica do novo líder ainda convergindo; `LeaderSyncingException`
em `putPlacement`) e criar um placement NOVO em outro nó; o cliente envia `OPEN` com esse `placementHint` e
`StorageRequestHandler.ownership()` o aceita, fazendo `SeriesHandleRegistry.open` criar uma série VAZIA no nó errado.
Resultado: o catálogo passa a apontar para a cópia vazia e os dados reais ficam órfãos no dono antigo.
Correções (todas):
- Líder: `PlacementRequestHandler` só cria placements quando o nó está fora da janela de sincronização —
  `LeaderSyncingException` (ou qualquer falha de `putPlacement`) responde `NOT_LEADER` com `leaderNodeId` (cliente retenta),
  e há um `placementGraceAfterLeadership` (default 3 s, configurável) após `onLeaderChanged` para self durante o qual
  `handlePlace` responde `NOT_LEADER`/retry em vez de criar. Placements existentes continuam sendo respondidos normalmente.
- Nó: `ownership()` deixa de aceitar `placementHint` por si só; a corrida líder→réplica local é resolvida por
  `catalog.placementStrong(key)` (consulta ao líder, já usada no caminho de `WRONG_OWNER`). O hint pode servir só como
  atalho quando `placementStrong` confirma o mesmo dono. Sem confirmação → `WRONG_OWNER(owner ou null)`.
- Nó: criar uma série nova no volume (`Ngrrd.open` de chave inexistente) só acontece via `OPEN` com dono confirmado;
  `writeBatch`/`read` nunca criam (já é assim pela auto-cura só reabrir definições conhecidas — confirme).
- Testes unitários: `handlePlace` em janela de graça → `NOT_LEADER`; `putPlacement` lançando → `NOT_LEADER` e nenhum
  placement gravado; `ownership` com hint não confirmado → `WRONG_OWNER`; com hint confirmado → OK.
- Teste de cluster: `PlacementUnderLeaderChurnClusterTest` — 3 nós + cliente; 30 séries escritas + checkpoint; laço de
  10 rodadas: derrubar e religar o líder (harness), abrir/escrever/ler as mesmas séries com um cliente novo a cada
  rodada; ao final, cada série tem exatamente UMA imagem entre os volumes (`storage().exists`) e a leitura devolve
  todos os pontos. `@Timeout(SEPARATE_THREAD)`, ≤ 180 s.

## 1. Configuração (`StorageNodeConfig`)
`rebalanceEnabled` (true), `rebalanceInterval` (60 s), `rebalanceMinDelta` (50 séries), `rebalanceTolerance` (0,10 = 10% da média),
`maxConcurrentMigrations` (2), `maxMovesPerCycle` (50), `migrationTimeout` (10 min), `migrationChunkBytes` (256 KiB),
`maxSeriesBytes` (64 MiB), `migrationStatusPollInterval` (500 ms). Todos validados no `Builder`.

## 2. Registry (`node/SeriesHandleRegistry`) — complementos
- `discard(String seriesKey)`: fecha (checkpoint+close) e remove a entrada SEM marcar `closedByClient` (usado pelo destino antes de gravar a imagem recebida, e pela origem após `FINISH`). Idempotente.
- `markMigrating`/`clearMigrating` já existem; `markMigrating` continua fazendo checkpoint+close e bloqueando `open`/`withHandle`.

## 3. Executor de migração no nó (`rebalance/MigrationExecutor extends RequestHandlerSupport`, comandos `MIGRATION_COMMANDS`)
Estado por migração em `ConcurrentHashMap<String migrationId, MigrationState>`:
`record MigrationState(String seriesKey, String migrationId, Role role /*SOURCE|TARGET*/, MigratePhase phase, int chunksReceived, int chunksExpected, long bytes, String sha256Hex, String message, long updatedAtMs)`; `enum MigratePhase { STARTED, TRANSFERRING, COMMITTED, FAILED, ABORTED, FINISHED }`. Entradas terminais expiram após 10 min (varredura no tick do reporter).

Origem (src):
- `MIGRATE_START(seriesKey, migrationId, targetNodeId)`: se já existe migração ativa da mesma série com outro id → `MigrateResponse(ERROR)`. Senão: `registry.markMigrating(key)`; lê a imagem `volume.storage().get(key)` (ausente → `ERROR` e `clearMigrating`); se `> maxSeriesBytes` → `ERROR`; calcula SHA-256; responde `MigrateResponse(OK)` IMEDIATAMENTE e executa a transferência numa thread própria (`ngrrd-migration-src`): envia `MIGRATE_CHUNK(seq, total, bytes)` em ordem ao destino via `ClusterRpc` (cada chunk aguarda OK; falha → fase `FAILED` com mensagem), depois `MIGRATE_COMMIT(sha256Hex, totalBytes)`; resposta `COMMITTED` → fase `COMMITTED` na origem; `HASH_MISMATCH`/`ERROR` → `FAILED`.
- `MIGRATE_STATUS(migrationId)`: devolve `MigrateResponse` com o `MigratePhase` mapeado (`COMMITTED`, `PARTIAL` para STARTED/TRANSFERRING, `ERROR` para FAILED/ABORTED, `UNKNOWN` se não conhece).
- `MIGRATE_ABORT`: fase `ABORTED`, `registry.clearMigrating(key)` (a série volta a ser servida aqui; o handle reabre sob demanda).
- `MIGRATE_FINISH`: `registry.discard(key)`; `volume.storage().delete(key)`; fase `FINISHED`; `clearMigrating(key)` (a partir daqui o catálogo aponta para o novo dono e o `ownership()` responde `WRONG_OWNER(newOwner)`). Idempotente: se a imagem já não existe, OK.

Destino (dst):
- `MIGRATE_CHUNK`: cria/atualiza estado `TARGET`; valida `seq == chunksReceived` e `total` consistente; acumula em staging (`ByteArrayOutputStream`, limite `maxSeriesBytes`); OK.
- `MIGRATE_COMMIT`: confere `bytes == totalBytes` e SHA-256; mismatch → `HASH_MISMATCH` + descarta staging + fase `FAILED`. OK → `registry.discard(key)` (se houver handle antigo), `volume.storage().atomicReplace(key, bytes)`, fase `COMMITTED`, responde `COMMITTED`. Idempotente: novo COMMIT do mesmo id após `COMMITTED` responde `COMMITTED`.
- `MIGRATE_STATUS`: como acima. `MIGRATE_ABORT`: descarta staging; se já `COMMITTED`, apaga a cópia (`storage().delete`) e fase `ABORTED`.
- Requisições de cliente para uma série cujo destino ainda não é dono pelo catálogo continuam recebendo `WRONG_OWNER`/`MIGRATING` pelo `ownership()` atual — sem mudança.

## 4. Coordenador de migração no líder (`rebalance/MigrationCoordinator`)
- `CompletableFuture<MigrationResult> migrate(String seriesKey, String src, String dst)`, com semáforo `maxConcurrentMigrations` e pool `ngrrd-migration-coord`. Passos:
  1. `placement = catalog.placementStrong(key)`; exige `ACTIVE` e `owner == src`, senão resultado `SKIPPED`.
  2. `catalog.putPlacement(key, SeriesPlacement.migrating(placement, dst, migrationId, now))`.
  3. `rpc.call(src, MIGRATE_START, ...)` → não OK → passo 6.
  4. Poll `MIGRATE_STATUS` no dst a cada `migrationStatusPollInterval` até `COMMITTED` (→ passo 5), `ERROR`/`HASH_MISMATCH` (→ 6) ou `migrationTimeout` (→ 6). Falha de transporte no poll conta como tentativa e continua até o timeout.
  5. `catalog.putPlacement(key, SeriesPlacement.completed(current, now))`; `rpc.call(src, MIGRATE_FINISH)` (3 tentativas com backoff; falha → log WARN, o reconciliador do M4 apaga a órfã); resultado `COMPLETED(bytes, durationMs)`.
  6. Abort (ordem revista pelo Refuter do M3): primeiro `catalog.putPlacement(key, SeriesPlacement.aborted(current, now))` com pré-condição forte (placement ainda `MIGRATING` com o mesmo `migrationId`; retentado enquanto líder); só se a reversão foi gravada, `rpc.call(src, MIGRATE_ABORT)` e `rpc.call(dst, MIGRATE_ABORT)` (erros só logados). Se a reversão não gravou (perda de liderança ou pré-condição falhou), nenhum ABORT é enviado e o próximo líder resolve via `resumeInFlight`. O passo 5 (`completed`) usa a mesma pré-condição no flip. No destino, ABORT com cópia `COMMITTED` e no `FINISH` da origem, o nó recusa apagar se `placementStrong` disser `ACTIVE(self)`. Resultado `FAILED(reason)`.
  Residual documentado: ABORT que chega ao destino antes do primeiro chunk responde OK sem registrar; chunks atrasados podem recriar o staging e commitar uma cópia órfã (segura) — o reconciliador do M4 apaga órfãs.
- `resumeInFlight()` ao assumir liderança (`LeadershipListener`): para cada placement `MIGRATING` do catálogo local: consulta `MIGRATE_STATUS(migrationId)` no `targetNodeId`: `COMMITTED` → passo 5; qualquer outro (ou inalcançável após 3 tentativas) → passo 6. Ao perder liderança, cancela polls em curso (as migrações seguem no nó; o próximo líder resolve).
- Hooks de teste (package-private): `MigrationHooks { default void beforeComplete(String migrationId) {} default void beforeStart(...) {} }` injetável, para os testes de queda do líder.
- Métricas: `migrationsStarted/completed/failed`, `migrationBytes`, `migrationLatency` (LatencyHistogram do M2), expostas no `NodeMetricsSnapshot` (`migrationsIn`/`migrationsOut` do executor por papel + contadores do coordenador).

## 5. Rebalanceador (`rebalance/RebalancePlanner` puro + `rebalance/Rebalancer` agendado, líder)
- `RebalancePlanner.plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner, Set<String> reachable, Set<String> migratingKeys, RebalanceSettings s)` → `List<Move(seriesKey, src, dst)>`, determinístico (chaves ordenadas):
  1. Nós `DRAINING` (alcançáveis ou não): todas as séries `ACTIVE` deles entram na fila, destino = nó `ACTIVE` alcançável de menor carga corrente (recalculada a cada move planejado). Sem destino → nenhum move de drenagem.
  2. Nós `ACTIVE` alcançáveis: `avg = total/n`; enquanto `max − min > max(rebalanceMinDelta, rebalanceTolerance × avg)`: move uma série (menor chave) do nó mais carregado para o menos carregado, atualizando as cargas.
  3. Ignora chaves em `migratingKeys`. Corta em `maxMovesPerCycle`.
- `Rebalancer`: `ScheduledExecutorService` a cada `rebalanceInterval` (só quando líder; `LeadershipListener` liga/desliga), também disparado por `MembershipListener` (com debounce de 5 s) e por `ADMIN_REBALANCE`. Um ciclo: monta o plano com `catalog.nodesLocal()`, `catalog.seriesByOwnerLocal()`, alcançáveis (coordinator + transport) e chaves `MIGRATING`; submete os moves ao `MigrationCoordinator` respeitando `maxConcurrentMigrations`; espera os futures do ciclo com prazo `migrationTimeout`; loga resumo `NGRRD_REBALANCE moves=... completed=... failed=... skipped=...`. Ciclos não se sobrepõem (flag `running`). `rebalanceEnabled=false` desliga o agendamento (admin.rebalance continua funcionando).
- `ADMIN_REBALANCE` (líder): dispara um ciclo e responde `AdminRebalanceResponse(planned, started)`; `ADMIN_STATUS.migrationsInFlight` passa a refletir o coordenador.

## 6. Cliente — ajustes para dono móvel
- `WriteDispatcher`: ao rerotear por `WRONG_OWNER` com dono conhecido, além de `noteOwner`, notificar o handle (`ownerChanged(seriesKey, newOwner)` via callback registrado pelo cliente) para que `RemoteSeriesHandle.owner` mude e `write()` passe a enfileirar no novo dono (nota do Refuter do M1c: sem isso cada lote seguinte é reroteado e prependido, invertendo a ordem). Teste: após o reroteamento, novas escritas vão direto ao novo dono e a ordem por série se mantém.
- `MIGRATING` em write/checkpoint/read: já retenta com backoff exponencial até `retryTimeout`; confirmar que a resposta `WRONG_OWNER(newOwner)` após o flip do catálogo é tratada sem novo `PLACE` (o `PlacementResolver` já compara `updatedAt`).
- `DefaultNgrrdClusterClient.connect`: passa a exigir líder + pelo menos UM storage node conectado (não todos), logando WARN para os demais; a conectividade por operação já é coberta pela retentativa de transporte.

## 7. Testes
Unitários: `RebalancePlannerTest` (drenagem primeiro, tolerância, minDelta, determinismo, `maxMovesPerCycle`, chaves em migração ignoradas, nó inalcançável nunca é destino), `MigrationExecutorTest` (volume real em `@TempDir`: origem lê e fragmenta; destino reconstrói e valida SHA; mismatch descarta; commit idempotente; abort após commit apaga cópia; finish apaga na origem; `markMigrating` bloqueia writes durante a transferência), `MigrationCoordinatorTest` com `ClusterRpc` fake (feliz; erro no START → abort e catálogo revertido; `HASH_MISMATCH` → abort; timeout → abort; `resumeInFlight` com `COMMITTED` → completa; com `PARTIAL` → aborta; semáforo respeita `maxConcurrentMigrations`), `WriteDispatcherTest` (ownerChanged).
Cluster (`-Pngrrd-cluster`):
- `RebalanceClusterTest`: 2 nós + cliente; 40 séries escritas e checkpointadas; captura SHA-256 da imagem de cada série no dono (`volume.storage().get`); `harness.addStorageNode(...)`; `client.rebalanceNow()` (ou `rebalanceInterval` curto); espera até o 3º nó ser dono de ≥ 8 séries (≤ 90 s); para cada série movida: SHA no novo dono == SHA capturado, imagem ausente no antigo, catálogo `ACTIVE` no novo; durante o rebalanceamento o cliente segue escrevendo (thread de escrita contínua) sem exceção; ao final, `checkpoint` + `read` em todas as 40 séries devolvem os dados (inclusive amostras escritas durante a migração).
- `LeaderFailoverDuringMigrationClusterTest`: 3 nós + cliente; hook `beforeComplete` bloqueia o coordenador após `COMMITTED` no destino; derruba o líder; o novo líder (`resumeInFlight`) completa a migração (catálogo `ACTIVE` no destino, imagem apagada na origem em ≤ 60 s). Segundo caso com hook `beforeStart` (nada transferido): novo líder aborta, série segue `ACTIVE` na origem e íntegra (SHA igual).

## 8. Verificação
```
mvn -pl nishi-utils-ngrrd-cluster verify
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster -Dtest='RebalanceClusterTest,LeaderFailoverDuringMigrationClusterTest,DistributedWriteReadClusterTest,NodeRestartClusterTest' -Dsurefire.failIfNoSpecifiedTests=false
```
`RebalanceClusterTest` 3× e `LeaderFailoverDuringMigrationClusterTest` 3×. Relatório: arquivos com `path:linha`, contagens reais por classe, JaCoCo, divergências, NÃO VERIFICADO.
