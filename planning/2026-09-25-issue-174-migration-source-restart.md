# Issue #174 — Origem de migração reiniciada recria série vazia; marcas em memória sem limite

Versão alvo: **8.6.0** (ainda não publicada; a release só sai depois desta issue). Base: `origin/main`
(`a3f8e1a`, #175). O `main` local está 2 commits atrás e precisa de `git pull --ff-only` antes do branch.

## Contexto

Ao concluir uma migração, a origem chama `SeriesHandleRegistry.forget` e apaga o objeto. A marca
`forgotten` (e `closedByClient`) vive só em memória. Se a origem reiniciar logo após o `MIGRATE_FINISH`
com a réplica local do catálogo ainda em `ACTIVE(self)`, um `OPEN` com `createIfMissing=true` (padrão, o
caminho da ingestão) passa por `ownership()` com a réplica local e `registry.open` **recria a série vazia
no dono antigo** — as escritas da janela vão para a órfã e se perdem quando o `LocalReconciler` a remove.
A #171 já fechou o caso `createIfMissing=false` (`confirmSeriesNotFound`). Além disso, `forgotten` e
`closedByClient` só perdem a chave num novo `open`, então crescem sem limite.

**Verificação pedida no comentário da issue (feita):** um storage recém-reiniciado **aceita** `OPEN`
durante o catch-up da réplica. `NGridNodeBuilder.start()` não espera o follower convergir
(`ReplicationManager.start()` só agenda os loops de relay), os handlers são registrados logo depois, e
`DistributedMap.getOptional(key, EVENTUAL)` lê o mapa em memória sem nenhuma checagem de sync
(`DistributedMap.java:454-492`, `MapClusterService.java:268`). `isLeaderSyncing()` é o gate de escrita
do líder recém-promovido, não um estado de follower. A janela do item 1 **não** encolhe → prioridade
mantida.

## Desenho

### 1. Confirmação no líder no caminho de criação do `OPEN` (alternativa 3)
`StorageRequestHandler` (`nishi-utils-ngrrd-cluster/.../node/StorageRequestHandler.java`):

- `Ownership` ganha `boolean confirmedByLeader` — `true` quando a decisão veio de `placementStrong`
  (caminho do hint, réplica local vazia, `ownershipForgotten`); `false` quando veio da réplica local ou
  de `registry.isOpen`.
- Em `openWithMetadata`, depois da checagem de prefixo: se **não** está aberta e o objeto **não existe**
  no volume, então:
  - `createIfMissing=false` → comportamento atual (`confirmSeriesNotFound`).
  - `createIfMissing=true` e `!ownership.confirmedByLeader()` → confirmar no líder antes de criar.
    `ACTIVE(self)` → segue e cria; outro dono → `WRONG_OWNER(dono)`; `MIGRATING` → `MIGRATING`;
    sem placement → `WRONG_OWNER(null)`; falha da consulta → `ERROR` (nunca cria às cegas).
  - `createIfMissing=true` e `confirmedByLeader` → cria direto (sem RPC duplicado).
- Refatorar `confirmSeriesNotFound` num único `confirmOwnerWithLeader(seriesKey)` que devolve o
  redirecionamento ou "confirmado `ACTIVE(self)`"; o chamador decide `NOT_FOUND` vs. criar. Mesma
  regra nos dois modos, um único ponto de verdade.
- Tudo continua sob `registry.operationLock(seriesKey)` (já tomado em `handleOpen`), que a migração
  também usa — sem nova janela de corrida.
- **Custo resultante:** série nova paga no máximo 1 leitura forte além do `PLACE`, e **zero a mais**
  quando a réplica do dono ainda não recebeu o placement (esse caso já ia ao líder pelo hint). O
  cliente já trata `WRONG_OWNER(dono)` no `OPEN` com `noteOwner` + retry direto no dono novo
  (`RemoteSeriesHandle.java:238-241`) — nenhuma mudança de cliente/protocolo.

### 2. Métrica do custo
`StorageHandlerMetrics` (record aninhado em `StorageRequestHandler`) ganha
`leaderConfirmations` (contagem de `placementStrong` feitos pela confirmação de criação/`NOT_FOUND`) e
`leaderConfirmationLatency` (`LatencyHistogram`/`LatencySnapshot`, padrão dos demais). Propagar para
`NodeMetricsSnapshot` e o log `NGRRD_NODE_STATUS` pelo mesmo caminho de `flushes`/`checkpoints`
(`NodeStatusReporter`). Serve à medição e à operação.

### 3. Fim do crescimento sem limite das marcas
`SeriesHandleRegistry` (`.../node/SeriesHandleRegistry.java`):

- **`closedByClient` deixa de existir.** `close(seriesKey)` passa a remover a definição em cache
  (`hashBySeriesKey.remove`) — sem definição, `reopenIfKnown` já não reabre. A checagem que hoje
  consulta `closedByClient` **dentro do lock** da entrada em `reopenIfKnown` vira a reconferência
  `hashBySeriesKey.containsKey(seriesKey)` sob o mesmo lock (fecha a corrida close×reopen do mesmo
  jeito). `open` continua sendo o único caminho de volta (recacheia a definição). `cachedYaml` só é
  usado em testes. Resultado: nenhum conjunto por série fechada; `hashBySeriesKey` fica limitado às
  séries abertas neste processo e não fechadas/esquecidas.
- **`forgotten` ganha validade por convergência da réplica**: a marca só protege enquanto a réplica
  local pode ainda dizer `ACTIVE(self)`. Quando a réplica local mostra a série com **dono ≠ self**, a
  marca é descartada:
  - preguiçoso em `ownership()`: `isForgotten` + réplica local com outro dono → descarta a marca e segue
    o caminho normal (`WRONG_OWNER(dono)` sem RPC ao líder — economiza a leitura forte de escritas
    atrasadas);
  - varredura periódica: `registry.pruneForgotten(Predicate<String> ownedElsewhere)` chamado por
    `StorageRequestHandler#pruneForgottenMarks()` (usa `placementLookup.placementLocal`), agendado no
    `tick()` do `NodeStatusReporter` ao lado de `registry.closeIdle()` (via `Runnable` injetado,
    mantendo os construtores de compatibilidade). Cobre as chaves que nunca mais recebem requisição.
  - `forgotten` é mantida (não removida): cobre o caso anômalo em que o `delete` do `FINISH` falhou e o
    objeto ainda existe localmente — aí a confirmação de criação (item 1) não dispara.

### 4. Medição em criação em massa
Novo `BulkCreateLeaderCostClusterTest` (perfil `ngrrd-cluster`, 3 storages in-process via
`NgrrdClusterTestHarness`): cria N séries (default 20 000, parametrizável por system property) com
`createIfMissing=true`, concorrência controlada, e registra: latência de `open` (p50/p99/total),
`placeCount` do cliente (`ClientMetricsSnapshot`) e `leaderConfirmations` somados dos storages.
Asserções de sanidade só (todas criadas, `leaderConfirmations ≤ N`); números vão para o log.
A/B: rodar o mesmo teste (sem as asserções da métrica nova) num worktree em `a3f8e1a` para a linha
de base. Resultados documentados em `doc/oss/ngrrd-cluster-operacao.md` (custo operacional) e no
plano em `planning/`.

## Testes (TDD — escrever falhando antes)

`StorageRequestHandlerTest` (fake `PlacementLookupFake` já conta `strongCalls`):
- `openCriandoComReplicaAtrasadaAposReinicioRespondeWrongOwnerENaoRecria` — **critério de aceite**:
  registry novo (sem `forgotten`, simula reinício), local `ACTIVE(SELF)`, strong `ACTIVE(OTHER)`,
  objeto ausente, `createIfMissing=true` → `WRONG_OWNER(OTHER)`, objeto não criado, 1 `strongCall`.
- criando com strong `MIGRATING` → `MIGRATING`; sem placement no líder → `WRONG_OWNER(null)`; falha do
  strong → `ERROR` e nada criado.
- série nova legítima (local e strong `ACTIVE(SELF)`) → cria, 1 `strongCall`.
- réplica local vazia + hint → cria com **1** `strongCall` (sem duplicar).
- objeto já existe / série já aberta → nenhum `strongCall` a mais.
- `forgotten` + réplica local com outro dono → `WRONG_OWNER` sem `strongCall` e marca descartada.
- `pruneForgottenMarks` descarta só as chaves com outro dono na réplica local.
- métricas `leaderConfirmations`/latência incrementam.

`SeriesHandleRegistryTest`: ajustar `closeExplicitoDoClienteImpedeReopenIfKnownAteNovoOpen`
(mesma semântica, sem conjunto); novo teste de corrida close×reopenIfKnown; `pruneForgotten`;
`close` remove a definição em cache e `open` a restaura.

Cluster (`-Pngrrd-cluster`): cenário de reinício da origem em `NodeRestartClusterTest` ou novo
`MigrationSourceRestartClusterTest` — migrar série, reiniciar a origem (`harness.restartStorageNode`),
`OPEN` criando direcionado à origem → não recria o objeto lá. Reprodução do atraso da réplica num
cluster real não é determinística; o determinismo fica no teste unitário e o cluster valida o fluxo
ponta a ponta. Mais o `BulkCreateLeaderCostClusterTest`.

## Documentação
- `doc/oss/ngrrd-cluster-operacao.md`: substituir "Limitação conhecida (issue #174)" pela descrição da
  confirmação no caminho de criação, custo medido e validade das marcas.
- `doc/oss/ngrrd-cluster.md`, seção 8 (migração/`finish`): origem reiniciada não recria a série.
- `doc/CHANGELOG.md`: acrescentar a #174 à entrada 8.6.0.
- Diagrama PlantUML de sequência do `OPEN` com confirmação no líder, se já houver diagrama do fluxo
  de `OPEN` em `doc/`; senão, incorporar ao doc existente mais próximo.
- `planning/2026-09-25-issue-174-migration-source-restart.md` com este plano + resultados da medição.

## Execução
1. `git pull --ff-only` no `main`; branch `fix/174-migration-source-restart`.
2. Commits atômicos (PT-BR, sem atribuição a agente, nunca `git add -A`):
   1. confirmação no líder no caminho de criação + `Ownership.confirmedByLeader` + testes;
   2. métricas `leaderConfirmations`/latência + propagação ao status do nó + testes;
   3. remoção de `closedByClient` (definição em cache como marca) + testes;
   4. validade de `forgotten` (preguiçosa + varredura) + testes;
   5. teste de cluster de reinício da origem + `BulkCreateLeaderCostClusterTest`;
   6. docs/CHANGELOG/planning com a medição.
3. Refuter (agente `opus`) revisa o diff e reexecuta os testes antes de declarar pronto.
4. PR para `main`; a release 8.6.0 (tag + `gh release create`) fica para depois do merge.

## Verificação
Sempre com `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`:
- `mvn -pl nishi-utils-ngrrd-cluster -am install -DskipTests` e
  `mvn -pl nishi-utils-ngrrd-cluster verify` — conferir contagem de testes, não só BUILD SUCCESS.
- `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster` (inclui reinício da origem e a medição;
  rodar a medição também no worktree em `a3f8e1a` para A/B).
- `mvn test` no raiz para garantir que nada fora do módulo quebrou.
