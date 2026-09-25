# Plano — #177 (WRONG_OWNER em pingue-pongue) e #169 (desconexões entre storages) — release 8.7.0

## Contexto

**#177 (8.6.0, produção TEMS):** com ingestão contínua, o rebalance `storage-a → storage-c` deixou séries presas num `WRONG_OWNER` que alterna a/c por minutos. Escritas nunca confirmadas → checkpoint "prazo de retentativa esgotado (último status: WRONG_OWNER)" → barreira de flush estoura → consumidor reinicia. Causas mapeadas no código:
1. **Storage** — `StorageRequestHandler.ownership()` (`node/StorageRequestHandler.java:608-615`) responde `WRONG_OWNER(src)`/`MIGRATING(src)` a partir da **réplica local** do catálogo, sem confirmar no líder. O destino com réplica atrasada aponta para a origem; a origem (esquecida, consulta o líder) aponta para o destino.
2. **Cliente** — `WriteDispatcher.applyStatus` (`client/WriteDispatcher.java:600-637`) e `RemoteSeriesHandle.noteWrongOwner` (`:626-639`) seguem a dica cegamente, com backoff fixo `backoffMin`, `extract` zerando tentativas (`:907`), laço quente quando `newOwner == owner`, e nunca consultam o líder. `PlacementResolver.noteOwner` carimba a dica com o relógio do cliente, fazendo-a parecer mais fresca que o catálogo.
3. **Observabilidade** — o `HIGH_REPLICATION_LAG` global é artificial: o líder usa um HWM rastreado obsoleto (`NGridNode.java:734-738`), e há desincronia de escala (tems#9/D9). O lag confiável é o por tópico (`ReplicationManager.getTopicReplicationStatuses()` para `map:ngrrd.catalog`), que não é exposto.
4. **CLI administrativo** permanece membro após sair (NGrid não tem LEAVE; `ClusterCoordinator.java:1556-1560`).

**#169 (pendência secundária):** falhas `PeerDisconnectedException` em `ngrrd.migrate.chunk` quando um nó (cliente) religa. Hipótese principal (H1, ancorada no código): o nó que entra propaga aliases de seed não resolvidos (`host:port`) no handshake/PEER_UPDATE; o storage troca o peer canônico pelo alias em `knownPeers` (`TcpTransport.java:379-386, 679-686, 736-743`), disca de novo para o mesmo processo sob outra chave, o outro lado fecha a conexão original pelo desempate e `handleDisconnect` (`:846-881`) confirma a queda e falha as respostas pendentes.

**Decisões do usuário:** lag por tópico no ngrrd + correção do lag do líder no core (desincronia de escala vira issue separada); LEAVE gracioso incluído; #169 investigada com reprodução; versão **8.7.0**.

## Execução

- Branch `fix/177-169-wrong-owner-transporte` a partir do `main` (se houver outro agente ativo na árvore, usar worktree paralelo). Um PR, commits atômicos. Maven sempre com `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`.
- Builders (sonnet) por fatia com spec autocontida; Refuter (opus) por fatia reexecuta os testes e tenta refutar. Core e ngrrd são fatias disjuntas e podem correr em paralelo (worktrees); o teste do CLI (A5) depende do core instalado (`mvn -pl nishi-utils-core clean install`).
- Idioma: identificadores em inglês; comentários/Javadoc/logs novos no ngrrd em PT-BR (padrão do módulo). **Divergência sinalizada:** `TcpTransport`, `ClusterCoordinator`, `NGridNode` e codecs têm comentários em inglês — seguir o padrão do arquivo (inglês) nesses arquivos. Commits/CHANGELOG/docs em PT-BR, sem atribuição a agente.
- TDD: cada correção começa pelo teste que reproduz a falha (vermelho), depois a mudança.

## Parte 1 — Core NGrid

### C. #169 — aliases de seed não derrubam links estabelecidos (primeiro, porque A3 mexe nos mesmos laços)
- **C0 (repro):** em `TcpTransportConcurrentMeshTest`, `joiningPeerWithUnresolvedSeedAliasKeepsEstablishedLinks` (storage-a/storage-c com mesh estável, request lento a→c em voo, client-1 com peers `127.0.0.1:pA`/`127.0.0.1:pC` e discagem ao alias de C bloqueada pelo `beforeDialHook`; broadcasts periódicos de a). Assertivas: `a.peers()` mantém `storage-c` e nunca o alias; nenhuma discagem de a ao alias; request em voo completa normalmente. Variante via `PEER_UPDATE` de um terceiro. Se H1 não reproduzir, investigar H2–H6 (desempate de conexões do mesmo lado, `handleDisconnect` ignorando sockets vivos não rastreados, `connectionLocks.remove` no disconnect, `send()` ignorando `shouldInitiate`, frame ultrapassando o handshake) com testes próprios antes de corrigir.
- **C1 `fix(ngrid): alias de seed não substitui peer canônico já verificado`** — unificar os três laços de merge em `mergeGossipedPeer(NodeInfo)`, com conjunto `verifiedPeers` preenchido pelo handshake; alias gossipado no host:port de um peer verificado é descartado. A limpeza no topo do handshake (8.5.0, `:632-649`) continua autoritativa.
- **C2 `fix(ngrid): não propaga aliases de seed não resolvidos`** — `gossipablePeers()` (knownPeers menos initialPeers não verificados) em `sendHandshake` (`:611`) e `broadcastPeerList` (`:702`). Protege storages 8.6.0 de clientes novos.
- **C3 (endurecimento, cada um com teste):** não remover `connectionLocks` no disconnect (só em forget/LEAVE); em `handleDisconnect`, antes de falhar pendentes, verificar se há outra conexão aberta e identificada para o mesmo peer (teste `PendingResponseDisconnectGraceTest` com `RawPeer`).
- **E2E:** `ClientRejoinMeshStabilityTest` (3 storages + cliente inelegível com `.peers(todos)`, reiniciado 3× com o mesmo id; zero "Disconnect confirmed for storage-" e request storage→storage em voo sem falha) — roda no `-Presilience`.

### B. `fix(ngrid): lag de replicação do líder é zero no snapshot operacional`
- `NGridNode.operationalSnapshot` (`:734-738`): helper estático `replicationLag(isLeader, trackedHwm, lastApplied)` → 0 no líder; no líder reportar `getAdvertisedHighWatermark()` como HWM rastreado (documentar no Javadoc de `NGridOperationalSnapshot`). `NGridAlertEngine.evaluateReplicationLag` (`:186`) retorna cedo se `snapshot.isLeader()`.
- Testes: `NGridOperationalSnapshotTest` (líder com HWM obsoleto 4.761.359 → 0; seguidor → lag positivo), `NGridAlertEngineTest` (líder não dispara `HIGH_REPLICATION_LAG`).
- Abrir issue separada da desincronia de escala (seed soma por tópico em `ReplicationManager:679-690` × commit por máximo em `:2013`), com sintoma, locais e critério de aceite.

### A. LEAVE gracioso
- **A1 `feat(ngrid): mensagem LEAVE e capacidade supportsLeave no handshake`** — `MessageType.LEAVE` no fim do enum; `LeavePayload(NodeInfo node, String reason)`; `HandshakePayload.supportsLeave` (ausente = false); `Connection.peerSupportsLeave`. Compatibilidade verificada: 8.3.0+ descarta tipo desconhecido com WARNING sem fechar conexão (`READ_UNKNOWN_ENUM_VALUES_AS_NULL`), e o flag evita até isso. Testes: `ProtocolCompatibilityIntegrationTest` (handshake legado → false), `JacksonMessageCodecTest` (round-trip).
- **A2 `feat(ngrid): envio best-effort de LEAVE no close do transporte`** — `TcpTransportConfig.leaveOnClose` (true) e `leaveFlushTimeout` (~500 ms); em `close()`: marca `leaving` (rejeita novas conexões em `registerConnection`), envia LEAVE às conexões com `peerSupportsLeave` aguardando flush por conexão, depois fecha como hoje. Teste novo `TcpTransportLeaveTest`.
- **A3 `feat(ngrid): peers esquecem membro efêmero que saiu`** — esquecimento completo só para membro inelegível a líder ou porta ≤ 0 (votantes seguem o caminho atual de desconexão, para não encolher maioria sem consenso). No receptor: `knownPeers.remove`, tombstone com TTL (`departedPeerTombstoneTtl`, 10 min; limpo por handshake direto do mesmo id, `addPeer` ou expiração), `NetworkRouter.forget`, falha dos pendentes, `TransportListener.onPeerLeft` (default → `onPeerDisconnected`). Gossip (handshake/PEER_UPDATE/reachability) ignora ids com tombstone — plugado no `mergeGossipedPeer` de C1. `ClusterCoordinator.onPeerLeft`: `members.remove` + limpeza de estado do peer + `recomputeLeader`; notifica listeners de membership só se estava ativo (sem evento extra para o debounce do `Rebalancer`); HEARTBEAT de id com tombstone é ignorado. Testes: `TcpTransportLeaveTest` (esquece, não redisca, PEER_UPDATE não ressuscita, mesmo id volta via handshake), `LeaveMembershipTest` (novo), extensão de `DepartedMemberQuorumElectionTest`.
- **A4 `feat(ngrid): retransmissão única de LEAVE`** — no primeiro recebimento, repassar aos vizinhos diretos com suporte (exceto origem e remetente); a tombstone deduplica. Teste com topologia A–B–C.
- **A5 (ngrrd)** — `AdminCliClusterTest` (`-Pngrrd-cluster`): após várias execuções do CLI, nenhum storage mantém `ngrrd-cluster-admin-*` em `transport().peers()`.

## Parte 2 — ngrrd-cluster (#177)

### 1. `feat(ngrrd): lag da réplica do catálogo no status do nó`
- Novo `catalog/CatalogReplicaStatus` (record serializável: `leader, lag, leaderHighWatermark, nextExpectedSequence, syncing, pendingBootstrap, streaming`; `ofLeader()`, `from(...)`, `lagKnown()` = líder ou hwm > 0, `caughtUp(maxLag)`).
- `StorageNodeStatus` ganha `catalogReplica` **anulável** (null = versão antiga/não reportado), com construtor de compatibilidade e propagação em `withLoad`/`withState`.
- `NodeStatusReporter.catalogReplication(Supplier)` alimentado em `NgrrdStorageNode` (~`:221`) com `node.replicationManager().getTopicReplicationStatuses().get("map:ngrrd.catalog")`; `catalogLag=` no `NGRRD_NODE_STATUS`.
- CLI `status`: coluna `CAT_LAG` (`lider`, `<n>`, `sync`, `boot`, `?`, `-`).
- Testes: `StorageNodeStatusTest` (JSON 8.6.0 → null, round-trips), `NodeStatusReporterTest`, `NgrrdClusterAdminCliTest`.

### 2. `fix(ngrrd): storage confirma no líder o redirecionamento derivado da réplica local`
- `PlacementLookup` do handler ganha `placementsAtLeader(keys, maxWait)` (lote, com prazo) e `localIsAuthoritative()`; adaptador em `NgrrdStorageNode` (`:175-186`) reutiliza `CatalogLookupClient` (`ngrrd.catalog.lookup`) com prazo explícito — **não** usar `placementStrong` aqui (até ~100 s de bloqueio em `DistributedMap.invokeLeader`).
- `ownership()` → `ownershipBatch()`: todo redirecionamento derivado da réplica (`ACTIVE(outro)` e `MIGRATING` quando não é origem copiando) é confirmado no líder, exceto se este nó é o líder. `handleWriteBatch` faz **uma** consulta por request. Prazo `OWNER_CONFIRMATION_TIMEOUT` = 2 s.
- Resultado: líder encontrou → `ownershipFromLeader`; ausente → `WRONG_OWNER(null)` + cache negativo existente; falha → resposta local (comportamento 8.6.0), contador de falha e cooldown de 1 s. Líder diz `ACTIVE(self)` sem handle → `OK` → `NOT_OPEN` → o cliente reabre e o OPEN abre o objeto já commitado (nada é criado).
- Cache positivo (TTL 5 s, teto 100k, válido só se o `updatedAtEpochMs` local não passou do confirmado), invalidado por listener de mudança de posse em `SeriesHandleRegistry` (`beginMigrationCopy`, `markMigrating`, `clearMigrating`, `forget`, `discard`). **Resposta do cache nunca marca `confirmedByLeader`** (preserva a proteção da #174 no caminho de criação).
- Métricas: `redirectConfirmations`, `redirectOverrides`, `redirectConfirmationFailures`, `redirectCacheHits` em `StorageHandlerMetrics`/`NodeMetricsSnapshot`/`NGRRD_NODE_STATUS`/CLI `metrics`; log FINE `NGRRD_OWNER_REDIRECT_OVERRIDE`.
- Testes em `StorageRequestHandlerTest` (réplica `ACTIVE(outro)`/`MIGRATING` com líder `ACTIVE(self)`, terceiro dono, lote com uma consulta, cache/TTL/invalidação, cache não autoriza criação, líder indisponível + cooldown, líder local sem consulta); ajustar o teste `serieEsquecidaComReplicaLocalJaConvergida...` (~`:411-430`), que passa a esperar uma confirmação.

### 3. `fix(ngrrd): cliente detecta dicas de dono contraditórias e resolve no líder` (3a/3b/3c)
- **3a `PlacementResolver`:** overrides com flag `authoritative`; `resolveExistingAtLeader(Collection, maxWait)` em lote; `noteOwner` não sobrepõe override autoritativo recente (2 s) e carimba a dica com o maior `updatedAt` conhecido, não com o relógio do cliente.
- **3b `WriteDispatcher`:** `SeriesRoute` ganha episódio de redirecionamento (`redirectedBy`, `redirectAttempts`, `confirmedOwner`, `ownerLookupPending`). Dica contraditória = aponta para o próprio nó, para nó já visitado no episódio, diverge do dono confirmado, ou ≥ 4 saltos (a cadeia legítima A→B→C do `WriteBarrierRegressionTest` continua sem consulta). Contraditória → série pausada sozinha, consulta ao líder **coalescida** (fila + uma tarefa por vez) e aplicação por série (ACTIVE → reroteia e fixa; MIGRATING → dono; ausente → `reopenAsync`; falha → permanece, backoff). Backoff exponencial por série via `redirectAttempts` (atravessa nós); `extract` passa a carregar as tentativas; OK zera o episódio. Corrige o laço quente.
- **3c `RemoteSeriesHandle`:** `OperationRetry` com o mesmo episódio; `noteWrongOwner(target, hint, retry)` consulta o líder na contradição, dentro do prazo existente.
- Testes: `WriteDispatcherTest` (A↔C converge com consulta e sem novo envio a A; auto-redirecionamento sem laço; backoff crescente; falha da consulta não bloqueia outra série; cadeia legítima sem consulta; OK zera episódio), `WriteBarrierRegressionTest.checkpointConcluiApesarDeDicasContraditorias` (reprodução direta), `RemoteSeriesRetryBudgetTest` (uma consulta; variantes somente leitura e falha), `PlacementResolverTest`.

### 4. `feat(ngrrd): rebalance não escolhe destino com réplica do catálogo atrasada`
- Config `ngrrd.rebalance.maxDestinationCatalogLag` (padrão 1000; `-1` desliga; `0` exige réplica em dia), em `StorageNodeConfig`/`RebalanceSettings` com construtores de compatibilidade.
- `rebalance/CatalogLagGate.exclusionReason(status, maxLag)`: nó sem campo (8.6.0) = elegível; lag desconhecido/sync/bootstrap/acima do limite = excluído como **destino** (continua origem e no cálculo de distribuição).
- `Rebalancer.buildPlan` (cobre `triggerNow`, `runCycle` e membership) passa as exclusões aos planners (guarda `receivers.isEmpty()` no `CapacityAwarePlanner`) e loga `NGRRD_REBALANCE_DEST_EXCLUDED`; `MigrationCoordinator.runMigration` rechecagem na execução → `SKIPPED` (não em `resumeInFlight`).
- `AdminRebalanceResponse.excludedDestinations`; novo `api/RebalanceTrigger` + `NgrrdClusterClient.triggerRebalance()` (default compatível; `rebalanceNow()` delega); CLI `rebalance` imprime planejados/iniciados/exclusões.
- Testes: `RebalancePlannerTest`, `RebalancerTest`, `MigrationCoordinatorTest`, testes de config YAML, `AdminRequestHandlerTest`, `NgrrdClusterAdminCliTest`.

### 5. `test(ngrrd): destino com réplica do catálogo atrasada (#177)`
- Gancho de teste `NgrrdStorageNode.start(..., UnaryOperator<PlacementLookup> lookupDecorator)` + overload no `NgrrdClusterTestHarness` (o core não permite pausar replicação; limitação documentada no Javadoc do teste).
- `StaleReplicaRedirectClusterTest` (3 nós, ingestão contínua, destino não líder com réplica congelada em `ACTIVE(src)` e, no caso 2, em `MIGRATING(src→dst)`): checkpoint conclui, zero falhas, todas as amostras confirmadas, `redirectOverrides ≥ 1`. Deve falhar antes da parte 2.2.
- Estender `AdminStatusClusterTest`: todos com `catalogReplica`, exatamente um líder, seguidores com `lagKnown()`.

## Parte 3 — Versão, docs e fechamento
- `chore: 8.7.0` nos POMs (raiz, core, oss, ngrrd-cluster, ngrid-test).
- `doc/CHANGELOG.md` (entrada 8.7.0); `doc/oss/ngrrd-cluster.md` (semântica de WRONG_OWNER confirmada no líder, `maxDestinationCatalogLag`, métricas); `doc/oss/ngrrd-cluster-operacao.md` (coluna `CAT_LAG`, saída do `rebalance`, troubleshooting "WRONG_OWNER alternando entre nós", CLI não fica mais como membro, ordem de upgrade: storages → clientes); versões em quickstart/README; `doc/oss/diagrams/ngrrd_cluster_sequence_write.puml` com a confirmação no líder; doc do NGrid sobre LEAVE.
- Copiar este plano para `planning/2026-09-25-issues-177-169-wrong-owner-transporte.md`.
- PR com descrição em PT-BR; após merge, release 8.7.0 via `gh release create` antes da tag. Comentar nas issues #169/#177 (pedir nova validação no TEMS). A #169 só é fechada se a reprodução C0 confirmar a causa.

## Verificação
- Unitários por fatia: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-core test -Dtest=TcpTransportConcurrentMeshTest,TcpTransportLeaveTest,ProtocolCompatibilityIntegrationTest,LeaveMembershipTest,NGridOperationalSnapshotTest,NGridAlertEngineTest` e `mvn -pl nishi-utils-ngrrd-cluster test -Dtest=StorageRequestHandlerTest,WriteDispatcherTest,WriteBarrierRegressionTest,RemoteSeriesRetryBudgetTest,PlacementResolverTest,Rebalance*Test,NgrrdClusterAdminCliTest`.
- Cada teste de reprodução (C0, `checkpointConcluiApesarDeDicasContraditorias`, `StaleReplicaRedirectClusterTest`) deve falhar antes da correção e passar depois; conferir por mutação/revert pontual.
- Suíte completa: `mvn clean install` (conferir contagem de testes vs. main: 807+/810+), `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1` (flaky conhecidos: `RelayStreamReplicationTest`), `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster`, `mvn verify -Pngrrd-integration` (falha pré-existente conhecida: `IfaceTrafficSmokeIT` yearly), `mvn verify -Pvalidate-javadoc`.
- Pedir ao TEMS (via usuário) a confirmação da configuração `peers` do client-1 e, se possível, linhas `Handling disconnect from <host:port>` dos logs de 19:29:03 para corroborar H1.
