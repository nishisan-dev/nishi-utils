# PLANO DE IMPLEMENTAÇÃO REFINADO — #124 Relay-log no follower

> Consolida os 4 designs de fase (IO path, Apply consumer, Snapshot/Failover, Wiring/NQueue) num plano executável fase a fase com commits atômicos.
> Base: `feature/ngrid-relay-log-follower` sobre 4.3.0. Fundações: #122 (op-log temporal) + #123 (sync-before-lead gate) já mergeadas.
> Design doc fechado: `planning/issue-124-relay-log-follower.md` (decisões A–F). Gate de extensão NQueue aprovado.

---

## 1. RESUMO DAS DECISÕES JÁ FECHADAS (gate + A–F)

O follower passa a **desacoplar recepção de aplicação**: cada `REPLICATION_REQUEST` é persistido num **relay-log NQueue por tópico** (decisão C) em ordem de chegada com chave `<epoch>-<sequence>` (decisão B), drenado por um **apply-consumer no próprio ritmo** via `peek → fence → apply → poll` (decisão D), aposentando o `sequenceBufferByTopic`/`resetState` em regime (cutover, decisão A). Snapshot/`resetState` só no **bootstrap** (relay+estado vazios) ou **lag > retenção** (decisão E); `apply < produção` sustentado vira replica-lag medido, não espiral. O modo é uma flag **do follower** `FollowerIngestMode { INLINE, RELAY_LOG }`, default INLINE, opt-in (decisão F). Pelo **gate aprovado**: relay é `NQueue<byte[]>` com envelope `RelayEntry(epoch,sequence,topic,operationId,byte[] payloadBytes)` (payload pelo codec Jackson canônico, pois `ReplicationPayload`/`data` não são `Serializable`); invariantes do relay `allowShortCircuit=false` (default é TRUE), `enableMemoryBuffer=false`, `RetentionPolicy.TIME_BASED` + nova flag `withRetentionClampToConsumer=true`, `retentionTime ≤ replicationLogRetentionTime` do líder, `fsync=false` (~213k ops/s, resend do #122 cobre a janela); e **fix obrigatório do `recordCount` stale** no caminho TIME_BASED (hoje `applyCompactionResult` em `NQueue.java:246-260` não recalcula `recordCount` após cutoff temporal, superestimando `relay.size()` que alimenta métrica de lag e drain-gate).

---

## 2. QUESTÕES ABERTAS RESOLVIDAS

| Questão | Decisão final (justificativa) |
|---|---|
| **Envelope `RelayEntry`** | `byte[]` binário puro com cabeçalho fixo (`version(1)·epoch(8)·sequence(8)·UUID(16)·topicLen(2)·topic·payloadLen(4)·payloadBytes`), `T = byte[]`. Evita dupla serialização (o `ObjectOutputStream` de `NQueue.toBytes` embrulha só um `byte[]` trivial) e contorna `ReplicationPayload`/`data` não-`Serializable`. `payloadBytes`: map reusa o `byte[]` já produzido em `MapClusterService.java:180`; queue exige **novo `QueueReplicationCodec`** simétrico ao `MapReplicationCodec` (default-typing ON), pois o `JacksonMessageCodec` do transporte perde o tipo concreto de `QueueReplicationCommand.value` → `ClassCastException` em `QueueClusterService.apply`. |
| **Regra de ACK** | ACK sai **após `relay.offer()` retornar com sucesso**, NÃO após apply (inverte a semântica inline onde ACK está dentro de `onSuccess` pós-apply em `ReplicationManager.java:866`). Quorum passa a significar "durável em N relays". `offer`-sucesso é **pré-condição estrita** do `sendAck`; se `offer` lançar `IOException`, **não ACKar** e deixar `retryPending` do líder reenviar. Sem perda silenciosa com `fsync=false`: tail perdido é **detectável** (`nextExpected < frontier`) e **recuperável** (resend do op-log #122), garantido pela invariante `relayRetention ≤ replicationLogRetentionTime` + `nextExpected` avança só pós-apply. |
| **Gap-fill** | **Buffer fino de reordenação em memória** (cap pequeno, ~centenas, << `MAX_SEQUENCE_BUFFER=250k`), backlog grande no disco (relay). Ao detectar `head.seq > nextExpected`: `requestSequenceResend(topic, nextExpected, head.seq-1)` (`ReplicationManager.java:1172`); resend entra no buffer fino; drena de `buffer ∪ relay` sempre escolhendo `seq == nextExpected`. Rejeitada a "apply direto sem buffer": num FIFO estrito o resend aterrissa no tail → head-of-line; "apply direto" colapsa em buffer de reordenação de qualquer forma. Cap estourado → bootstrap (B2). |
| **Dois cursores duráveis** | **`nextExpected` (per-tópico) é a autoridade lógica de apply**; `consumerOffset` do relay é só posição física de leitura (reseta a 0 no crash via `rebuildStateFromLog`, `count>0?0:size`). **Diverge da decisão D textual** ("cabeça do relay É o frontier sem cursor durável"): com OFFER não-idempotente + `fsync=false`, re-drenar de 0 sem `nextExpected` duplicaria toda a fila. Para **queue**, `nextExpected`/`appliedSequence` é persistido **atomicamente com o estado do handler** (no meta durável do NQueue de dados, mesmo ponto de fsync); `sequence-state.dat` vira cache. No restart `nextExpected = max(sequence-state.dat, frontier-derivado-do-handler)`. Para **map** (LWW idempotente) basta o hint coalescido. Invariante: `nextExpected` nunca regride abaixo de um OFFER já durável → at-least-once (re-peek) + fence = efetivamente-once. |
| **Drain-sem-peer** | Release do gate de escrita depende **exclusivamente de `relay.size()==0` por tópico**, NÃO de peer. `attemptLeaderSync` no ramo `syncSource==null` (`ReplicationManager.java:1698-1701`) hoje faz `leaderSyncing.set(false)` — **neutralizado no modo relay** (guard `RELAY_LOG ⇒ return` em `attemptLeaderSync`/`retryLeaderSync`). Nó promovido sem peer e com relay cheio continua gateado até drenar (o relay local já tem os dados, drenável sem peer). Substitui "sem peer ⇒ posso liderar já" por "sou líder quando meu relay drenar" — mais forte (não promove com backlog pendente). |
| **Init-order** | **Gravação no relay é independente do handler** (ingest path grava no disco mesmo sem `handlers.get(topic)` — resolve o race de op chegar antes do `registerHandler`). O **apply-loop por tópico é acoplado a `registerHandler`** (`ReplicationManager.java:244-246`): `ensureRelayApplyLoop(topic, handler)` sob gate `RELAY_LOG && !isLeader`, **idempotente** (um consumer por tópico). NÃO amarrar o loop a `manager.start()` (rodaria antes do handler → reintroduziria o race). O disco é a fila de espera entre ingest e registro. |
| **recordCount fix** | `applyCompactionResult` (`NQueue.java:246-260`) **recalcula `recordCount` recontando o segmento vivo** `[consumerOffset, producerOffset)` do novo arquivo sob lock (reusa padrão de `rebuildStateFromLog`, `NQueue.java`), e seta `approximateSize`. Recontagem (O(registros vivos), pequeno pós-compaction) > propagar `descartados` pelo `CompactionResult`, porque o clamp acontece fora de `findTimeBasedCutoff` — recontar é robusto a off-by-one e fecha o bug nos dois caminhos (com e sem clamp). Benigno em DELETE_ON_CONSUME (só descarta o já-decrementado), bug real em TIME_BASED. |

---

## 3. PLANO FASE A FASE (ordem de execução + commits atômicos)

> Convenção: cada commit compila verde e passa testes isolado; `git revert` cirúrgico. Build: `mvn -pl nishi-utils-core clean install`. Fases 0–1 são no-op comportamental (default INLINE) e podem mergear cedo; Fases 4+5 formam unidade atômica de comportamento.

### FASE 0 — Extensão NQueue (clamp-to-consumer + recordCount fix) — ISOLADA, antes do ReplicationManager

**Objetivo:** entregar a extensão de substrato com testes de regressão, sem tocar em ngrid. Commits 0.2 e 0.3 são independentes (podem mergear sem o resto da #124).

**Commit 0.1 — `feat(nqueue): flag withRetentionClampToConsumer`**
- Arquivos: `NQueue.java` — campo `retentionClampToConsumer=false` em `Options` (junto dos booleans, ~`:1348`); builder `withRetentionClampToConsumer(boolean)` (após `withRetentionTime`, ~`:1383`); **`copy()`** propagar o flag (`:1407-1426` — crítico: sem isso o grid perde o clamp no `enforceGridOptions`); `Snapshot` campo + atribuição no construtor (`~:1533-1551`).
- Commit atômico: só campo/builder/copy/snapshot. Default false → zero impacto.
- Testes: `NQueueOptionsTest#clampFlagPreservedByCopyAndSnapshot` — assert `copy()` e `snapshot()` preservam o flag.

**Commit 0.2 — `fix(nqueue): recompute recordCount apos compaction TIME_BASED`**
- Arquivos: `NQueue.java:246-260` (`applyCompactionResult`) — após adotar `newConsumerOffset`/`newProducerOffset`, recontar `recordCount` via helper `countRecordsLocked(dataChannel, consumerOffset, producerOffset)` (lê só headers, padrão de `rebuildStateFromLog`) e `approximateSize.set(recordCount)` antes de `persistCurrentStateLocked()` (para o meta gravado ficar consistente no restart).
- Ponto de integração: `CompactionResult` (`CompactionEngine.java:48`) **não precisa** carregar `newRecordCount` (recontagem é local ao NQueue).
- Commit atômico: independe de 0.1. Bug pré-existente.
- Testes: `NQueueTimeBasedRetentionTest#recordCountExactAfterTimeBasedCompaction` — produz N, deixa cabeça expirar sem consumir, força compaction, assert `size()==getRecordCount()==drain físico` (eliminar o stale provado no `NQueueRelaySpikeTest.t3-A`).
- **Regressão:** promover `NQueueRelaySpikeTest.t3` (hoje observacional/throwaway) a asserção firme num novo `NQueueRelaySpikeRegressionTest` — onde antes só logava o stale, agora **asserta** `recordCount` exato pós-compaction. Manter t1/t2/t4 como cobertura de peek-não-destrutivo / crash-apply→poll / throughput.

**Commit 0.3 — `feat(nqueue): clamp copyStartOffset ao consumerOffset em compaction TIME_BASED`**
- Arquivos: `CompactionEngine.java:195-196` — `long timeCutoff = findTimeBasedCutoff(...); copyStartOffset = options.retentionClampToConsumer ? Math.min(timeCutoff, snap.consumerOffset) : timeCutoff;` (usa `snap.consumerOffset` lido sob lock em `:182`).
- Ponto de integração: depende de 0.1 (lê `options.retentionClampToConsumer`).
- Commit atômico: só o clamp.
- Testes: `NQueueTimeBasedRetentionTest#clampNeverDiscardsUnconsumed` — TIME_BASED + clamp, cabeça expirada mas não consumida; assert que registros `≥ consumerOffset` **sobrevivem** à compaction (o oposto do comportamento provado em t3-A sem clamp).

**DoD da fase:** `mvn -pl nishi-utils-core test -Dtest=NQueue*Test` verde; bug do stale eliminado e clamp provado.

---

### FASE 1 — `FollowerIngestMode` + wiring (no-op, default INLINE)

**Objetivo:** introduzir o enum e propagá-lo até o `ReplicationConfig` sem ativar nada.

**Commit 1.1 — `feat(replication): FollowerIngestMode em ReplicationConfig`**
- Arquivos: novo `ngrid/replication/FollowerIngestMode.java` (top-level, `{ INLINE, RELAY_LOG }`); `ReplicationConfig.java` — campo final (~`:39`), construtor (~`:41-44`/`:57`), getter (~`:157`), builder field default `INLINE` (~`:171`), builder setter (~`:288`), `build()` (~`:294-296`).
- Commit atômico: só camada A. Default INLINE → inerte.
- Testes: `ReplicationConfigTest#followerIngestModeDefaultsInlineAndBuilderPropagates`.

**Commit 1.2 — `feat(ngrid): propaga followerIngestMode (NGridConfig -> NGridNode)`**
- Arquivos: `NGridConfig.java` — campo (~`:71`), construtor (~`:117`), getter (~`:341`), builder field default `INLINE` (~`:388`), setter (~`:705`), `import dev.nishisan.utils.ngrid.replication.FollowerIngestMode;` (higiene de imports); `NGridNode.java:358-360` — `replicationBuilder.followerIngestMode(config.followerIngestMode());` (sem guard null, default não-nulo).
- Commit atômico: B+C interdependentes (getter consumido no wiring) → commit único.
- Testes: `NGridConfigWiringTest#followerIngestModePropagatesToReplicationConfig`.

**Commit 1.3 — `feat(ngrid): followerIngestMode via YAML e facade`** (opcional, aditivo)
- Arquivos: `ClusterPolicyConfig.java:188-233` (POJO `ReplicationConfig`: campo `String followerIngestMode` + get/set, chave `cluster.replication.followerIngestMode`); `NGridConfigLoader.java:143-146` (parse defensivo tolerante, fallback INLINE em valor inválido, padrão de `NMapPersistenceMode`); `NGridNodeBuilder.java:199-208` (método fluente + propagação em `start()`).
- Commit atômico: D+E, superfície de config nova, sem efeito no caminho programático.
- Testes: `NGridConfigLoaderTest#followerIngestModeFromYaml` (incl. valor inválido → INLINE).

**DoD:** wiring completo, `mvn -pl nishi-utils-core test` verde, comportamento idêntico ao 4.3.0.

---

### FASE 2 — IO path (shadow-write → relay)

**Objetivo:** gravar no relay em paralelo ao caminho INLINE (modo SHADOW), sem mover ACK nem early-return, medindo custo de I/O e validando `relay.size()`. Fronteira segura do 1º commit em produção.

**Commit 2.1 — `feat(ngrid): QueueReplicationCodec simétrico ao MapReplicationCodec`**
- Arquivos: novo `ngrid/queue/QueueReplicationCodec.java` (espelho de `MapReplicationCodec`, `ObjectMapper` com `activateDefaultTyping` NON_FINAL), `encode/decode(QueueReplicationCommand)`.
- Testes: `QueueReplicationCodecTest#roundTripTypeFaithful` — POJO dentro de `value` sobrevive ao round-trip (sem `LinkedHashMap`).

**Commit 2.2 — `feat(ngrid): RelayEntry envelope + RelayEntryCodec`**
- Arquivos: novos `ngrid/replication/RelayEntry.java` (record `epoch,sequence,topic,operationId,payloadBytes`) + `RelayEntryCodec.java` (encode/decode do layout binário §2.2).
- Testes: `RelayEntryCodecTest#frameRoundTrip` (incl. topic com `:`, payload vazio, UUID).

**Commit 2.3 — `feat(ngrid): relay NQueue por tópico (lazy open + lifecycle), sem wiring no hot path`**
- Arquivos: `ReplicationManager.java` — campo `relayByTopic = new ConcurrentHashMap<>()` (junto de `replicationLogBySequence`, ~`:142`); `openRelay(Path)` com Options invariantes (`withFsync(false)`, `withShortCircuit(false)`, `withMemoryBuffer(false)`, `withRetentionPolicy(TIME_BASED)`, `withRetentionTime(relayRetention)`, `withRetentionClampToConsumer(true)`); `relayRetention = min(config.replicationLogRetentionTime(), teto-local)` **validado ≤ líder**; `sanitize(topic)` (mapeia `:`/`/`); diretório `dataDirectory()/relay/<sanitized>`; close de todos em `stop()`/`close()` (~`:1557+`), **após** parar apply-consumer. Ainda **não** chamado de `handleReplicationRequest`.
- Testes: `RelayStoreTest#openInvariantsAndClose` (assert Options aplicadas, retenção ≤ líder, sanitize de topic).

**Commit 2.4 — `feat(ngrid): shadow-write do relay em handleReplicationRequest`**
- Arquivos: `ReplicationManager.java` — `relayAppend(payload, message)` chamado entre o fencing por epoch (`:823`) e o dedup (`:826`), **modo SHADOW**: só `relay.offer(frame)` + métricas (`relaySize`, `relayOfferLatencyNanos`), **sem ACK**, **sem return**, erros engolidos com log. `payloadBytes` via `ReplicationHandler.toWireBytes(Object)` (map = cast; queue = `QueueReplicationCodec.encode`) para o `ReplicationManager` ficar agnóstico de `instanceof`. Guard por `ingestMode == SHADOW` (3º valor do enum, junto de INLINE/RELAY_LOG — **adicionar SHADOW** ao `FollowerIngestMode`).
- Ponto de integração: caminho INLINE intacto (buffer, `applyReplication`, ACK em `onSuccess` inalterados).
- Testes: `ReplicationManagerShadowWriteTest#shadowWritesRelayWithoutAffectingApplyOrAck` — assert apply/ACK idênticos ao INLINE + `relay.size()` cresce + erro no relay não quebra produção.

**DoD:** shadow mensurável; com TIME_BASED+clamp e consumer parado o relay não cresce sem limite (cutoff temporal domina). `mvn test -Presilience` verde.

---

### FASE 3 — Apply consumer (peek → fence → apply/resend → poll)

**Objetivo:** drenar o relay por tópico com exactly-once via fencing antes do handler, com gap-fill por buffer fino + resend, e dois cursores reconciliados.

**Commit 3.1 — `feat(ngrid): apply-consumer por tópico (peek->fence->apply->poll)`**
- Arquivos: `ReplicationManager.java` — `ensureRelayApplyLoop(topic, handler)` (thread daemon `ngrid-relay-apply-<topic>`, idempotente, um consumer/tópico); loop `peek → RelayEntryCodec.decode → fence(epoch<lastSeen ⇒ poll/descarta; epoch> ⇒ avança; seq<nextExpected ⇒ poll/descarta; seq>nextExpected ⇒ gap; seq==nextExpected ⇒ apply) → advanceFrontier → poll`; `handler.apply` só para `seq==nextExpected` (mantém OFFER seguro); wake-up via `notifyRelayWrite(topic)`/`LockSupport.park` sinalizado pelo ingest (sem busy-spin, sem `poll` bloqueante destrutivo antes do fence). `nextExpectedSequenceByTopic` per-tópico.
- Ponto de integração: hook em `registerHandler` (`:244-246`) sob `RELAY_LOG && !isLeader`; ingest no modo RELAY_LOG real (early-return + ACK pós-offer) entra aqui — `relayAppend` em modo RELAY_LOG faz `offer → sendAck(:1532) → return`, NÃO cai no inline. **Promove o SHADOW da Fase 2 para RELAY_LOG.**
- Testes: `RelayApplyConsumerTest#appliesInSequenceOrderExactlyOnce`; `#fencingDiscardsObsoleteEpochAndOldSeqBeforeHandler` (OFFER não duplica).

**Commit 3.2 — `feat(ngrid): gap-fill por buffer fino de reordenação + resend`**
- Arquivos: `ReplicationManager.java` — buffer fino per-tópico (cap pequeno), `gapFill(topic, nextExpected, head.seq)` → `requestSequenceResend` (`:1172`); resposta entra no buffer; drena `buffer ∪ relay` por `seq==nextExpected`; `skipEvictedGapAndDrain` (`:1302-1340`) adaptado para avançar `nextExpected` sobre a cabeça do relay (poll dos `seq<novo nextExpected`) quando líder reporta `missingSequences`; cap estourado → sinaliza bootstrap B2 (Fase 4).
- Testes: `RelayGapFillTest#resendFillsGapWithoutDuplicatingOffer`; `#evictedGapSkipsAndDrains`.

**Commit 3.3 — `feat(ngrid): cursor durável nextExpected acoplado ao handler (anti-duplicação OFFER)`**
- Arquivos: `ReplicationManager.java` — persistir `appliedSequence` per-tópico no meta durável do handler de aplicação (queue); restart `nextExpected = max(sequence-state.dat, frontier-handler)`; ordem de commit `apply(durável) → checkpoint frontier(durável) → nextExpected++ → relay.poll(best-effort)`. Map permanece hint coalescido. `MapClusterService`/`QueueClusterService`: `apply` aceitar `byte[]` simetricamente (queue decodifica `payloadBytes`→command via `QueueReplicationCodec.decode`).
- Testes: `RelayCursorReconciliationTest#crashBetweenApplyAndPollNoDuplicateOffer` (mata entre apply e poll, reabre, assert sem duplicação); `#sequenceStateBehindHandlerDoesNotReapply`.

**Commit 3.4 — `feat(ngrid): métrica de relay lag (primária size + secundária lógica)`**
- Arquivos: `ReplicationManager.java` — getter `relay[topic].size()` (confiável pós-fix 0.2) como lag primário; `leaderHighWatermark - lastAppliedSequence` (`:305-309`) como secundário/alarme.
- Testes: `RelayLagMetricTest#primaryLagDropsToZeroOnDrain`.

**DoD:** apply-consumer drena com exactly-once; crash-apply→poll sem duplicação de OFFER. `mvn test -Presilience` verde.

---

### FASE 4 — Snapshot bootstrap-only

**Objetivo:** desarmar os gatilhos de full-snapshot do hot-path; snapshot só em B1 (relay+estado vazios) ou B2 (lag > retenção / head obsoleto).

**Commit 4.1 — `feat(ngrid): neutraliza gatilhos de full-snapshot no hot-path (RELAY_LOG)`**
- Arquivos: `ReplicationManager.java` — curto-circuito em `checkLagAndSync` (`:301-324`: `if (RELAY_LOG) return;`); cap `MAX_SEQUENCE_BUFFER` (`:887-898`) morto por cutover (buffer aposentado); `applyReplication`→`requestSync` (`:963-966`) vira re-tentar head idempotente. Não toca `handleSyncResponse`.
- Commit atômico: reversível, no-op com flag off.
- Testes: `BootstrapOnlyTest#hotPathNeverRequestsSnapshotInRelayMode`.

**Commit 4.2 — `feat(ngrid): snapshot bootstrap-only (condição B1/B2)`**
- Arquivos: `ReplicationManager.java` — B1: `relay.size()==0 && nextExpected==1` (size confiável pós-0.2); B2: idade-do-head via `peekRecord().meta().getTimestamp()` (`NQueue.java:943`) mais antiga que `retentionTime - margem`, **com `expireAfterWrite` OFF** (senão `skipExpiredRecordsLocked` removeria a cabeça e mascararia o gap); só B1/B2 alcançam `resetState`/`requestSync` (`:720`).
- Ponto de integração: depende de 4.1 (nada mais chama snapshot).
- Testes: `BootstrapOnlyTest#coldStartTriggersBootstrap`; `#headOlderThanRetentionTriggersBootstrapNotResend`; `#transientGapUsesResendNotReset`.

**DoD:** snapshot só em bootstrap; gap transitório usa resend. `mvn verify -Pdocker-resilience` verde.

---

### FASE 5 — Failover drain-gate

**Objetivo:** generalizar o gate da #123 de "snapshot instalado" para "relay drenado"; promoção sem peer continua gateada até `relay.size()==0`.

**Commit 5.1 — `feat(ngrid): drain-gate no failover (release por relay.size()==0)`** — **núcleo do slice, commit coeso**
- Arquivos: `ReplicationManager.java`:
  - `onLeaderChanged` (`:1646-1671`): em RELAY_LOG, `leaderSyncTopics = handlers.keySet()` passa a significar "relays não drenados"; apply-consumer **não para** na promoção (drena = catch-up forte), só a escrita é gateada.
  - **Remover** `leaderSyncing.set(false)` dos pontos de snapshot (`:713-715`, `:755-757`) no modo relay; **mover** release para drain-check: `if (RELAY_LOG && leaderSyncing.get() && relay[topic].size()==0 && leaderSyncTopics.remove(topic) && leaderSyncTopics.isEmpty()) leaderSyncing.set(false);` — avaliado no loop de apply (após cada poll que zera um relay) **e** num drain-check agendado (cobre relay já vazio na promoção).
  - **Guard `RELAY_LOG ⇒ return`** em `attemptLeaderSync` (`:1686`) e `retryLeaderSync` (`:1708`) — neutraliza o clear-on-null (`:1698-1701`) que liberaria escrita com relay cheio sem peer.
- **Fronteira crítica:** 4.2 e 5.1 NÃO podem ir em PRs distintas — bootstrap-only desarma snapshot de regime, drain-gate redireciona release; se 4.2 mergear sem 5.1, nó promovido sem peer libera escrita com relay cheio (o bug que o slice fecha). Ambos partilham "no modo relay, snapshot/`leaderSyncing` desacoplam do op-log de regime".
- Testes: `FailoverDrainGateTest#promotedNodeRejectsWritesUntilRelayDrained`; `#drainReleaseWithoutPeer`; `#emptyRelayAtPromotionReleasesImmediately`.

**Commit 5.2 — `feat(ngrid): diagnóstico/métrica de drain pending`** (aditivo)
- Arquivos: `ReplicationManager.java` — log no início do drain (`relay.size()` por tópico na promoção) + métrica "relay drain pending"; callers (`QueueClusterService.ensureLeaderReady:596`, `MapClusterService.waitForReplication:343-371`) **sem** novo retry interno (fail-fast da #123 preservado: drain longo ⇒ rejeição longa ⇒ cliente decide).
- Testes: `FailoverDrainGateTest#drainPendingMetricExposed`.

**DoD:** failover-sob-carga sem espiral, sem promover com backlog. `mvn verify -Pdocker-resilience` verde.

---

### FASE 6 — Cutover (aposentar buffer)

**Objetivo:** remover o caminho de buffer em memória no modo relay (alinhado a "ir sempre à frente", decisão A). INLINE permanece intacto.

**Commit 6.1 — `refactor(ngrid): aposenta sequenceBufferByTopic no caminho RELAY_LOG`**
- Arquivos: `ReplicationManager.java` — no modo RELAY_LOG, `handleReplicationRequest` só faz `relayAppend → return` (remove o ramo buffer/`processSequenceBuffer`/`sequenceWaitStartByTopic` do hot-path relay). `MAX_SEQUENCE_BUFFER` e estruturas de buffer ficam vivas **apenas** para INLINE (sem dual-mode no caminho relay).
- Commit atômico: remoção do ramo morto sob RELAY_LOG; INLINE preservado.
- Testes: regressão completa `mvn test -Presilience` (INLINE inalterado) + `RelayCutoverTest#relayPathDoesNotTouchInMemoryBuffer`.

**DoD:** caminho relay limpo, INLINE preservado, suíte completa verde.

---

## 4. TESTES DE VERIFICAÇÃO (fase final)

| Verificação | Cenário | Profile / comando |
|---|---|---|
| **Idempotência crash apply→poll** | Mata o nó entre `handler.apply` (OFFER) e `relay.poll`; reabre; assert **sem duplicação** de itens na queue e Map convergente (LWW). Cobre `nextExpected` perdido parcial (`sequence-state.dat` atrás do handler) → frontier-derivado-do-handler corrige. | `mvn test -Presilience` (`RelayCrashIdempotencyIT` / in-process) + `mvn verify -Pdocker-resilience` (crash real de container) |
| **Failover-sob-carga** | Líder ~2.8k ops/s, 2 nós; mata líder com relay do follower **cheio**; assert: promovido **rejeita escrita** (`LeaderSyncingException`) até `relay.size()==0`, depois libera; **sem `resetState`/full-snapshot** no hot-path; sem peer ⇒ ainda gateado até drenar. | `mvn verify -Pdocker-resilience` (markers `CURRENT_LEADER_STATUS`/`ACTIVE_MEMBERS_COUNT`) |
| **Soak — espiral reproduzida/eliminada** | Baseline INLINE: reproduzir espiral (gap > 250k → snapshot crescente → starvation). RELAY_LOG: mesma carga → replica-lag limitado, **sem** reset+snapshot, follower converge pelo relay; `relay.size()` confiável (sem stale). | `mvn test -Psoak -Dngrid.soak.durationMinutes=720` (e variantes curtas p/ CI local) |
| **Bootstrap B1/B2** | B1: follower novo (relay+estado vazios) → snapshot único. B2: follower fora > retenção (head obsoleto) → bootstrap, não resend infinito. | `mvn verify -Pdocker-resilience` |

> Lembrete: **ngrid não roda no CI** — validar Fases 2–6 localmente com `-Presilience` e `-Pdocker-resilience`. Fase 0 roda em `mvn test` padrão.

---

## 5. RISCOS RESIDUAIS E MITIGAÇÕES

1. **`apply < produção` sustentado (não rajada):** relay cresce até a retenção truncar → bootstrap (B2). O relay **não cria capacidade de apply** (decisão E). *Mitigação:* alarme na métrica secundária (`leaderHighWatermark - lastAppliedSequence` sustentado); dimensionar `retentionTime` ≥ pior catch-up esperado.
2. **`fsync=false` + crash:** tail não-fsynced perde-se. *Mitigação (já no design):* `relayRetention ≤ replicationLogRetentionTime` + `nextExpected` durável → gap detectável (`nextExpected < frontier`) e recuperável (resend #122). Janela excedida → fallback explícito a snapshot (B2), nunca perda silenciosa.
3. **File handles × N tópicos** (decisão C, risco #3): um relay NQueue + uma thread de apply por tópico. *Mitigação:* monitorar contagem; relay lazy-open só sob tráfego; close determinístico em `stop()`.
4. **Buffer fino estoura** (gap > resend dentro da janela): *Mitigação:* cap pequeno bounded → bootstrap B2 (raro, limitado, não a espiral).
5. **Divergência textual com decisão D** (cursor durável vs "cabeça é o frontier"): documentar explicitamente que OFFER não-idempotente + `fsync=false` **exige** o segundo cursor; atualizar `issue-124-relay-log-follower.md` §D para refletir a correção (não é regressão de design, é refinamento forçado pelas decisões de durabilidade já aprovadas).
6. **`epoch > lastSeenLeaderEpoch` desacoplado** (recepção vs apply): avançar `lastSeenLeaderEpoch` **na recepção** (ingest), apply só **lê** para descartar epoch obsoleto no relay — manter coerência com resend e rejeição do líder atual.
7. **Drain longo bloqueia escrita** (Fase 5): aceitável e desejado (fail-fast #123). *Mitigação:* métrica "drain pending" + log na promoção para o operador distinguir "gate preso por bug" de "drain legítimo".

---

## 6. DEFINITION OF DONE

- **Documentação (`doc/`):** atualizar `doc/ngrid/oplog-ha-hardening.md` com a seção do relay-log (IO path desacoplado, dois cursores, drain-gate, bootstrap-only); referenciar `planning/cardinal-ha-oplog-hardening.md`. Corrigir `issue-124-relay-log-follower.md` §D (cursor durável). Salvar **este plano** em `/planning/issue-124-execution-plan-refined.md`.
- **Diagramas (`doc/diagrams/`):** novo `ngrid_relay_log_apply.puml` (sequência: `REPLICATION_REQUEST → relayAppend → ACK` ∥ `peek→fence→apply→poll`) + `ngrid_relay_failover_drain_gate.puml` (atividade: promoção → gate → drain → release). Incorporar no Markdown via `![titulo](https://uml.nishisan.dev/proxy?src=<RAW_PUML>)`. Validar renderização sem erro de sintaxe. (Nota: os existentes estão em `doc/diagrams/`, não `docs/diagrams/` — seguir o layout do projeto.)
- **CHANGELOG:** não há arquivo `CHANGELOG.md` no repo; o changelog vive na **GitHub Release**. Para o 4.4.0 (próxima minor): entrada "Relay-log no follower (#124): recepção desacoplada do apply, drain-gate no failover, snapshot bootstrap-only; extensão NQueue `withRetentionClampToConsumer` + fix `recordCount` TIME_BASED". Usar `gh release create` antes da tag.
- **API pública 4.3.0 preservada (aditivo):** `FollowerIngestMode` novo enum; `withRetentionClampToConsumer` novo método em `Options`; `followerIngestMode(...)` novos setters em `ReplicationConfig.Builder`/`NGridConfig.Builder`/`NGridNodeBuilder`. **Nenhuma assinatura existente alterada/removida**; default INLINE garante comportamento idêntico ao 4.3.0 sem opt-in. `mvn verify -pl nishi-utils-core -Pvalidate-javadoc` verde (Javadoc nos novos getters/setters).
- **Build/testes:** `mvn clean install -DskipTests` + `mvn test` + `mvn test -Presilience` + `mvn verify -Pdocker-resilience` + soak verdes antes do merge.

---

**Arquivos-âncora (absolutos):**
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/queue/NQueue.java` (Options ~1348/1383/1407-1426/1533-1551; `applyCompactionResult` 246-260)
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/queue/CompactionEngine.java` (clamp 195-196; `CompactionResult` 48; cutoff 280-297; newCO 224)
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java` (handleReplicationRequest 801-920; fencing 813-823; ACK 866/1532; registerHandler 244-246; checkLagAndSync 301-324; cap 887-898; onLeaderChanged 1646-1671; attemptLeaderSync 1686-1706; release-em-snapshot 713-715/755-757; resetState 720; resend 1172-1268)
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationConfig.java`; `.../structures/NGridConfig.java`; `.../structures/NGridNode.java:358-360`; `.../structures/NGridNodeBuilder.java:199-208`; `.../config/ClusterPolicyConfig.java:188-233`; `.../config/NGridConfigLoader.java:143-146`
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/map/MapClusterService.java:180`; `.../ngrid/queue/QueueClusterService.java` (enforceGridOptions 574-578; ensureLeaderReady 596-600)
- `/home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/queue/NQueueRelaySpikeTest.java` (base do teste de regressão da Fase 0; t3 promovido a asserção)
- `/home/lucas/Projects/nishisan/nishi-utils/planning/issue-124-relay-log-follower.md` (decisões A–F; §D a corrigir)
