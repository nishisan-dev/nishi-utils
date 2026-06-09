# Plano — Replicação confiável estilo MySQL relay-log (RELAY_STREAM) + saneamento de epoch

## Contexto (por que esta mudança)

O cluster HA do Cardinal (2 nós, topic `cardinal-state`, ~2500 ops/s) roda sobre o
`ngrid`/`ReplicationManager` do **nishi-utils**. Sob firehose, o follower não acompanha o
líder: fica preso pedindo `SEQUENCE_RESEND_REQUEST` em ranges antigos enquanto ops live
chegam muito à frente; `Gaps Detected` cresce aos milhares. O objetivo do dono do projeto
é o modelo **MySQL master/slave**: o líder gera o log, o follower **persiste o log em disco
e aplica com tempo**, sequencial, sem gap nem re-pedido de sequência.

A investigação (código + evidência read-only em pre-prod 218/219) revelou **dois problemas
distintos e independentes**, ambos precisam ser resolvidos:

1. **Bug de coordenação (P0, bloqueante AGORA):** `epoch` é um contador **local por nó**, não
   um termo de cluster acordado. O líder (node-1) perdeu seu `leader-epoch.dat` (dataDir
   relativo `var/cardinal/replication`, recriado no cutover 3.1.0) e **regrediu o epoch de
   7 → 1**. O follower (node-2) reteve `trackedLeaderEpoch/lastSeenLeaderEpoch=4` em memória
   volátil e agora **fencia (descarta) todo heartbeat e replicação do líder legítimo** porque
   `epoch 1 < 4`. Não há convergência para baixo — só restart do follower destrava. Mesmo um
   RELAY_STREAM perfeito seria fenced igual (o frame do stream carrega epoch).

2. **Modelo de transporte de replicação (arquitetural):** mesmo com epochs alinhados, a
   replicação é **push-uma-vez (broadcast) + recuperação por NAK**, com **reordenação num
   buffer de memória limitado** (`relayReorderByTopic`, cap 50k). O follower **já persiste-
   primeiro no disco** (`handleReplicationRelay` grava no `NQueue` antes do ACK), mas o relay
   é gravado em **ordem de chegada** e a ordenação/preenchimento de gap acontece em memória →
   sob firehose o buffer transborda → NAK storm / snapshot fallback. Não é um stream
   sequencial confiável.

**Resultado pretendido:** (Fase 0) coordenação sã e monotônica que nunca trava o follower;
(Fases 1+) replicação como **stream estritamente sequencial dirigido por cursor durável** —
o follower puxa o op-log do líder, persiste em ordem, e aplica no seu ritmo; em regime
permanente **zero gap, zero NAK, zero snapshot-storm**.

## Evidência de produção (12:44, 08-Jun-2026)

| Métrica | Líder node-1 (218) | Follower node-2 (219) |
|---|---|---|
| Role / Leader Epoch | LEADER / **1** | FOLLOWER / **4** |
| Valid Lease | true | **false** |
| Op-log Applied | 3.010.354 | **130.408** (travado) |
| Gaps Detected | 0 | **3.090** |
| Outbound Backlog/**Drop** | 0 / **0** | 0 / **0** |

Log do líder: `Loaded leader epoch: 6` → `changed: 7 (elected)` → `changed: 1 (elected)`
(regressão) → daí toda reeleição volta em `1`. **Drop=0 nos dois** ⇒ o gap NÃO vem de drop
de transporte. Config confirmada: `followerIngestMode: RELAY_LOG`, `persistentResendLog: true`,
`leaderPauseOnJoin: true`, `minClusterSize: 1`, `dataDir: var/cardinal/replication` (relativo).

## Decisões aprovadas

- **Escopo:** RELAY_STREAM definitivo (sem fallback de runtime para push+NAK).
- **Durabilidade:** ASYNC como default + SEMI-SYNC como knob opt-in.
- **Epoch:** Fase 0 obrigatória; abordagem = **termo de cluster monotônico + fencing por
  identidade do líder acordado + persistência durável**.
- **Mitigação operacional imediata (ação do operador, não código):** reiniciar o **node-2**
  zera o epoch volátil e destrava o pre-prod enquanto a Fase 0 é desenvolvida.

---

## FASE 0 — Saneamento de epoch (pré-requisito P0)

Arquivos: `ngrid/cluster/coordination/ClusterCoordinator.java`,
`ngrid/replication/ReplicationManager.java`, `common/HeartbeatPayload.java`,
e os payloads follower→líder (`ReplicationAck`/progress/fetch).

**Raiz a corrigir:** epoch usado como token de fencing mas computado como contador per-nó
(`leaderEpoch.incrementAndGet()` em `updateLeader` 630-642 e `stepDown` 669-682), comparado
entre nós cujos contadores divergem. Identidade do líder JÁ é acordada por `max(NodeId)`
(`recomputeLeader` 574-596); só o **número** do epoch diverge.

### 0.1 Epoch vira termo de cluster monotônico convergido
- Todo nó, ao observar um `leaderEpoch` (em heartbeat 727-747, replicação 1115-1125, e futuro
  stream) **maior** que o seu, adota: `leaderEpoch.updateAndGet(e -> max(e, observed))` +
  persiste quando cresce. Epoch passa a ser relógio lógico compartilhado, não contagem local.
- Líder-eleito carimba `newEpoch = max(epochLocal, maiorEpochObservado) + 1` (substitui o
  `incrementAndGet()` puro em 638/673). Garante epoch do líder ≥ tudo que qualquer follower viu.
- **Canal de convergência** para um líder que regrediu re-aprender o epoch alto: incluir
  `highestSeenEpoch` nas mensagens follower→líder (ACK de replicação, fetch do stream,
  follower-progress). O líder adota `max` e, ao detectar epoch observado > o seu, **re-carimba
  seu termo para `max+1`** e continua liderando (suas próximas mensagens já saem com epoch ≥
  o que o follower lembra). _Verificar se followers emitem heartbeat; independentemente, o
  canal via ACK/fetch é suficiente._

### 0.2 Fencing por identidade do líder acordado (o destrave)
- O follower **aceita** mensagens cujo `source == leader.get()` (o líder acordado por
  `max(NodeId)`), **adotando** o epoch desse líder mesmo que MENOR (reset de
  `trackedLeaderEpoch`/`lastSeenLeaderEpoch` para o epoch do líder acordado). A identidade é
  autoritativa.
- Fencing passa a rejeitar apenas mensagens de um `source` que **não é** o líder acordado E
  cujo epoch é ≤ o termo corrente (ex-líder stale de NodeId menor). Mantém a proteção
  anti-split-brain que o comentário 600-602 descreve, mas sem travar o líder legítimo.
- Aplicar a mesma regra nos dois pontos de fencing: heartbeat (`ClusterCoordinator` 736-741)
  e replicação (`ReplicationManager` 1117-1122).

### 0.3 Persistência durável e não-regressiva
- `loadEpoch` (158-169): carregar **max** (nunca regredir); arquivo ausente → 0 mas converge
  de peers (0.1) antes de emitir como líder; arquivo **corrompido** → SEVERE + recusar
  liderança até reconciliar (em vez de zerar em silêncio, que foi o que permitiu 7→1).
- Resolver `dataDirectory` para **caminho absoluto** no carregamento de config (ou validar e
  alertar se relativo). Documentar para operadores. _A regressão do Cardinal veio do dataDir
  relativo + var recriado._

### 0.4 Eleição por afinidade de prioridade (postura AP — "lead-while-alone")
Hoje a eleição é `max(NodeId)` (`recomputeLeader` 594) com `preferredLeader` apenas transiente
(TTL, 582-589) e `pairMode`+`minClusterSize:1` deixando qualquer nó sozinho liderar — origem da
"reeleição curiosa" (todo restart vira eleição + bump de epoch). Implementar afinidade estática:
- **Prioridade auto-anunciada:** cada nó anuncia a própria prioridade no handshake/heartbeat
  (dentro do `NodeInfo`). A eleição compara `(prioridade anunciada, depois NodeId)` — NodeId é o
  desempate ("exceto em caso de empate"). Evita config divergente onde dois nós se acham preferido.
- **Nó preferido (maior prioridade conhecida):** elege-se imediatamente (ninguém o supera).
- **Nó não-preferido:** **defere** — fica follower até confirmar isolamento por uma **janela de
  descoberta no boot** (gossip/handshake). Só após a janela, e ainda sem ver um par de prioridade
  ≥, ele se elege (lead-while-alone). Janela curta o bastante para não estagnar se o preferido
  morreu de fato; longa o bastante para o gossip revelar o preferido vivo.
- **Gossip descobre preferido depois:** dispara **reeleição** → não-preferido faz step-down, o
  preferido assume. Compõe com 0.1/0.2 (handoff bumpa o termo; fencing-por-identidade faz o
  follower adotar o novo líder limpo).
- **Postura sob partição (ambos vivos):** **AP** — o não-preferido pode liderar sozinho após a
  janela. Aceita divergência da **fila** (`offer` não-idempotente) sob partição; **mapa LWW**
  reconcilia sozinho.
- **Reconciliação na reconexão:** o perdedor (não-preferido) faz step-down e **re-bootstrap do
  vencedor** (descarta sua cauda divergente; com RELAY_STREAM isso é o snapshot+retomada de
  cursor da Fase 5). A perda da cauda da fila é **recuperável na aplicação**: o Cardinal deriva o
  estado do **Kafka** (`clusterName` = consumer group com failover de offset), re-drivado de forma
  idempotente. Documentar essa dependência explicitamente.

### 0.5 Observabilidade
- Reportar **`leaderEpoch` (local) E `trackedLeaderEpoch` E `lastSeenLeaderEpoch`** no
  dashboard/stats (hoje só "Leader Epoch"); alertar quando divergem. Reportar prioridade
  anunciada e contagem de reeleições. Estender `NGridDashboardReporter`/snapshot.

### 0.6 Testes (perfil `-Presilience`, in-process via `NGrid.local(n)`)
- `epochConvergesWhenAgreedLeaderRegresses`: follower lembra epoch alto, líder acordado emite
  epoch menor → follower adota e a replicação volta a fluir (sem restart).
- `staleExLeaderFencedByIdentity`: source que NÃO é o líder acordado é rejeitado.
- `epochMonotonicAcrossRestartNoRegression`: restart não regride o termo persistido.
- `preferredLeadsImmediately_NonPreferredDefersUntilDiscoveryWindow`.
- `gossipDiscoversPreferredTriggersReelectionAndCleanHandoff`.
- `splitBrainReconnectPreferredWinsLoserReBootstraps`: partição com ambos liderando → reconexão →
  preferido vence, perdedor re-bootstrapa; termo converge estritamente crescente.

---

## FASES 1–8 — RELAY_STREAM (binlog/MySQL definitivo)

Reusa o que já existe: **`ResendLog`/`ResendLogStore`** (op-log do líder: segmentado,
append-only, indexado por sequência, `read(from,to)`/`get(seq)`/`oldestSequence()`),
**`RelayStore`/`NQueue`** (relay durável do follower; o `consumerOffset` É o cursor),
**`RelayEntryCodec`** (mesmo frame nos dois lados), snapshot/bootstrap e heartbeat watermark.

### Fase 1 — Protocolo (`common/MessageType.java` + novos payloads)
- `RELAY_STREAM_FETCH` (follower→líder): `{topic, fromSequence, maxBatch}`.
- `RELAY_STREAM_BATCH` (líder→follower): `{topic, fromSequence, List<byte[]> frames (contíguos,
  ascendentes), leaderHighWatermark, oldestSequence, boolean needSnapshot}`.
- Ambos são **tráfego de controle** (não-dropável; `OutboundChannel.isCountable` só conta
  `REPLICATION_REQUEST`). `frames` carrega os bytes crus do `ResendLog` (zero re-encode).

### Fase 2 — Config (`FollowerIngestMode` + `ReplicationConfig`)
- `+RELAY_STREAM` no enum (Javadoc do modelo pull/binlog). `@Deprecated` em `INLINE`/`RELAY_LOG`.
- Knobs novos: `relayStreamFetchBatch`, `relayStreamPollInterval` (~50-100ms),
  `relayStreamFetchTimeout` (~2s), `relayMaxBacklog` (flow-control), `relayStreamSemiSync`
  (default false). Expor em `NGridConfig`/`NGridNodeBuilder`.

### Fase 3 — Líder serve o fetch do op-log durável
- `start()`: instanciar `resendLogStore` **incondicional** em stream mode (hoje só se
  `persistentResendLog`); o binlog é a fonte da verdade do stream.
- `handleRelayStreamFetch` (espelhar `handleSequenceResendRequest` ~1843): só o líder serve;
  `from < oldestSequence` ⇒ `needSnapshot=true`; `from > hwm` ⇒ batch vazio; senão `read(topic,
  from, from+maxBatch-1)` e **cortar no primeiro buraco** (run contíguo). Confiar no que
  `read()` devolve (evita corrida commit-vs-append). Rotear em `onMessage` (900-918).

### Fase 4 — Follower puxa e persiste EM ORDEM
- IO thread por tópico `relayFetchLoop` (espelhar `ensureRelayApplyLoop`): `fromSequence` =
  cauda durável do relay (`head+size`, lock-free; não tocar `sequenceBufferLock` no hot path);
  emite `RELAY_STREAM_FETCH`; pipeline com 1 fetch em voo; re-emite em timeout (idempotente —
  o cursor é durável).
- `handleRelayStreamBatch`: fencing por epoch (0.2) e por `< expected` (dedup); `relayFor(topic)
  .offer(frame)` **em ordem**; `signalRelay`; pede o próximo run. `needSnapshot` ⇒ `requestSync`.
- **Flow-control:** se `getRelaySize(topic) >= relayMaxBacklog`, pausar o fetch até a apply loop
  drenar abaixo do low-watermark. "O follower só busca o que consegue persistir."
- `replicateToFollowers`: em stream mode **não faz broadcast push** (ASYNC §Fase 5); mantém o
  `leaderEmissionLock` por tópico (sequência monotônica) — só remove o bloco de envio.
- A apply loop (`drainRelayOnce`) é reusada: o relay nunca tem buraco ⇒ o ramo de gap vira
  caminho morto (teste exige `gapsDetected==0`).

### Fase 5 — Semântica de commit + cutover/restart
- **ASYNC (default):** líder commita em local + op-log durável e NÃO espera follower. Cliente
  recebe sucesso com durabilidade single-node. Mapa (LWW) seguro; Fila (offer não-idempotente)
  tem janela de perda da cauda na falha do líder — igual ao MySQL async.
- **SEMI-SYNC (opt-in, `relayStreamSemiSync`):** líder só completa o future do cliente após
  ≥1 follower persistir até a seq (provado pelo `fromSequence` do próximo fetch). Lag limitado,
  sem perda em falha única. _Commit separado, bateria de testes própria._
- **Cutover de snapshot:** `completeSnapshotCutover` ancora `nextExpected=watermark+1`,
  `discardStaleRelayPrefix` limpa o prefixo obsoleto do relay, e a fetch loop retoma de
  `watermark+1`.
- **Restart limpo:** retoma do cursor (consumerOffset + frontier flushado no `stop()`), **sem
  snapshot**. **Unclean (crash):** `relayPendingBootstrap` força snapshot (já existe).
- **Failover:** `resendLogStore` já é instanciado em qualquer papel ⇒ ex-follower promovido já
  tem binlog. **Adicionar:** em `commitRelayBatch`, se o nó é líder em stream mode, espelhar
  cada entrada aplicada no `resendLogStore` (continuidade do binlog através da promoção). A
  fetch loop cessa quando `coordinator.isLeader()`.

### Fase 6 — Observabilidade do stream
- Por tópico: cursor (=`nextExpected`), `leaderHighWatermark` (do batch), lag, `oldestSequence`
  do líder, bytes/s do stream, flag `streaming`. Estender `TopicReplicationStatus` + reporter.

### Fase 7 — SEMI-SYNC (opt-in) — pós-MVP
- Implementa a variante de durabilidade ≥2 nós sob o knob; testes de durabilidade dedicados.

### Fase 8 — Limpeza definitiva (remoção do legado)
- Remover `INLINE`/`RELAY_LOG`; default do builder → `RELAY_STREAM`. Remover: reorder buffer
  (`relayReorderByTopic`/`RELAY_REORDER_CAP`/`drainReorderContiguous`/`bufferRelayResendOperations`),
  todo o protocolo NAK (`SEQUENCE_RESEND_REQUEST/RESPONSE` + handlers + payloads), o ramo de gap
  em `drainRelayOnce`, o broadcast push em `replicateToFollowers`, e a drop policy do
  `OutboundChannel` (sem push, sem drop). Atualizar/substituir `RelayLogReplicationTest`/
  `SequenceResendProtocolTest`.

### Testes RELAY_STREAM (perfil `-Presilience`, `NGrid.local(n)`)
Espelhar `RelayLogReplicationTest`, `SequenceResendProtocolTest`, `FollowerSnapshotCutoverTest`,
`ProactiveColdJoinWatermarkTest`, `PersistentResendLogRegressionTest`:
- `streamContiguousUnderFirehoseNoNak`: firehose ≥3000 ops ⇒ `gapsDetected==0`,
  `snapshotFallbackCount==0`, `syncRequestCount==0`, follower converge, estado idêntico.
- `slowFollowerLagsButNeverLoses`: apply lenta ⇒ lag cresce, `relayMaxBacklog` segura o fetch,
  converge sem perda nem snapshot ao cessar o firehose.
- `belowFloorTriggersSnapshotThenResumesStream`.
- `restartResumesFromCursorNoSnapshot` + `uncleanRestartBootstraps`.
- `failoverContinuesBinlogAcrossPromotion`.
- (se SEMI-SYNC) `semiSyncDurabilityNoLossOnSingleFailure`.

### Riscos/armadilhas de concorrência
- Não tocar `sequenceBufferLock` (tryLock 15s) no hot path da fetch loop; derivar `fromSequence`
  do relay (durável por poll/offer), não do flush coalescido (~1s).
- Preservar a ordem **commit-frontier-antes-de-poll** (1413-1414). Espelhamento no binlog do
  líder dentro de `commitRelayBatch` é tolerante a falha (não falha o commit).
- 2 threads/tópico (fetch+apply): no `stop()`, interromper+join as fetch loops ANTES das apply
  loops e ANTES do flush/marker, para não persistir relay após o marker limpo.
- Clampar `maxBatch`/bytes do batch para não estourar o limite de frame do transporte.

---

## Sequência de commits (atômicos, cada um compila e passa)
0. `fix(coordination): epoch como termo de cluster monotônico` (0.1+0.3)
0b. `fix(replication): fencing por identidade do líder acordado` (0.2)
0c. `feat(coordination): eleição por afinidade de prioridade + janela de descoberta + reeleição por gossip` (0.4) + observabilidade (0.5) + testes (0.6)
1. `feat(protocol): RELAY_STREAM_FETCH/BATCH + payloads`
2. `feat(config): FollowerIngestMode.RELAY_STREAM + knobs`
3. `feat(replication): líder serve fetch do op-log durável`
4. `feat(replication): follower fetch loop + persist em ordem`
5. `feat(replication): cutover/restart/failover do RELAY_STREAM`
6. `feat(observability): métricas por tópico do stream`
7. `feat(replication): semi-sync opt-in` (pós-MVP)
8. `refactor(replication): remove push+NAK+reorder; RELAY_STREAM default`

## Arquivos críticos
- `nishi-utils-core/.../ngrid/replication/ReplicationManager.java` (núcleo, ~3116 linhas)
- `nishi-utils-core/.../ngrid/cluster/coordination/ClusterCoordinator.java` (Fase 0)
- `nishi-utils-core/.../ngrid/replication/ResendLog.java` / `ResendLogStore.java` (binlog do líder)
- `nishi-utils-core/.../ngrid/replication/RelayStore.java` (relay do follower)
- `nishi-utils-core/.../ngrid/replication/ReplicationConfig.java` + `FollowerIngestMode.java`
- `nishi-utils-core/.../ngrid/common/MessageType.java` + novos payloads
- `nishi-utils-core/.../ngrid/structures/NGridConfig.java` / `NGridNodeBuilder.java` / `NGridNode.java`
- `cluster/transport/OutboundChannel.java` (limpeza, Fase 8)

## Verificação end-to-end
- Unit + resiliência: `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1`
  (rodar os novos testes de epoch e de RELAY_STREAM; suíte completa antes de cada commit).
- Build: `mvn -B -DskipTests -pl nishi-utils-core,nishi-utils-oss -am package`.
- Pre-prod (read-only) após deploy: confirmar nos stats `Leader Epoch` igual nos dois nós,
  `Gaps Detected==0`, follower `Applied` acompanhando o líder, lag estável; sob firehose nenhum
  `SEQUENCE_RESEND_REQUEST` no `replication-ngrid.0.log`.
- **Validação local obrigatória** da suíte ngrid (CI não roda a suíte de resiliência ngrid).

## Documentação (DoD)
- Atualizar `doc/` (pt-BR) com o modelo RELAY_STREAM e o termo de epoch; diagrama de sequência
  (líder op-log → fetch → relay → apply) e de estados de epoch em `doc/diagrams/*.puml`,
  incorporados via `uml.nishisan.dev`.
- Copiar este plano aprovado para `/planning` na execução.
