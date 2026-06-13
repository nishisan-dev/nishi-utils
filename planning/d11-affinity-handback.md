# Plano — Handback automático orquestrado por snapshot (Cardinal HA / nishi-utils)

## Context

A Cardinal roda a lib de replicação NGrid em par HA (node-1 `10.200.20.218` prio 100,
Xmx48g, líder preferido; node-2 `10.200.20.219` prio 50, Xmx16g). A subida funciona; **a
volta do líder por afinidade perde consistência** mesmo com todo o ciclo D1–D10 released
(nishi-utils 5.2.0 / TEMS 3.1.0, jar D10 em produção).

**Causa-raiz provada por trace (12/jun, read-only em pré-prod):** o reclaim por afinidade é
*delta-only*. Com `leaderLocalApply=false` o líder avança `lastApplied` por commit de quórum;
ao reassumir, node-1 não absorve a cauda interina do node-2, e **nenhum caminho reancora o
`lastApplied` de um follower para baixo**. Isso cria um offset permanente de linhagem entre os
contadores (observado AO VIVO: constante **54024**, nascido em 11/jun 16:22 logo após um
reclaim, acumulado de 1745→54024 em failovers sucessivos). Como
`reclaimQuiesceThreshold=4096` ≪ 54024, o reclaim-quiesce do D10b **nunca engata** → a volta
degrada para dual-leader/descarte = **perda silenciosa**.

**A cura já existe na lib:** `completeSnapshotCutover` faz `SET` (não `max`) de
applied/global/topic = watermark do líder servindo, trunca o binlog local e reancora o cursor
do relay. Um snapshot-full + cutover a cada handback **zera o offset por construção e é
auto-curável**. Resultado pretendido: a volta deixa de perder dados e o offset não acumula.

## Decisões travadas (Lucas, 12/jun)

- **Política:** handback **automático na religada** — node-1 volta, detecta interino saudável
  e inicia o handshake sozinho; **não** faz self-elect nem reclaim por watermark.
- **Mecanismo:** **snapshot-full sempre** + cutover `SET` (sem otimização delta).
- **Definitivo, sem fallback para legado:** o handshake **substitui** o reclaim por
  delta/watermark. A eleição de **failover genuíno** (líder morre → follower promove)
  permanece intacta.

## Arquitetura — handshake de handover (máquina de estados)

Reusa máquinas existentes: snapshot multi-chunk (`SYNC_REQUEST/RESPONSE`), `completeSnapshot-
Cutover` (D8), o gate de produção do `replicate()`, e a maquinaria de `relayPendingBootstrap`
(mesma do `onDualLeaderYield`, porém disparada deliberadamente e com papéis invertidos).

```
CANDIDATO (node-1, volta como follower — NÃO self-elege)
  detecta: sou maior afinidade ∧ existe líder SAUDÁVEL ∧ estou estável (sem bootstrap pendente)
  └─► envia HANDBACK_REQUEST(applied, observedLeaderWatermark)

INTERINO (node-2, líder) ── recebe HANDBACK_REQUEST
  ├─ entra LEADER_PREPARING; handoverFreezing=true; W = max(global,applied)  [stop-the-world]
  ├─ onHandoverPrepare() → app PARA de consumir + faz flush do pipe ≤ W
  ├─ replicate() passa a lançar LeaderSyncingException (nada produz acima de W)
  └─► responde HANDBACK_GRANT(topic, W, epoch)

CANDIDATO ── recebe HANDBACK_GRANT
  ├─ relayPendingBootstrap.add(topic)  → fura o stale-sync guard (instala mesmo com frontier alto)
  ├─ requestSync(topic,0) → instala snapshot full → completeSnapshotCutover(topic, W)  [SET → offset 0]
  ├─ assumeLeadershipForHandback(epoch)  → epoch estritamente acima de W
  └─► envia HANDBACK_COMPLETE(W, newEpoch); onLeaderChanged dispara promoção

INTERINO ── observa novo líder (epoch maior) / HANDBACK_COMPLETE
  ├─ handoverFreezing=false; adota o epoch do candidato (sem ladder); demote a follower limpo
  └─ onDemotedAfterHandover(newLeader) → app segue como follower (applied == W == candidato)

CANDIDATO ── drain-gate libera (isLeaderSyncing=false) → onPromotionComplete → app volta a consumir
```

**Transição única e limpa (sem dual-leader):** o interino está *congelado, não produzindo*
desde o PREP; o candidato só assere liderança *após* o cutover, em epoch estritamente acima do
concedido. A detecção de dual-leader e o re-stamp de epoch são **suprimidos** enquanto
`handbackRole != NONE` (o interino adota o epoch do candidato em vez de competir). O detector
de dual-leader (D10c) **permanece como backstop** para abortos sujos.

## Divisão de repositórios e mudanças (file-level)

> Referências de linha são seams a confirmar na implementação (podem ter drift).

### nishi-utils (`nishi-utils-core`, `dev.nishisan.utils.ngrid`) — protocolo + máquina de estados + gating

- `common/MessageType.java` — +4 constantes: `HANDBACK_REQUEST/GRANT/ABORT/COMPLETE` (path JSON
  do `CompositeMessageCodec`, sem mudança de codec binário).
- `common/Handback{Request,Grant,Complete,Abort}Payload.java` — 4 records novos (estilo
  `SyncRequestPayload`, Jackson). Request: `{candidate, candidateApplied, observedLeaderWatermark}`;
  Grant: `{topic, frozenWatermark, leaderEpoch}`; Complete: `{cutoverWatermark, newEpoch}`;
  Abort: `{reason}`.
- `ngrid/HandoverListener.java` — interface nova (callbacks default no-op): `onHandoverPrepare`,
  `onDemotedAfterHandover(NodeId)`, `onPromotionComplete`, `onHandoverAborted(String)`.
- `replication/ReplicationManager.java` — núcleo:
  - estado: `handbackRole` (enum NONE/CANDIDATE_REQUESTING/CANDIDATE_INSTALLING/LEADER_PREPARING/
    LEADER_SERVING), `handoverFreezing`, `handoverFrozenWatermark`, `handbackPeer`,
    `handbackStartedMs`, `handbackCooldownUntilMs`.
  - `replicate()` — 4º gate: lança `LeaderSyncingException` se `handoverFreezing` (nada acima de W).
  - `onMessage()` — dispatch dos 4 tipos: `handleHandbackRequest/Grant/Complete/Abort`.
  - `maybeInitiateHandback()` (schedule 200ms) — predicado do candidato (follower ∧ maior
    afinidade ∧ líder saudável ∧ estável ∧ cooldown ok) → envia REQUEST; **não** chama recompute.
  - `checkHandover()` (schedule) — timeouts/abort (Fase de falhas).
  - `addHandoverListener`, `isHandoverFreezing()`; wira `coordinator.setAffinityHandbackMode(...)`
    em `start()`.
- `cluster/coordination/ClusterCoordinator.java`:
  - `affinityHandbackMode` (campo+setter); em `recomputeLeader()`: quando ligado e o nó local
    venceria por afinidade mas é follower e **existe líder saudável**, **não** promove pelos
    gates de watermark — permanece follower (handshake assume). Failover genuíno (sem líder
    saudável) **inalterado**.
  - `localIsPreferredLeader()` (reusa `outranks`), `isAgreedLeaderHealthy()` (último heartbeat
    com `leader()==true` dentro do `heartbeatTimeout`), `assumeLeadershipForHandback(epoch)`
    (epoch = max(local,concedido)+1; `updateLeader(localId)`; suprime contest de dual-leader).
  - Mantém: eleição de failover, boot-discovery, backstop de dual-leader (suprimido durante
    handover ativo), `onDualLeaderYield`.
- `replication/ReplicationConfig.java` — knobs novos no builder: `affinityHandbackMode`
  (default **false** — lib fica legado/opt-in), `handoverMaxDuration`, `handoverSnapshotTimeout`,
  `handoverRequestTimeout`, `handoverCooldown`.

### tevent-cardinal / tevent-lib — orquestração + pause/resume + default definitivo

- `tevent-lib/.../config/HAReplicationConfig.java` — defaults da Cardinal: `affinityHandbackMode=
  true`, `handoverMaxDurationMs=120_000`, `handoverSnapshotTimeoutMs=120_000`,
  `handoverRequestTimeoutMs=30_000`, `handoverCooldownMs=60_000`; **força `leaderPauseOnReclaim=
  false`** quando handback ligado (o freeze explícito substitui o reclaim-quiesce — elimina a
  armadilha do threshold 4096 ≪ offset).
- `tevent-cardinal/.../cluster/CardinalClusterManager.java` — `implements HandoverListener`;
  registra em `startCluster()`; implementa os 4 callbacks reusando `pauseConsumer/resumeConsumer`
  e o drain-gate existente (`scheduleActivateWhenCaughtUp`); `onHandoverPrepare` pausa consumo e
  **drena o pipe a zero** (bloqueia até timeout) antes de retornar; `buildReplicationConfig` wira
  os knobs novos e força `leaderPauseOnReclaim(false)`.
- `tevent-cardinal/.../cluster/CardinalReplicator.java` — `active()` += `&& !replicationManager.
  isHandoverFreezing()`.
- `ConsumerCardinalThread.java` / `TEventCardinal.java` — sem mudança obrigatória (o hold por
  `isConsumerRunning()` e o shutdown hook D5 já cobrem; opcional `shouldHoldConsumerForHandover()`
  para observabilidade).

## Falhas / timeout / abort (sem líder preso, sem dual-leader)

- `checkHandover()` no interino: se `LEADER_PREPARING/SERVING` exceder `handoverMaxDurationMs`,
  ou o candidato sair da membership → **ABORT**: `handoverFreezing=false`, role=NONE, cooldown,
  envia `HANDBACK_ABORT`, `onHandoverAborted` → app `resumeConsumer()`, **continua líder** (sem
  demoção → sem dual-leader). Reusa o padrão de `checkReclaimQuiesce`.
- Candidato sem GRANT em `handoverRequestTimeoutMs` → role=NONE, cooldown, segue follower.
- **Candidato morre mid-install** (após GRANT, antes do COMPLETE): seguro — tinha
  `relayPendingBootstrap` setado → no restart sobe bootstrap-pending (advertise `-1`, inelegível),
  puxa snapshot fresco, segue follower; meio-estado descartado pelo `resetState()` do chunk 0. O
  interino aborta por candidato-ausente e volta a produzir.

## Plano de testes

**nishi-utils E2E** (harness de 2 `NGridNode` reais, estilo `DivergentLineageReclaimE2ETest`,
produção contínua):
- `AutomaticLeaderHandbackE2ETest` — prova canônica: pós-handback `node1.applied ==
  node2.applied` (**offset == 0**), `node1.applied == node1.global`, líder único, epoch sobe
  pouco (≤3, sem ladder), conteúdo correto (NEWER_OPS presentes, zero resíduo stale), **nunca**
  houve intervalo com dois líderes produzindo.
- `HandbackBoundedPauseE2ETest` — pausa ≤ `handoverMaxDurationMs`; `node2.global == W` (nada
  produzido acima de W durante o freeze).
- `HandbackAbortOnCandidateDeathE2ETest` — candidato morto mid-install; interino retoma e
  **continua líder**; sem dual-leader; restart do candidato converge com offset 0.
- `HandbackDoesNotEngageOnGenuineFailoverE2ETest` — líder morre → sobrevivente promove pela
  **eleição normal** (predicado separa os caminhos).

**nishi-utils unit:** `HandbackStateMachineTest` (transições de role + freeze gate + abort);
`HandbackPredicateTest` (`localIsPreferredLeader`/`isAgreedLeaderHealthy`; recompute não
auto-reclama com handback ligado, mas elege em failover).

**Cardinal:** `CardinalHandbackConsumeGateTest` (prepare pausa+drena; abort retoma; demote seta
leader=false); regressão de `buildReplicationConfig` (knobs novos + `leaderPauseOnReclaim=false`
chegam ao builder).

**Regressão obrigatória:** lib default `affinityHandbackMode=false` ⇒ todos os E2E existentes
(`DivergentLineageReclaimE2ETest`, `LeaderSyncBeforeReclaimE2ETest`, `StaleFrontierReclaimE2ETest`,
`DualLeaderLivelockE2ETest`) **devem permanecer verdes**. Rodar suíte completa do módulo.

## Validação em pré-prod (218/219)

1. **Reproduzir** o offset 54024 no build atual (legado): failover controlado de node-1 → node-2
   produz sob firehose → retorno → confirmar offset constante + WARN "counter-scale desync".
2. **Deploy** do build handback (`affinityHandbackMode=true`, `leaderPauseOnReclaim=false`),
   restart limpo nos 2 nós (reancora).
3. **Mesmo failover+retorno** → grep do handshake nos logs (REQUEST→prepare→GRANT(W)→cutover W→
   assume→demote).
4. **Provar offset 0**: `HAStats.appliedSeq` igual nos 2; `node-1.applied==global`; WARN de
   desync **desaparece**.
5. **Epoch limpo** (sobe pouco, sem dual-leader); **backlog Kafka drena** após `resumeConsumer`,
   sem perda em `cardinal.log`.
6. **Repetir 2–3 ciclos** — provar que o offset NÃO acumula mais.

## Riscos / bordas

- Pausa multi-GB sob firehose: dezenas de s a min; Kafka bufferiza (lag, não perda);
  `handoverMaxDurationMs=120s` limita; LZ4 já liga. **Trade-off aceito (decisão travada).**
- `minClusterSize=1`/pairMode: handback só dispara com líder saudável; partição (sem líder
  acordado) cai na eleição normal; backstop de dual-leader mantido.
- **Restart conjunto dos 2 nós:** sem líder para snapshot → predicado falso → boot-discovery +
  eleição + bootstrap cold (handback é puramente aditivo; não trava cluster frio).
- Remover delta-reclaim só afeta a Cardinal (lib default off) → D8/D9/D10 e seus E2E intactos;
  validar que desligar reclaim-quiesce não regride o cenário D10b (coberto pelo
  `HandbackBoundedPauseE2ETest` — convergência sob produção contínua).

## Definition of Done

- [ ] Suíte nishi-utils completa verde (incl. E2E legados) + novos testes handback.
- [ ] Suítes tevent-lib/tevent-cardinal verdes.
- [ ] Validação pré-prod 218/219: offset 0 pós-handback em 2–3 ciclos, sem dual-leader, sem perda.
- [ ] Doc `doc/ngrid/oplog-ha-hardening.md` (+ runbook) atualizada com o protocolo de handback.
- [ ] Plano aprovado copiado para `/planning` em ambos os repos (regra do projeto).
- [ ] Branches: `feature/affinity-handback` (lib) e `feature/cardinal-affinity-handback` (Cardinal);
      commits atômicos por fase.
