# Plano — Correção definitiva do "offset de linhagem" no handback D11 (NGrid / nishi-utils)

## Context

A Cardinal roda a lib NGrid (`nishi-utils-core` 7.1.0, branch `main`) em par HA: **node-1 = 218** (prio 100, Xmx48g, líder preferido) e **node-2 = 219** (prio 50, Xmx16g). O recurso **D11 (handback automático orquestrado por snapshot)** está em validação pré-prod. O `ngrid-log-analyzer.py` reporta eventos `DESYNC` após as "voltas", e o usuário observou a **2ª volta falhar** depois de acumular ~70–100k identifiers sob firehose.

**O que realmente aconteceu (diagnóstico confirmado por código + revisão adversarial de 3 lentes + sondagem read-only ao vivo nos 218/219):**

- **Não houve perda de dados nem cluster sem líder.** Os handbacks completaram (cutover ok, líder único). A linhagem por-tópico (`nextExpectedSequenceByTopic`) está íntegra, com dedup por `operationId` e gaps roteados para recovery. As 3 lentes convergiram: `benign-apply-skew`, `isDataLoss=false`, alta confiança.
- O `DESYNC` é o **"offset de linhagem" counter-scale** — exatamente o sintoma que o D11 deveria **zerar** e **não zerou**. Ao vivo (13/jun 17:05): node-1 líder `applied=6.849.180 < node-2 follower 6.849.227` (Δ≈47, estável/oscilando 9–47). O detector `ClusterCoordinator:882` compara só os dois **odômetros globais**, nunca o conteúdo — por isso não distingue offset de fork, e o cluster corretamente **retém liderança** ("counter-scale desync … never a reason to abdicate", CC:873-875).
- **A "2ª volta que falhou" = node-1 reiniciou no meio do handback.** Prova viva: processo java do 218 subiu `Sat Jun 13 15:55:11`. Ao reiniciar **sem o arquivo de epoch**, `loadEpoch` começa em 0 (CC:315) → 1ª eleição loga `epoch 1` (abaixo do epoch do cluster). O restart sob backlog de 70–100k produziu naquele ciclo: `ERROR Failed to install snapshot chunk`, **duplo SNAP-START** (janitor de sync travado re-pedindo, RM:2167/1735), churn de epoch e um offset transitório grande. **O usuário deu `kill -TERM` normal (shutdown hook), sem `kill -9` e sem limpar o data dir** → logo o epoch renascer em 1 é **bug de durabilidade** (achado secundário C).

### Causa-raiz (assimetria no odômetro global)

`appliedSequence`/`lastAppliedSequence` é **um contador global** (RM:77), mas as sequências são **por-tópico**. Ele avança em **bases numéricas diferentes** conforme o papel:

| Papel | Como avança `applied` | Evidência |
|---|---|---|
| Líder (commit) | `appliedSequence.updateAndGet(c -> max(c, op.sequence))` — adota a linhagem | `ReplicationManager.java:1018` |
| Follower (relay) | `recordApplied(entries.size())` → `appliedSequence.addAndGet(n)` — **contagem cega**, deriva em re-pull/duplicata | `ReplicationManager.java:1981 / 3380-3382` |
| Candidato que volta | `completeSnapshotCutover` → `appliedSequence.set(watermark)` — **SET, offset zerado** | `ReplicationManager.java:1262-1276` |
| **Incumbente rebaixado** | `acceptHandbackWinner` só `updateLeader + observeEpoch` — **NÃO re-ancora** | `ClusterCoordinator.java:1115` |

O candidato zera o offset (SET); o ex-líder rebaixado **mantém a base do odômetro** de quando era líder → offset permanente, congelado a cada ciclo e re-inflado pela contagem cega do follower sob firehose. Já existe a fórmula canônica lineage-fiel usada no seed de restart: `applied = max(Σ(nextExpected[t]-1), globalSequence)` (`seedAppliedSequenceFromFrontier`, RM:639-690) — base correta para a correção.

**Objetivo deste plano (decisão do usuário): corrigir a lib (A+B)** para que o offset seja **0 por construção** a cada volta e o WARN de desync **desapareça** (meta original do D11). Sem fallback para legado; o caminho `affinityHandbackMode=false` (default) permanece intacto.

---

## Mudanças (cirúrgicas, em `nishi-utils-core`)

### Fix A — Re-ancorar o incumbente rebaixado

Novo método privado `reanchorAsDemotedIncumbent(long W)` em `ReplicationManager`, espelhando o subconjunto relevante de `completeSnapshotCutover` **sem instalar snapshot** (o incumbente já detém estado canônico até W, pois congelou produção exatamente em W = `handoverFrozenWatermark`). Para o(s) tópico(s) registrado(s):

1. `appliedSequence.set(W); lastAppliedSequence = W;` (SET, não max — RM:1272-1273 análogo).
2. `globalSequence.updateAndGet(c -> W);` (RM:1274) — produção futura contígua a partir de W+1.
3. `sequenceByTopic[t].set(W)` (RM:1275).
4. Sob `acquireSequenceLock()`: `nextExpectedSequenceByTopic.put(t, W+1); sequenceStateDirty = true;` (RM:1281).
5. `isStreamMode()` sob `relayIngestLock(t)`: `purgeRelay(t); relayStreamCursor(t).set(W);` + `relayFetchPendingUntilByTopic.put(t, 0L); signalFetch(t);` (RM:1303-1314).
6. Truncar o binlog local de `t` (`resendLogStore.logFor(t).truncate()`, RM:1291-1294).

**Chamadas (dois caminhos de demote — ambos obrigatórios):**
- **Caminho COMPLETE:** em `handleHandbackComplete` (RM:3064), chamar `reanchorAsDemotedIncumbent(payload.cutoverWatermark())` **antes** de `coordinator.acceptHandbackWinner(...)` (RM:3078), para já anunciar W no próximo heartbeat.
- **Caminho backstop (furo a fechar):** o incumbente também rebaixa pelo heartbeat de epoch maior do candidato, **sem** passar pelo COMPLETE (documentado como backstop). Re-ancorar também em `onLeaderChanged` (RM:2536), no ramo `previousLeader.equals(localId)` quando o novo líder é o `handbackPeer`, usando o watermark anunciado pelo líder (idempotente com o COMPLETE; clamp com `max`). Sem isso, perder a mensagem COMPLETE faz o offset reaparecer.

Após o re-anchor, `applied == W` == frontier do novo líder → `isCaughtUpToCluster` deixa de ver "peer acima" → **CC:882 não dispara**; o cursor em W puxa W+1… em ordem, sem rejeição "stale sync".

### Fix B — Odômetro do follower lineage-fiel

Em `commitRelayBatch` (RM:1971-1987), substituir `recordApplied(entries.size())` (a contagem cega) por um reconcile-to-frontier **com o lock de sequência já detido**, após o `nextExpectedSequenceByTopic.merge(...)`. Novo helper `appliedFrontierLocked()` reusa a fórmula do seed (sem re-ancorar `nextExpected` a partir de `sequenceByTopic` — isso é só no restart):

```
frontierSum = Σ (não-sintético) max(0, nextExpected[t]-1)
target      = max(frontierSum, max(0, globalSequence))
appliedSequence.set(max(appliedSequence.get(), target));   // monotônico, nunca regride
lastAppliedSequence = appliedSequence.get();
```

- Single-topic (Cardinal): `target == last.sequence() == frontierSum == globalSequence` — **idêntico em quiescência**, só remove a deriva por re-pull/duplicata. Métrica de lag (RM:1346) fica mais precisa; `advertisedLeaderHighWatermark` (RM:549-556) passa a comparar maçãs com maçãs.
- O único caller de `recordApplied(int)` é este; o `recordApplied()` sem-arg (apply assíncrono do líder, RM:1034) **fica intocado**.
- **Multi-tópico (risco a sinalizar):** o líder ainda usa `max(op.sequence)` (RM:1018) ≠ `frontierSum`. Para Cardinal (1 tópico) coincidem; manter (B) só no follower e **deixar comentário** de que a simetria total (mover líder e follower para `frontierSum`) é tarefa separada com E2E multi-tópico próprio. Não reescrever o hot-path do líder neste patch.

**Por que A e B juntos:** (A) zera o offset no instante da volta; (B) garante que ele **permaneça 0** sob firewall/re-pull contínuo (sem (B), uma re-pull re-infla o follower e re-arma o CC:882 alguns heartbeats depois). O novo teste falha com só-(A) e passa com (A)+(B).

### Arquivos
- `nishi-utils-core/.../replication/ReplicationManager.java` — `reanchorAsDemotedIncumbent` (novo), chamadas em `handleHandbackComplete` (RM:3064) e `onLeaderChanged` (RM:2536); `commitRelayBatch` (RM:1971) + helper `appliedFrontierLocked`.
- `nishi-utils-core/.../cluster/coordination/ClusterCoordinator.java` — sem alteração de lógica (re-anchor vive no ReplicationManager, que detém os mapas/cursores). Confirmar apenas o watermark anunciado disponível ao ramo do backstop.
- `nishi-utils-core/.../common/HandbackCompletePayload.java` — confirmar que `cutoverWatermark()` carrega W (já carrega).

---

## Testes

### Novo E2E — `RepeatedHandbackZeroOffsetE2ETest`
`repeatedHandbacksUnderFirehoseKeepZeroOffsetAndNoLeaderBehindWarning()` — reproduz o que o `AutomaticLeaderHandbackE2ETest` **não** pega (mede offset 1x, em instante quiescente, single-topic). Reusa o harness existente (`newNode`, `awaitLeader`, `awaitStablePair`, `awaitApplied`, `Producer`, `DualLeaderWatcher`).

Estrutura:
1. Boot node-1 (prio 100) + node-2 (prio 50), `affinityHandbackMode(true)`, tópico único.
2. **Sentinela `LeaderBehindWarningSentinel`:** `java.util.logging.Handler` no logger `ClusterCoordinator` dos 2 nós que seta `AtomicBoolean` se algum record contiver `"Current leader observes a peer watermark above its own applied"` (equivalente programático do grep ao vivo).
3. `Producer` firehose roda o teste inteiro (só `pause()/resume()` por ciclo, nunca `stop()` entre ciclos).
4. `for ciclo in 1..N` (N=3): handback limpo (fases 2-5 do teste existente); drena até quiescência por ciclo (`awaitApplied` em ambos == `node1.global`); **asserts por ciclo:**
   - `node1.global == node1.applied` (offset 0 no líder)
   - `node1.applied == node2.applied` (offset 0 no follower)
   - `node1.global == node2.applied` (cross-check da deriva de (B))
   - `sentinel.fired() == false` (CC:882 nunca dispara)
5. Pós-loop: para firehose, `rethrowUnexpected()`, prova de conteúdo (acked-vs-drained, sem perda/duplicata); `DualLeaderWatcher.maxDualWindowMs() < MAX_DUAL_LEADER_WINDOW_MS`.

### Regressão obrigatória (devem permanecer verdes, `-Presilience`)
`DivergentLineageReclaimE2ETest`, `LeaderSyncBeforeReclaimE2ETest`, `StaleFrontierReclaimE2ETest`, `DualLeaderLivelockE2ETest`, `AutomaticLeaderHandbackE2ETest`, `HandbackSafetyE2ETest`.

```bash
mvn -pl nishi-utils-core clean install -DskipTests
mvn -pl nishi-utils-core test -Presilience -Dtest='*ReclaimE2ETest,DualLeaderLivelockE2ETest,*HandbackE2ETest,HandbackSafetyE2ETest,RepeatedHandbackZeroOffsetE2ETest' -Dsurefire.rerunFailingTestsCount=1
mvn -pl nishi-utils-core test -Presilience   # gate completo
```

---

## Validação ao vivo (218/219 — usuário autorizou hands-on)

1. **Baseline (capturar o bug):** `grep -c "peer watermark above its own applied" <node1.log>`; `grep "peer watermark..." | tail -1` → `(X < Y)` = `líder_applied < follower_watermark` (Δ≈47); registrar `applied`/`global` por nó.
2. **Deploy:** `mvn -pl nishi-utils-core clean install -DskipTests`, montar o fat-jar `tevent-cardinal-3.1.0.jar` com a lib nova, restart **rolling respeitando afinidade**: **219 (follower) primeiro** (rejoin estável) **depois 218** — evita churn extra no deploy.
3. **Handback controlado:** firehose rodando → `kill -TERM <pid_218>` → 219 promove (failover genuíno) e produz uma cauda → restart do 218 (rejoin como follower) → handback instala snapshot do 219 → 218 assere liderança → 219 rebaixa e **(A) re-ancora em W**. Manter sob firehose alguns minutos (vários heartbeats) para exercitar o caminho steady-state de (B).
4. **Critérios (read-only):**
   - **WARN some:** marcar timestamp T da linha `"Affinity handback: ... demoting"`; nenhum `peer watermark above its own applied` novo após T (`grep -c` antes vs 10 min depois **não** aumenta).
   - **Offset 0 no líder (218):** `applied == global` (Δ ≈47 → 0).
   - **Follower (219) == líder (218):** `node2.applied == node1.applied == node1.global`, amostrado várias vezes minutos à parte (estável, não fluke).
   - **Sem churn:** `grep -c "Requesting sync\|Starting sync\|reclaim"` para de crescer após o handback.
5. **Rollback:** restaurar o jar original nos 2, rolling 219→218. Tudo gateado por `affinityHandbackMode` (já on em prod) exceto (B), numericamente equivalente em quiescência — risco baixo.

---

## Achados secundários (fora do foco A+B — registrar/decidir depois)

- **C — Epoch do node-1 renasce em 1 no restart limpo (durabilidade).** `kill -TERM` sem wipe do data dir não deveria zerar o epoch; investigar persistência/leitura de `epochPath` (CC:315 `loadEpoch`, `persistEpoch`, flush no shutdown hook) no 218. Causou o churn de epoch e as eleições espúrias da 2ª volta. (Ideia do usuário de knob `wipeDataDir:true` é ortogonal — o bug é o epoch não sobreviver a um restart limpo.)
- **D — Detector super-dispara.** `syncReclaimLagThreshold=0` + reset agressivo do `reclaimCaughtUpLatch` (CC:1418/1550) fazem o WARN repetir a cada ~60s mesmo para skew benigno. Após A+B o offset é 0 e o ruído some; opcionalmente dar pequena tolerância/comparar fronteira por-tópico.
- **E — Path de erro de snapshot.** `handleSyncResponse` catch (RM:1253-1257) só faz `syncingTopics.remove` sem re-ancorar/limpar contadores — risco latente de stall/contagem semi-aplicada. Tornar o retry idempotente e re-ancorado.

## Definition of Done
- [ ] (A) re-anchor do incumbente nos 2 caminhos de demote (COMPLETE + backstop) e (B) odômetro lineage-fiel implementados.
- [ ] `RepeatedHandbackZeroOffsetE2ETest` verde + toda a suíte `-Presilience` verde (incl. E2E legados).
- [ ] Validação 218/219: offset 0 e WARN ausente após 2–3 voltas sob firehose; sem dual-leader; sem perda.
- [ ] Doc `doc/ngrid/oplog-ha-hardening.md` + runbook atualizados com o re-anchor simétrico do handback.
- [ ] Plano aprovado copiado para `/planning` (regra do projeto). Branch `fix/d11-handback-lineage-offset`, commits atômicos por fix (A, B, teste).
