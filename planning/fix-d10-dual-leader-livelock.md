# Fix D10 — Dual-leader livelock no handoff por afinidade sob produção contínua

**Repo:** `/home/lucas/Projects/nishisan/nishi-utils` · **Módulo:** `nishi-utils-core`
**Branch:** `fix/d10-dual-leader-livelock` a partir de `fix/d8-divergent-history-reclaim` (stacked sobre a PR #142; rebase para main após o merge)
**Análise base:** `planning/issue-tems9-d10-dual-leader-livelock.md`

## Contexto

A validação do D9 em pré-prod (2026-06-10) expôs o D10: no rejoin do nó de maior afinidade sob firehose contínuo, o cluster entra em **dual-leader estável** — o gate B do incumbente (threshold 0) nunca abre porque o delta in-flight nunca zera; o candidato arma o `reclaimCaughtUpLatch` contra watermark de heartbeat stale e assume; nenhum lado cede e a mecânica de epoch vira escada infinita de re-stamps (22→105+). Contenção atual é manual (SIGTERM + wipe + cold bootstrap).

Sobre a sugestão original ("incumbente para e espera sincronizar para o handoff"): pausar pelo catch-up **inteiro** seria indisponibilidade de escrita não-bounded (64s no incidente). A versão refinada adotada (fix b) mantém o catch-up assíncrono com produção ligada e **só pausa no trecho final** (candidato a ≤ threshold de aproximação), com duração máxima e cooldown — pausa de segundos, não de minutos.

**Escopo aprovado:** pacote completo (a)+(b)+(c). **(b) é opt-in** (default false, como `leaderPauseOnJoin`). F2 do D9 permanece intocado (gate A nunca evicta o líder corrente).

## Diagnóstico confirmado do (a) — release precoce do join-quiesce

Verificado no código (não é mais hipótese):

- **H1 (causa primária — mismatch de escala):** o release compara o `lastAppliedSequence` do follower (escala da linhagem do cluster) contra `globalSequence.get()` cru (`ReplicationManager.java:2532, 2564, 2597`). Um incumbente **promovido de follower** tem `globalSequence` baixo (contador de produção local, nunca re-ancorado em promoção sem snapshot cutover) enquanto seu watermark real é `max(globalSequence, lastAppliedSequence)` — a fórmula que o próprio `advertisedLeaderHighWatermark()` já usa (`ReplicationManager.java:486-493`). O primeiro `FOLLOWER_PROGRESS` do candidato (seeded alto pela re-âncora do D9) satisfaz a condição trivialmente → release em ~1,5s.
- **H2:** o follower manda `lastAppliedSequence` cru mesmo com bootstrap gate engajado, e o líder ignora o campo `epoch()` do `FollowerProgressPayload` (`ReplicationManager.java:2556-2583`).
- **H3:** `followerAppliedByNode` nunca é limpo em disconnect/rejoin — entrada stale da sessão anterior pode liberar via `checkJoinQuiesce()` (`:2599`).

## Fase 1 — Fix (a): join-quiesce cobre o catch-up real

Arquivo: `ReplicationManager.java`.

1. Helper `long leaderQuiesceTarget()` = `Math.max(globalSequence.get(), lastAppliedSequence)`; usar nos 3 pontos (`onMembershipChanged`, `handleFollowerProgress`, `checkJoinQuiesce`).
2. `followerAppliedByNode`: `Map<NodeId, Long>` → `Map<NodeId, FollowerProgress>` (record interno `applied/epoch/atMillis`).
3. `handleFollowerProgress()`: descartar payload com `appliedSequence() < 0` ou `epoch() != coordinator.getLeaderEpoch()`.
4. `onMembershipChanged()`: para membro novo (fora de `knownActiveMembers`), `followerAppliedByNode.remove(member)` antes de avaliar.
5. `checkJoinQuiesce()`: `removeIf` exige entrada fresca (idade ≤ 3× `followerProgressInterval`) e epoch corrente. Timeout (`joinQuiesceMaxDuration`) segue como único release não-catch-up.
6. `sendFollowerProgress()`: reportar `advertisedLeaderHighWatermark()` (−1 enquanto bootstrap-gated) em vez de `lastAppliedSequence` cru.

## Fase 2 — Fix (b): quiesce-assisted reclaim ("leader pause on reclaim")

Espelho do join-quiesce, estado **separado** (não reusar `joinQuiescing`: triggers e releases distintos; se join-quiesce ativo, reclaim-quiesce não engaja).

**Estado novo (RM):** `AtomicBoolean reclaimQuiescing`, `volatile NodeId reclaimQuiesceFor`, `volatile long reclaimQuiesceStartedMs`, `volatile long reclaimQuiesceCooldownUntilMs`.

**Configs novas** (padrão do `leaderPauseOnJoin`: `ReplicationConfig.Builder` + plumbing `NGridConfig`/`NGridNodeBuilder`/`NGridNode`):

| Config | Default |
|---|---|
| `leaderPauseOnReclaim` | `false` (opt-in; TEMS/Cardinal habilitam) |
| `reclaimQuiesceThreshold` | `1024` seqs ("perto") |
| `reclaimQuiesceMaxDuration` | `5s` (pausa bounded) |
| `reclaimQuiesceCooldown` | `60s` (anti-loop) |

**Engage** — `checkReclaimQuiesce()` agendado a 200ms (junto de `checkJoinQuiesce`, `:375`) + reavaliação em `handleFollowerProgress`. Todas as condições: líder, feature ligada, sem join-quiesce ativo, fora de cooldown; candidato ativo de afinidade **estritamente maior** (mesmo comparator da eleição: priority, depois nodeId); progresso fresco com epoch corrente e `applied >= 0`; `0 <= leaderQuiesceTarget() - applied <= reclaimQuiesceThreshold`.

**Pausa** — em `replicate()` (`:765-771`), após o check de join-quiesce: lançar `LeaderSyncingException` (clientes já fazem retry).

**Handoff coordenado — os gates existentes convergem, sem mecânica nova de cessão:**
1. Produção pausada → watermark do incumbente congela em W.
2. Candidato streama até W (fetch/apply independem de produção).
3. **Aceleração:** em `handleFollowerProgress`, quando `reclaimQuiescing && source == reclaimQuiesceFor && applied >= leaderQuiesceTarget()` → novo `ClusterCoordinator.noteFollowerWatermark(NodeId, long)` (merge max em `peerHighWatermark` + `reevaluateLeadership()`) — sem esperar o próximo heartbeat (até 3s).
4. Gate B (`candidateIsBehindLocalWatermark`, `ClusterCoordinator.java:979-989`) abre → `recomputeLeader()` elege o candidato → incumbente vira follower.
5. **Release:** em `onLeaderChanged()` do RM, junto de `clearJoinQuiesce()`, chamar `clearReclaimQuiesce()`.
6. Gate A do candidato arma o latch contra watermark **congelado** = caught-up verdadeiro (a fraqueza do latch stale vira correção).

**Abort:** timeout (`reclaimQuiesceMaxDuration`) ou candidato saiu da membership → WARN, release, cooldown, **retém liderança** (disponibilidade primeiro).

## Fase 3 — Fix (c) parte 1: flag de líder no heartbeat (retro-compatível)

- `HeartbeatPayload` (`common/HeartbeatPayload.java`): novo campo `boolean leader` (Jackson default false quando ausente). `sendHeartbeat()` (`ClusterCoordinator.java:~658`) passa `isLeader()`.
- `BinaryFrameCodec` (`cluster/transport/codec/BinaryFrameCodec.java`): +1 byte ao **final** do frame; decode lê só se `buffer.remaining() > 0` (frame antigo → false; decoder antigo ignora byte extra — leitura sequencial para após os 3 longs). Atualizar javadoc do layout. Precedente: `leaderUnavailable` do D9.
- Alternativa por inferência (sem mudança de protocolo) avaliada e rejeitada: ambígua; o frame evoluir é o primeiro passo da fundação epoch-aware já planejada como follow-up.

## Fase 4 — Fix (c) parte 2: detecção + resolução determinística

No `ClusterCoordinator`, ramo HEARTBEAT do `onMessage` (**antes** de `observeEpoch`):

- **Detecção:** `payload.leader() && isLeader() && source != local` → `dualLeaderObservations.merge(source, 1, sum)`. Heartbeat sem flag, peer desconectado ou perda de liderança local → zera contagem.
- **Debounce:** resolver só com **3 heartbeats consecutivos** com flag (constante documentada; ~1s em teste com HB 300ms, ~9s em pré-prod). Janela de overlap de handoff legítimo é sub-heartbeat — nunca atinge o debounce.
- **Resolução `resolveDualLeader(rival)`** (sob `leaderComputationLock`): comparator idêntico ao da eleição (priority > , tie-break nodeId). Ordem total estrita ⇒ ambos os lados computam o mesmo vencedor ⇒ **nunca os dois cedem**. Info do rival desconhecida/placeholder (port ≤ 0) → não resolve (nunca decidir sobre prioridade desconhecida).
  - **Perdedor:** WARNING, invoca `dualLeaderYieldHook` (registrado pelo RM), `updateLeader(rival)`, limpa contagem.
  - **Vencedor:** retém; WARNING rate-limited. Nenhuma ação.
- **Supressão da escada de epoch:** durante o debounce, se o comparator diz que o local perderá → flag `yieldingToDualLeader`; `observeEpoch()` adota `observed` (sem `+1`) enquanto ativa. Escada morre em ≤1 ciclo de heartbeat.
- **Pós-step-down (reuso integral do D8):** novo `ReplicationManager.onDualLeaderYield()`: seta `dualLeaderYielding = true` e arma `relayPendingBootstrap` para todos os tópicos (linhagem dual é divergente por definição). O ciclo D8 existente faz o resto: `drainRelayOnce()` → `requestSync` → `completeSnapshotCutover()` (SET dos contadores + `ResendLog.truncate()` da cauda dual + re-âncora do cursor). Enquanto pending: anuncia −1 + inelegível.
  - **Guard de corrida obrigatório:** o ramo "líder promovido auto-desarma pending" de `drainRelayOnce()` (`:1624-1628`) passa a exigir `coordinator.isLeader() && !dualLeaderYielding`. Flag limpa no cutover quando `relayPendingBootstrap.isEmpty()`.
- A cauda produzida pelo perdedor na janela dual é **descartada** — comportamento já documentado/aceito na contenção operacional (wipe + cold bootstrap), agora automático e bounded.

## Fase 5 — Testes

Modelos: `LeaderlessStalemateEscapeTest` (coordinator), `DivergentLineageBootstrapGateTest` (RM harness), `CleanRestartExLeaderRejoinE2ETest` (E2E).

| Teste | Nível | Cobre |
|---|---|---|
| `JoinQuiesceReleaseGateTest` | unit RM | H1 (reproduz o release de 1,5s — **falha hoje**), H2, H3; regressões |
| `ReclaimQuiesceTest` | unit RM | engage só com candidato de maior afinidade dentro do threshold; não engaja (delta alto / menor afinidade / progresso stale / join-quiesce ativo); pausa em `replicate()`; timeout + cooldown; clear em `onLeaderChanged` |
| `DualLeaderResolutionTest` | unit coordinator | perdedor cede após debounce (hook 1×); vencedor nunca cede; flap não resolve; escada de epoch suprimida; tie-break por nodeId; info desconhecida não resolve |
| `BinaryFrameCodecTest` (+JSON) | unit codec | round-trip com flag; frame antigo (curto) → `leader=false`; JSON sem campo → false |
| `DualLeaderYieldResyncTest` | unit RM | yield arma pending; guard `dualLeaderYielding` em `drainRelayOnce`; cutover limpa flag/trunca/SETa |
| `DualLeaderLivelockE2ETest` | E2E | **novo padrão: produção contínua durante handoff** (thread + `offerWithRetry` registrando acks). 2 nós prio 100/50, HB 300ms, `leaderPauseOnJoin+OnReclaim(true)`. node-1 sai limpo, node-2 promove, produção segue, node-1 religa. Asserts: líder único dentro do deadline; nunca 2 líderes por > ~2 ciclos de HB; delta de epoch bounded (< 10); janela máxima de rejeição de escrita < `reclaimQuiesceMaxDuration` + margem; prova por conteúdo (todo ack presente 1×, zero duplicatas). **Falha hoje.** |

Regressões: `mvn -pl nishi-utils-core test` (477 testes hoje, 0 falhas) e `mvn test -Presilience` — atenção a `CleanRestartExLeaderRejoinE2ETest`, `DivergentLineageReclaimE2ETest`, `StaleFrontierReclaimE2ETest`, `LeaderSyncBeforeReclaimE2ETest` (tocamos join-quiesce, heartbeat e eleição).

## Fase 6 — Commits atômicos (conventional commits PT-BR, sem referência ao agente)

1. `fix(replication): join-quiesce cobre o catch-up real do candidato (tems#9, D10a)` — Fase 1 + `JoinQuiesceReleaseGateTest`
2. `feat(replication): quiesce-assisted reclaim — leader pause on reclaim (tems#9, D10b)` — Fase 2 + `ReclaimQuiesceTest`
3. `feat(coordination): flag de líder no heartbeat com frame binário retro-compatível` — Fase 3 + testes de codec
4. `fix(coordination): detecção e resolução determinística de dual-leader (tems#9, D10c)` — Fase 4 + `DualLeaderResolutionTest` + `DualLeaderYieldResyncTest`
5. `test(ngrid): E2E de handoff por afinidade sob produção contínua (tems#9, D10)` — `DualLeaderLivelockE2ETest`
6. `docs(ngrid): documenta quiesce-assisted reclaim e resolução de dual-leader (tems#9, D10)` — Fase 7

Cada commit compila e passa a suíte isoladamente.

## Fase 7 — Documentação

- `planning/issue-tems9-d10-dual-leader-livelock.md`: registrar diagnóstico confirmado do (a) e desenho final; atualizar status.
- `planning/fix-d10-dual-leader-livelock.md`: cópia deste plano (convenção do repo).
- `doc/ngrid/oplog-ha-hardening.md`: corrigir seção 3b (release do pause-on-join) + novas seções "Leader pause on reclaim" e "Resolução determinística de dual-leader" (configs novas).
- `doc/ngrid/playbook-resiliencia.md` e `doc/runbooks/unstable_leader.md`: substituir contenção manual do dual (SIGTERM+wipe) pela autocorreção, destacando o descarte da cauda do perdedor.
- `doc/CHANGELOG.md`: entrada do D10.

## Verificação fim-a-fim

1. `JoinQuiesceReleaseGateTest` (caso H1) e `DualLeaderLivelockE2ETest` **falham antes** dos fixes e passam depois — prova de reprodução.
2. Suíte completa `mvn -pl nishi-utils-core test` + `mvn test -Presilience` verdes.
3. Validação manual em pré-prod (mesmo roteiro do incidente de 2026-06-10): firehose contínuo, restart limpo do node-1 (prio 100); observar join-quiesce cobrindo o catch-up real, handoff coordenado com pausa < 5s, epoch estável, zero `re-stamp above observed` recorrente; reverter a contenção operacional do runbook.

## Arquivos críticos

- `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java`
- `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java`
- `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/HeartbeatPayload.java`
- `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/BinaryFrameCodec.java`
- `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationConfig.java` (+ plumbing `structures/NGridConfig.java`, `NGridNodeBuilder.java`, `NGridNode.java`)
