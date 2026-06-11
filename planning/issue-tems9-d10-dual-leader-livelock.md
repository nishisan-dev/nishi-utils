# D10 — Dual-leader livelock no handoff por afinidade sob produção contínua

> Descoberto em 2026-06-10 (tarde) durante a validação do D9 em pré-prod. Mesmo ciclo da issue
> tems#9. **ABERTO** — defeito pré-existente que era mascarado pela abdicação patológica do D9;
> com o F2 do D9 removendo a capitulação, o estado latente aflorou como dual-leader estável.

## Sintoma (ambiente real, 2 nós, firehose contínuo)

1. node-1 (priority 100) religa LIMPO, streama a cauda do incumbente (F1 do D9 ok, ~64s de
   catch-up) e **reclama a liderança por afinidade** (17:33:50).
2. node-2 (incumbente) **nunca cede**: sob produção contínua o candidato fica eternamente "atrás"
   pelo delta in-flight, e o gate B é estrito (threshold 0).
3. Resultado: **dual-leader estável** com escada infinita de re-stamps de epoch (22→105+, um
   re-stamp "above observed" a cada heartbeat de 3s), **ambos consumindo o consumer group** com as
   partições divididas, e o applied do node-1 rastejando.
4. Não autocorrige. Contenção manual: SIGTERM no nó de menor afinidade (node-2, 17:37:15) →
   líder único; ressync do zero do nó parado (wipe var + cold bootstrap — o estado da janela dual
   é descartado).

## Cadeia causal (evidências de log + código)

1. **Gate B estrito + produção contínua**: o incumbente só cede quando o candidato de maior
   afinidade está caught-up (`syncReclaimLagThreshold = 0`); com o firehose produzindo, o delta
   in-flight nunca zera → o incumbente retém para sempre. Correto isoladamente; sem mecanismo de
   emparelhamento, vira retenção eterna.
2. **Join-quiesce liberou cedo demais**: `leaderPauseOnJoin` engatou no rejoin (17:32:44) mas
   liberou em **1,5s** (17:32:45.320 `Join-quiesce released; resuming production`) — o catch-up
   real levou 64s, e `joinQuiesceMaxDurationMs` é 30s. A janela que deveria deixar o candidato
   emparelhar não cobriu nada. **Investigar a condição de liberação** (suspeita: o critério de
   "candidato sincronizado" avalia um watermark stale/parcial logo após o connect).
3. **Reclaim do candidato por latch contra watermark stale**: o `reclaimCaughtUpLatch` do gate A
   armou comparando com o watermark anunciado do incumbente (heartbeat de até 3s atrás) — o
   candidato se vê caught-up contra um alvo que já andou. Ele eleva epoch e assume.
4. **Sem resolução de dual-leader**: cada lado re-stampa o epoch acima do observado ao ver o
   heartbeat do outro (mecânica anti-split-brain de epoch virou o motor da escada). Nenhum dos
   dois compara afinidade+watermark para decidir deterministicamente quem cede.
5. **O F2 do D9 não é o culpado** — a capitulação que ele removeu era pior (abdicação do líder
   saudável + leaderless de 62 min) e "resolvia" o dual por acidente. O F2 fica; falta o par dele.

## Direções de fix (próximo ciclo)

- **(a) Liberação precoce do join-quiesce**: diagnosticar e corrigir a condição de release — o
  quiesce deve cobrir o catch-up real (até `joinQuiesceMaxDurationMs`), não 1,5s.
- **(b) Quiesce-assisted reclaim ("leader pause on reclaim")**: espelho do pause-on-join — quando
  um candidato de MAIOR afinidade se aproxima (dentro de tolerância), o incumbente pausa a
  produção, deixa o candidato emparelhar exato e cede de forma coordenada. Mata a causa-raiz: o
  delta in-flight eterno.
- **(c) Detecção de dual-leader + resolução determinística**: dois líderes na membership se
  observando → o de MENOR afinidade cede (step-down) e ressinca; quebra a escada de re-stamps em
  um ciclo de heartbeat.
- **(d)** F2 do D9 permanece como está; (b) e (c) são o complemento que faltava.

## Contenção operacional (até o fix)

- **Evitar restart do nó de maior afinidade** (node-1): o caminho de reclaim por afinidade é o
  que entra em dual sob produção contínua.
- Se entrar em dual: SIGTERM no nó de menor afinidade → líder único; depois wipe do var do nó
  parado e cold bootstrap (ressync do zero) — o estado produzido na janela dual não é confiável.

## Evidências (pré-prod 2026-06-10, 17:27–17:40)

- 17:32:44 → 17:32:45.320: `Join-quiesce released; resuming production` (1,5s; catch-up real 64s).
- 17:33:50: node-1 reclama por afinidade (latch armado contra watermark stale).
- 17:33:50 → 17:37:15: dual-leader; `converged to N (leader re-stamp above observed N-1)` a cada
  ~3s nos dois nós (epochs 22→105+); ambos com partições do consumer group.
- 17:37:15: SIGTERM node-2 (contenção); 17:40: regime estável (node-1 LEADER, node-2 FOLLOWER
  ressincado do zero, lag 0.0s, snapshot fallbacks 0).
- 0 ocorrências de `Refusing RELAY_STREAM_FETCH` / `taking leadership` — o F3 do D9 não participou.

## Relacionados

- D9 (`issue-tems9-d9-leaderless-stalemate.md`): o F2 de lá expôs este defeito latente.
- Follow-up de ordenação epoch-aware de linhagem (D8/D9): a resolução determinística do item (c)
  provavelmente compartilha a mesma fundação.
