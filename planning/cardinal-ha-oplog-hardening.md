# Plano — Endurecimento do op-log de HA (estratégia "A": fazer funcionar sob volume real)

## Contexto

A Cardinal (TEMS) usa o op-log da `nishi-utils-core` (`ReplicationManager` standalone) para HA
active/standby. A Fase 1 (failover de eleição) foi entregue na 4.1.2. Em pré-prod, com volume real
do Kafka (~2.000+ ops/s, estado de dezenas de MB), a replicação **não converge** e o cluster
**congela**. A investigação (logs + thread dumps + verificação adversarial) fechou as causas-raiz
abaixo. A decisão de produto é **estratégia "A": endurecer o op-log existente até funcionar de forma
madura sob o caso real**, mantendo a evolução para modelo state-based/coalescing como roadmap futuro.

## Causas-raiz confirmadas

1. **(RESOLVIDO) Backlog do índice de resend no líder.** O `indexReplicationPayload` rodava no fim
   do apply-local assíncrono (`completeOperation`), que ficava ~1.94M ops atrás do frontier de envio.
   Resultado: o líder reportava como "missing" ops que já tinha enviado → snapshot infinito.
   **Fix aplicado e verificado** (`Glob Seq == Applied` no líder): flag `leaderLocalApply=false` +
   commit/index **síncrono** ao atingir quórum (o engine da Cardinal é a fonte da verdade; o apply
   local no líder era redundante e gerava o backlog).

2. **Snapshot > limite de frame de 64MB.** Sob volume o snapshot excede o frame do transporte.
   **Fix aplicado:** snapshot multi-chunk byte-sliced (`onSnapshotInstalled` na lib + handler da
   Cardinal acumula/decodifica).

3. **Janitor D2 matava transferência multi-chunk em andamento.** Liberava o `syncingTopics` por
   falta de avanço de `nextExpected` (que só avança no último chunk) → `resetState` no meio →
   nunca converge. **Fix aplicado:** D2 libera por ausência de **chunk** (`lastSyncActivityByTopic`).

4. **FREEZE do follower = lock órfão (lost-unlock).** Causa do travamento do nó (verificado por 2
   thread dumps a 360s de distância, idênticos, CPU congelado: 4 threads do pool `ngrid-replication`
   parkadas em `sequenceBufferLock` (`ReplicationManager.java:636`) e **zero dono** do `ReentrantLock`).
   Uma thread adquiriu o lock e saiu sem `unlock()` pareado (provável `Error`/OOM escapando do
   `catch(Exception)`, ou na janela entre `lock()` e `try`). `ReentrantLock` não libera na morte da
   thread → lock eternamente "held" sem dono → `lock()` puro parka para sempre.

5. **Gap head-of-line + hot-loop de resend (85k).** Op evictada do log de resend do líder
   (`replicationLogRetention`) vira "missing" permanente; o follower martela o mesmo offset dezenas
   de milhares de vezes, satura o líder, atrasa o lease → **flapping de liderança**. Amplificado
   pelo check redundante `isOperationCommitted` (audit log de só 2000 entradas) que dá falso-"missing"
   mesmo para ops dentro da retention de 100k.

6. **Buffer de replicação ilimitado.** Follower preso no gap bufferiza o stream vivo sem limite →
   pressão de memória → o `Error` que dispara o lost-unlock (causa #4).

7. **Liderança frágil em 2 nós.** `minClusterSize=2` derruba a liderança quando um peer some
   (foi o que aconteceu quando o follower congelou).

8. **(secundário) I/O sob o lock.** `saveSequenceState` (disco) e `sendAck` (rede) na seção crítica
   do `sequenceBufferLock`, por op → tempo de retenção alto + gargalo de throughput.

## Fases de execução (commits atômicos, testes entre fases)

### Fase 0 — Consolidar o que já está verificado (commits)
Organizar as mudanças já aplicadas e verificadas em commits atômicos:
- (lib) Multi-chunk byte-sliced snapshot (`onSnapshotInstalled`).
- (lib) Janitor de sync por atividade de chunk (`lastSyncActivityByTopic`).
- (lib) `leaderLocalApply` + commit/index síncrono.
- (cardinal) handler multi-chunk + `leaderLocalApply(false)` + `replicationLogRetention(100k)`.

### Fase A1 — Resiliência do lock (o showstopper) — `nishi-utils-core`
- **`tryLock(timeout)` em vez de `lock()`** no caminho de replicação (`handleSyncResponse`,
  `handleReplicationRequest`, callbacks `onSuccess`, `processSequenceBuffer`). Timeout → erro
  recuperável (re-sync/re-tentativa) em vez de freeze permanente. **Auto-cura de lock órfão.**
- **`catch(Throwable)`** (não só `Exception`) nas tasks do executor + garantir liberação do lock.
- **Tirar I/O de baixo do lock:** `saveSequenceState` assíncrono + coalescido (não a cada op);
  `sendAck` fora da seção crítica.
- Commits: (1) tryLock+timeout no caminho de replicação; (2) catch Throwable nas tasks;
  (3) saveSequenceState async/coalescido; (4) sendAck fora do lock.

### Fase A2 — Despressurizar o hot-loop + bound do buffer — `nishi-utils-core`
- **Remover o `isOperationCommitted` redundante** no resend (`replicationLogBySequence` só contém
  payloads já committados/indexados; o re-check no audit log de 2000 causa falso-"missing").
- **Recuperação de gap evictado (skip-and-drain):** ao confirmar que a op foi produzida-e-evictada
  (líder responde "missing" e o follower já tem sequências maiores no buffer), pular o gap, drenar o
  buffer em massa e ir ao vivo. Quebra o head-of-line block. (Trade-off de consistência eventual,
  documentado; alinhado ao modelo aceito de "maior NodeId vence".)
- **Backoff no resend** (exponencial por offset) — não martelar o mesmo offset.
- **Cap no `sequenceBuffer`** — ao exceder, cair para snapshot/skip em vez de OOM.
- Commits: (5) remover isOperationCommitted redundante; (6) skip-and-drain de gap evictado;
  (7) backoff de resend; (8) cap do buffer.

### Fase A3 — Estabilidade de liderança em 2 nós — `nishi-utils-core` + cardinal
- **`minClusterSize=1` / pairMode** — líder não cai ao perder o peer; split-brain reconciliado por
  maior NodeId na reconexão (documentado). Já autorizado pelo usuário.
- Commits: (9) knob pairMode/minClusterSize na lib (se necessário) + config na Cardinal.

### Fase A4 — Testes + validação
- **Testes de resiliência (lib, RED→GREEN):**
  - Reproduzir lock órfão/contenção sob storm e provar que `tryLock`+recovery destrava (não congela).
  - Reproduzir gap evictado e provar `skip-and-drain` (follower vai ao vivo).
  - Reproduzir kill-de-peer em 2 nós e provar que o sobrevivente continua líder (minClusterSize=1).
- **Suíte completa verde:** `mvn -pl nishi-utils-core test` + `-Presilience`.
- **E2E pré-prod (Xmx48g):** follower converge e fica vivo num soak (≥ X min); matar líder → failover.
- **Release 4.1.3** + bump da Cardinal + doc/diagramas atualizados.

## Definition of Done (critérios de aceite)

- [ ] Follower converge para dentro de N ops do líder sob volume real e **mantém por ≥ X min sem
      freeze/deadlock** (soak).
- [ ] **Nenhum lock órfão** sob storm: um unlock vazado/contenção degrada para recovery (`tryLock`),
      não para freeze permanente.
- [ ] Buffer **não cresce sem limite** (cap + recovery).
- [ ] Hot-loop de resend **eliminado** (sem martelar o mesmo offset; sem falso-"missing").
- [ ] Matar o líder → **sobrevivente assume e serve** (minClusterSize=1), split-brain documentado.
- [ ] Suíte da lib **verde** + novos testes de resiliência **verdes** (RED→GREEN evidenciado).
- [ ] `catch(Throwable)` no caminho do executor; **sem I/O sob o lock**.
- [ ] Doc (`doc/ngrid`) + diagramas PlantUML atualizados (fluxo do lock, recuperação de gap, pairMode).

## Riscos / pontos de atenção

- **Concorrência é delicada:** mudanças no `sequenceBufferLock`/executor exigem testes RED→GREEN reais
  (storm) — loopback puro não reproduz; pode precisar de injeção de carga/falha.
- **skip-and-drain** sacrifica consistência forte por liveness (eventual LWW). Documentar o trade-off
  e o comportamento no failover.
- **A não coalescia** o volume (continua ~2k ops/s legítimas). Se mesmo o steady-state legítimo afogar
  o apply serial, A é o sinal empírico para migrar ao modelo state-based (roadmap B/C/E).
- **Sem fallback para legado:** o caminho defeituoso é substituído, não mantido em paralelo.

## Roadmap futuro (fora do escopo de A)

- Modelo **state-based / coalescing** (enviar valor atual de chaves alteradas com versão LWW, em vez
  de cada mutação em ordem) + anti-entropy periódico (Merkle/checksum). Ataca a raiz do volume e
  elimina head-of-line blocking por construção. A serve como o experimento que decide a necessidade.
