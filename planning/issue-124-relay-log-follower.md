# Design — #124 Relay-log no follower (receber+persistir desacoplado do apply)

> **Status:** design aprovado em princípio (brainstorm fechado). **Ciclo dedicado**, sequenciado
> **depois** da PR de #122 + #123 (que são a fundação). Sem código nesta fase — este doc orienta o
> ciclo de implementação.

## Contexto / problema

Follower sob volume de produção (pré-prod, 2 nós) entra em **espiral de morte**:

- Líder ~2.800 ops/s; progresso do follower ~1.400/s, dominado por re-installs de snapshot.
- Quando o gap passa de `MAX_SEQUENCE_BUFFER` (250k), a lib pede snapshot; `resetState()` **zera o
  estado em memória** e reinstala um snapshot **completo que cresce com o estado** (379 → 716 MB).
- O líder fica em starvation servindo snapshots gigantes; o follower nunca converge.

A reação ao atraso (`apply < produção`) é **destruir o estado e recarregar tudo** — o oposto do
desejável.

## Modelo-alvo (relay log estilo MySQL)

Desacoplar **recepção** de **aplicação** no follower:

1. **IO (receber+persistir):** cada entrada do op-log recebida é gravada num **relay log persistente
   em disco (NQueue)**. Barato (só bytes), acompanha a vazão do líder, **nunca dropa**. Bounded por
   tempo/tamanho (reusa a retenção temporal da #122).
2. **Apply:** um consumidor separado drena o relay **no próprio ritmo**, atualizando o estado em
   memória. Pode ficar defasado (replica lag) — aceitável para standby. Lag ≠ perda.
3. **Sem snapshot/`resetState` em regime:** snapshot só no **bootstrap** (follower novo, relay+estado
   vazios) ou quando o follower ficou fora **mais que a retenção** do relay. Follower só atrasado
   **alcança pelo relay**, nunca zerando estado + full-snapshot.
4. **Failover gated por drain:** follower promovido a líder **só ativa o consumo depois de drenar o
   relay (lag → 0)**. Generaliza o sync-before-lead da #123 (hoje espera `isLeaderSyncing()`; passa a
   esperar "relay drenado").

## Decisões de design (fechadas no brainstorm)

### A. Cutover definitivo
Pipeline de relay é **aditivo atrás de um flag de modo**, mas o destino é **cutover** para o relay
como caminho de ingestão do follower — sem dual-mode eterno (alinhado a "ir sempre à frente"). O
buffer em memória atual (`sequenceBufferByTopic`/`processSequenceBuffer`/`resetState`) é aposentado
no modo relay.

### B. Ordenação — por ordem de chegada + chave `<epoch>-<seq>`
Grava em **ordem de chegada**; cada entrada carrega a chave de ordenação `<leaderEpoch>-<sequence>`
(ex.: `100-1, 100-2, 100-3 … 200-1` após troca de líder/epoch). **Apenas o líder produz** (caso do
cliente atual) → ordem de chegada ≈ ordem de produção; ordenação não é problema no regime esperado.
O `epoch` no prefixo dá ordem total mesmo através de trocas de liderança e serve de cerca (fencing)
contra entradas de epoch obsoleto. Gaps por hiccup de transporte são preenchidos por **resend**
servido pelo op-log do líder retido por tempo (#122).

### C. Uma NQueue por tópico
O op-log é por-tópico (sequências por tópico). **Um relay NQueue por tópico** (casa com o modelo
atual de sequência) em vez de uma fila única intercalando tópicos. Custo: mais file handles —
aceitável.

### D. Leitura não-destrutiva — `peek` → apply idempotente → `poll`/remove
O consumer de apply faz: **`peek()`** a cabeça → **apply** → **`poll`/remove** (avança). Sem cursor
durável separado: a **cabeça do relay É o frontier de apply**. Crash entre apply e remove →
re-apply no restart; **apply é idempotente** (LWW whole-object + fencing por sequência/epoch), logo
at-least-once + apply idempotente = efetivamente-once. Retenção (#122 tempo/tamanho) limita o
**backlog não-aplicado** (tail), não a cabeça.
- Começa com `peek` simples (1 a 1): simplicidade > throughput.
- `batchPeek`/`peek(n)` fica como **evolução futura** — traz consumo parcial (precisaria de
  ack/commit parcial), por isso não entra no primeiro corte.

### E. `apply < produção` sustentado — aceitar e medir
O relay converte a **falha catastrófica** (reset + snapshot crescente + starvation) em **replica lag
limitado**. Se `apply < produção` for **sustentado** (não rajada), o relay cresce até a retenção
truncar → aí, e só aí, um **bootstrap snapshot** é necessário (caminho raro). O relay **não cria
capacidade de apply** que não existe — mas remove a espiral. Comportamento real só medindo; evoluir
isso amadurece o projeto.

### F. Flags follower/leader separadas
`leaderLocalApply` é knob **do líder**. O modo relay é **do follower**. Modelar como flag composável
separada (ex.: `FollowerIngestMode { INLINE, RELAY_LOG }`), sem overloadar um OpMode global que
confunda os dois lados.

## Arquitetura (modo RELAY_LOG)

```
            ┌─────────────────────────── Follower ───────────────────────────┐
REPLICATION │  IO path (barato, acompanha o líder)                            │
_REQUEST ──▶│  handleReplication → relayLog[topic].offer(<epoch-seq, payload>)│
            │                                   │ (nunca dropa; bounded #122) │
            │  Apply path (próprio ritmo)       ▼                             │
            │  loop: e = relayLog[topic].peek()                               │
            │        if gap(e) → requestResend (op-log do líder #122)         │
            │        else apply(e) [idempotente] ; relayLog[topic].poll()     │
            └─────────────────────────────────────────────────────────────────┘
```

- **Bootstrap snapshot:** só quando relay+estado vazios (follower novo) ou lag > retenção do relay.
- **Failover (drain-gate):** ao ser eleito, o nó **drena o relay até lag 0** antes de aceitar
  escrita; integra com o gate `LeaderSyncingException` da #123 (o gate passa a cobrir "relay não
  drenado", não só "sync de snapshot em curso").

## Dependências

- **#122** — o relay É o op-log persistido por tempo; a retenção temporal bounded vem de lá.
- **#123** — "drenar relay antes de promover" é a forma forte do sync-before-lead; reusa o gate.

## Riscos / questões abertas (resolver no início do ciclo)

1. **Spike NQueue como log replayável (make-or-break):** validar `peek` não-destrutivo + `poll` na
   cabeça + truncamento por retenção (tempo/tamanho) independente do ritmo de apply, sob a volumetria
   alvo. Confirmar custo de I/O do "offer por op recebida" acompanhando ~3k ops/s. Se faltar algo,
   é extensão pontual na NQueue — **antes** de cravar como substrato.
2. **Garantia de idempotência do apply** sob re-apply pós-crash (LWW + fencing por epoch/seq) —
   provar em teste de crash no meio do par apply→poll.
3. **Multi-tópico:** número de relay NQueues × file handles/descritores sob N tópicos.
4. **`apply < produção` sustentado:** caminho de bootstrap quando o tail excede a retenção; métrica
   de lag e alarme.
5. **Failover catch-up vs retenção do Kafka:** durante o drain, consumo do Kafka pausa; mensagens não
   se perdem (offset do grupo compartilhado), limitado por `retenção Kafka × tempo de catch-up` —
   dimensionar.

## Plano por fases (rascunho do ciclo dedicado)

1. **Spike** NQueue-como-relay (peek/poll/retenção/throughput) → decide substrato.
2. `FollowerIngestMode` + wiring (flag follower, composável; sem mexer no líder).
3. **IO path:** gravar cada `REPLICATION_REQUEST` no relay por tópico (chave `<epoch>-<seq>`).
4. **Apply consumer:** peek → (gap? resend : apply idempotente) → poll; métrica de relay lag.
5. **Snapshot bootstrap-only:** desliga `resetState`/full-snapshot do hot-path; snapshot só em
   bootstrap/lag>retenção.
6. **Failover drain-gate:** promoção só após relay drenado; integra com o gate da #123.
7. **Aposentar** o buffer em memória no modo relay (cutover).
8. **Testes:** crash no par apply→poll (idempotência), failover-sob-carga (sem espiral), soak.

## Fora de escopo
- A PR atual (#122 + #123). A #124 entra **depois** dela mergeada.
- `batchPeek`/`peek(n)` (evolução pós primeiro corte).
