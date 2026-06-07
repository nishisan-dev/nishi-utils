# Plano de execução — #124 Relay-log no follower do ReplicationManager (modo ultracode, por fases)

> Plano aprovado para o ciclo dedicado da #124, em cima do design `issue-124-relay-log-follower.md`.
> Fundações já entregues no 4.3.0: retenção temporal do op-log (#122) e gate `sync-before-lead` (#123).
> Branch: `feature/ngrid-relay-log-follower`.

## Contexto

Sob volume de produção (~2.8k ops/s, 2 nós) o follower do `ReplicationManager` entra em **espiral de
morte**: quando o gap passa de `MAX_SEQUENCE_BUFFER` (250k) a lib pede snapshot, `resetState()` zera o
estado em memória e reinstala um full-snapshot que cresce com o estado (379 → 716 MB); o líder fica em
starvation servindo snapshots gigantes e o follower nunca converge. O design substitui o buffer em memória
por um **relay-log persistente em disco (NQueue), uma por tópico**, desacoplando recepção (IO path barato,
nunca dropa) de aplicação (apply consumer no próprio ritmo). Decisões A–F do doc fechadas.

## Achados de viabilidade (exploração + agente Plan) que afinam o spike

- **GAP make-or-break:** retenção `TIME_BASED` da NQueue trunca registros **não-consumidos na cabeça**
  (`CompactionEngine.findTimeBasedCutoff` ignora `consumerOffset`) — inverte a premissa da Decisão D. Exige
  extensão `withRetentionClampToConsumer` (clamp do cutoff ao consumer) + detecção explícita de
  `lag>retenção`→bootstrap. **A propor no gate, não implementar no spike.**
- **Serialização:** `ReplicationPayload`/`data` não são `Serializable`; NQueue usa `ObjectOutputStream`.
  Relay como `NQueue<byte[]>` com `RelayEntry(epoch,sequence,topic,operationId,byte[] payloadBytes)`,
  `payloadBytes` codificado pelo codec Jackson do cluster.
- **Idempotência não-universal:** map (PUT/REMOVE) é LWW; queue `OFFER` duplica em re-apply. Exactly-once
  vem do **fencing por seq no consumer do relay**, não do handler.
- **Dois cursores duráveis:** `consumerOffset` (NQueue, per-poll) vs `nextExpected` (coalescido) — reconciliar.
- **Invariantes do relay NQueue:** `enableMemoryBuffer=false`, `allowShortCircuit=false`, fsync calibrado.

## Fases (workflows com check-in)

1. **GATE — Spike** NQueue-como-relay (peek/poll/retenção/crash/throughput). Saída: relatório + proposta da
   extensão NQueue para sign-off. **Para no gate.**
2. **DESIGN** — refina as 8 fases; fecha serialização, gap-fill, cursores, drain-gate, init. Mostra antes de codar.
3. **IMPLEMENTAÇÃO** (commits atômicos): FollowerIngestMode+wiring → IO path (shadow-write) → Apply consumer →
   Snapshot bootstrap-only → Failover drain-gate → Cutover (aposentar buffer).
4. **VERIFICAÇÃO** — adversarial (idempotência, failover-sob-carga) + soak reproduzindo e eliminando a espiral.

## Arquivos críticos

- `ngrid/replication/ReplicationManager.java`, `ReplicationConfig.java`
- `queue/NQueue.java`, `queue/CompactionEngine.java` (extensão clamp-ao-consumer)
- `ngrid/structures/NGridConfig.java`, `ngrid/structures/NGridNode.java`
- `ngrid/config/NGridYamlConfig.java` (+ loader), facade `NGrid`
- `ngrid/queue/QueueClusterService.java`, `ngrid/map/MapClusterService.java`

## Verificação local (ngrid NÃO roda no CI)

```bash
mvn clean install -DskipTests
mvn test -Presilience
mvn verify -Pdocker-resilience
mvn verify -pl nishi-utils-core -DexcludeNgrid=true -Pvalidate-javadoc
mvn test -Psoak -Dngrid.soak.durationMinutes=...   # aceite central
```

## Restrições

pt-BR; commits atômicos; sem atribuição de IA; Java 21; API pública do 4.3.0 preservada (aditivo).
