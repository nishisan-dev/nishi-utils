# Plano — Issue #115: DistributedMap leader-local by-reference

## Context

Migrando o estado quente do `tevent-cardinal` (motor de correlação TEMS) de
`ConcurrentHashMap` para `DistributedMap` (ngrid), em cenário **active-standby**
(só o líder escreve). O código legado depende da semântica de **referência
compartilhada** do `ConcurrentHashMap`: `cache.put(id, dto)` seguido de
`cache.get(id)` devolve **a mesma instância**, e mutações in-place no objeto
obtido são visíveis sem um `put` explícito.

Hoje o `DistributedMap` **quebra** essa expectativa: mesmo no líder de 1 nó, o
`put` serializa o valor e o `data` local recebe a **cópia desserializada** (não a
referência original). Confirmado no código:

- `MapClusterService.put` (`MapClusterService.java:133-140`) faz `encode` +
  `replicate`, **não** faz `data.put` local. O `data.put` ocorre só no `apply`
  (`MapClusterService.java:327-382`), com o valor **decodificado**.
- O apply local do líder roda no executor de replicação
  (`ReplicationManager.checkCompletion`, `:436-455`), mas `put()` **bloqueia**
  em `waitForReplication(future.get())` e a *future* só completa em
  `completeOperation` **após** o apply. Logo **read-after-write já é garantido**;
  o que falha é a **identidade** (`get(k) == v` → false, é cópia).

A issue pede um modo **opcional** "leader-local by-reference": no líder, guardar a
**referência original** no `data` local; a serialização continua **só** para
replicar aos followers (que recebem cópia decodificada, com fidelidade de tipos).

**Escopo deste plano:** apenas o **Eixo 1 (by-reference local)**. O **Eixo 2**
(identidade compartilhada **entre** mapas, levantado no comentário da issue) fica
como **RFC separada**, direção escolhida **(C) `DistributedRef`** — ver Apêndice.

## Decisões confirmadas

1. **Abordagem dual-payload** (espelha o que o `QueueClusterService` já faz: passa
   objeto vivo como payload e deixa o transporte serializar para followers). No
   mapa, separa-se o *payload de wire* (bytes — followers/log/resend/snapshot) do
   *payload de apply local do líder* (objeto vivo `MapReplicationCommand`).
2. **Sem modo "apply síncrono" extra.** Read-after-write já é garantido pela espera
   na *future* de commit; aplicar antes do quorum quebraria o invariante
   commit-após-quorum. Responde à pergunta nº 2 da issue.
3. **Flag opt-in `leaderLocalByReference`**, default `false`, por-mapa
   (`MapConfig`) com default global (`NGridConfig`), espelhando o padrão de
   `mapPersistenceMode`.
4. **Persistência (R2):** para mapas by-reference com WAL habilitado, o ramo PUT do
   `apply` usa `appendSync` (captura o estado-no-apply, idêntico ao replicado aos
   followers; determinístico). Mutações in-place posteriores sem novo `put` **não**
   são replicadas nem persistidas — limitação documentada e aceita pela issue.
   Guardrail PRODUCTION **mantido** (sem exceção); hot-state puro roda com
   persistência `DISABLED` em DEV/STAGING.

## Mudanças por arquivo

### 1. `ReplicationManager.java` — infra dual-payload (comportamento inalterado por default)
- `PendingOperation`: adicionar campo `final Object localApplyPayload`; novo
  construtor `(opId, topic, payload, localApplyPayload, epoch, quorum)`; o
  construtor atual delega com `localApplyPayload = payload`.
- Novo overload `replicate(String topic, Object wirePayload, Object localApplyPayload, Integer quorumOverride)`;
  os overloads existentes (`:320`, `:324`) delegam com `localApplyPayload = wirePayload`.
- **Troca de uma única linha** no apply local do líder (`:438`):
  `handler.apply(operation.operationId, operation.localApplyPayload)`.
- **Manter** `operation.payload` (bytes) em `replicateToFollowers` (`:372`),
  `retryPending` (`:402`) e `completeOperation`/index de resend (`:476`).
  `PendingOperation`/`ReplicatedRecord` não são serializados — carregar objeto vivo
  em `localApplyPayload` é seguro.

### 2. `MapClusterService.java` — apply dual + caminho by-reference no put
- `apply` (`:327`): aceitar ambos os tipos —
  `MapReplicationCommand command = (payload instanceof MapReplicationCommand mrc) ? mrc : MapReplicationCodec.decode((byte[]) payload);`
  Resto inalterado. No líder by-reference, `command.value()` é a referência
  original → `data.put(key, originalRef)`. Em follower, chega `byte[]` → decode →
  cópia (fidelidade de tipos preservada).
- `put` (`:133`): se `leaderLocalByReference`, montar `MapReplicationCommand command = MapReplicationCommand.put(key, value)`,
  `byte[] encoded = MapReplicationCodec.encode(command)`, e chamar
  `replicate(topic, encoded /*wire*/, command /*localApply*/, null)`. Senão,
  caminho atual (`replicate(topic, encoded)`).
- Persistência (R2): no ramo PUT do `apply`, quando `leaderLocalByReference && persistence != null`,
  usar `appendSync` em vez de `appendAsync`.
- Flag `private final boolean leaderLocalByReference`; sobrecargas **aditivas** dos
  construtores usados por `createMapService` (o de 3 args e o de 5 args) recebendo
  o flag; construtores antigos delegam com `false` (não quebrar testes existentes,
  ex.: `MapClusterServiceConcurrencyTest`).

### 3. `MapConfig.java` — flag por-mapa
- Campo `boolean leaderLocalByReference` (default `false`) + getter +
  `Builder.leaderLocalByReference(boolean)`.

### 4. `NGridConfig.java` — default global
- Campo/getter `mapLeaderLocalByReference` (default `false`) +
  `Builder.mapLeaderLocalByReference(boolean)`, espelhando `mapPersistenceMode`
  (`:68`, `:300`, `:615`).

### 5. `NGridNode.java` — resolução do efetivo + guard de offsets
- `Map<String,Boolean> mapByReferenceOverrides`, populado no loop de
  `startServices` (`:403-408`, junto de `mapPersistenceOverrides`).
- `createMapService` (`:897-919`): efetivo =
  `mapByReferenceOverrides.getOrDefault(name, config.mapLeaderLocalByReference())`,
  **forçando `false` para `_ngrid-queue-offsets`** (defesa-em-profundidade; `Long`
  imutável torna by-reference irrelevante para ele de qualquer forma). Passar o
  flag ao construtor de `MapClusterService` em ambos os ramos (com e sem
  persistência).

### 6. Facade — `NGrid.java` builders
- `NGridLocalBuilder` e `NGridNodeBuilder`: overload `map(String name, boolean leaderLocalByReference)`
  (ou setter fluente) repassando para `MapConfig.builder(name).leaderLocalByReference(...)`.
  `NGridCluster` apenas repassa nomes — sem mudança.

## Semântica e limitações (documentar)

- **Identidade só no líder corrente.** Após failover, o novo líder tem cópias
  decodificadas; a identidade de referência não sobrevive à eleição (aceitável no
  active-standby colocalizado com o líder). `MapClusterService` não sobrescreve
  `onBecameLeader`, então não há re-apply.
- **Leituras by-reference.** Com o flag, `get` devolve a referência viva; mutar o
  objeto retornado por `get` altera o estado do líder **sem replicar** — é a
  semântica `ConcurrentHashMap` desejada, mas diverge de followers.
- **Sem mutação durante o `put`.** Contrato single-writer: não mutar o valor
  enquanto `put` não retorna (o encode dos bytes ocorre antes do apply local;
  mutação concorrente por outra thread divergiria líder × followers).
- **Caminho follower→líder** (`DistributedMap.putOptional` ramo follower): o líder
  recebe valor decodado e guarda a cópia como "referência" — inócuo (escritor está
  em outra JVM). O ganho só vale para `put` originado no próprio líder.

## Testes (verificação)

Harness: `InMemoryTransport` + quorum=1 (padrão de `MapClusterServiceConcurrencyTest`)
para unitários; `NGrid.local(n)` para cluster.

1. **[CHAVE] Identidade de referência (líder):** `leaderLocalByReference=true`,
   `put("k", dto)` → `assertSame(dto, map.get("k").get())`. Com `false` →
   `assertNotSame` (trava o comportamento atual).
2. **[CHAVE] Mutação in-place visível (líder):** `true`, `put` → `dto.setX(42)` sem
   novo put → `assertEquals(42, map.get("k").get().getX())`. Com `false` → não
   reflete.
3. **Apply dual / fidelidade de tipos:** `apply` com `MapReplicationCommand` vivo
   grava tipo concreto; com `byte[]` decoda corretamente (cobrir o `instanceof`).
4. **Guard de offsets:** `_ngrid-queue-offsets` permanece em bytes mesmo com default
   global `true`; semântica monotônica de `Long` intacta.
5. **Persistência + by-reference (R2):** `true` + WAL habilitado; `put` → reload →
   estado-no-put preservado (via `appendSync`).
6. **Cluster (`NGrid.local`):** líder `assertSame`; follower presente, **igual por
   conteúdo** mas `assertNotSame` (cópia decodada) — confirma que a referência não
   vaza cross-JVM. Análogo a `DistributedMapPojoReplicationTest` com o flag ligado.
7. **Config:** override por-mapa vence o default global.
8. **Facade:** `NGrid.local(n).map(name, true)`.
9. **Não-regressão:** `mvn test` + `mvn test -Presilience` (inclui
   `SequenceResendProtocolTest`, `MapNodeFailoverIntegrationTest`,
   `ConsistencyIntegrationTest`). Queue inalterado.

## Documentação

- Atualizar doc do `DistributedMap` em `doc/` (pt-BR) com a seção by-reference e as
  limitações acima.
- Adicionar diagrama de sequência PlantUML (líder by-reference × follower decode)
  e incorporar via `uml.nishisan.dev` conforme convenção.

## Sequência de commits (atômicos)

1. `feat(ngrid): infra dual-payload no ReplicationManager (no-op por default)`
2. `feat(ngrid): MapClusterService.apply aceita comando vivo ou bytes`
3. `feat(ngrid): caminho by-reference no put + appendSync na persistência`
4. `feat(ngrid): config leaderLocalByReference (MapConfig + NGridConfig global) + guard de offsets`
5. `feat(ngrid): expor by-reference na facade NGrid`
6. `test(ngrid): identidade de referência, mutação in-place, cluster e config`
7. `docs(ngrid): documenta leader-local by-reference + diagrama`

Pós-aprovação: copiar este plano para `/planning` (raiz do projeto). Branch:
`feature/ngrid-leader-local-by-reference`. Rodar `mvn clean install` (multi-módulo)
após alterar dependências entre módulos.

---

## Apêndice — RFC (separado): Eixo 2 = (C) `DistributedRef`

Registro da direção escolhida para a issue/épico futuro (não implementado aqui).

**Problema:** a mesma instância referenciada por N mapas/aninhamentos vira N cópias
divergentes e infla a replicação. `@JsonIdentityInfo` (já suportado via #110,
`MapReplicationCodecExtensionTest`) só deduplica **dentro de um documento**, nunca
entre mapas/entradas. Snapshots são **por-mapa** (`MapClusterService.java:386-448`).

**(A) interning global — rejeitado** como solução principal: não resolve o inchaço
(serializa o subgrafo inline mesmo assim) e cria ambiguidade de versão canônica
entre tópicos (sequências são por-mapa). Útil só como mitigação intra-documento.

**(C) `DistributedRef<K,V>` (escolhido):** tipo de referência que **serializa como
`{mapName, key}`** e **resolve lazy** contra o `DistributedMap` dono. Resolve
divergência **e** inchaço (fonte única de verdade = mapa dono) e compõe com o Eixo 1
(o dono guarda a instância canônica; refs resolvem para ela).

Pontos a resolver no RFC (custo real, confirmado no código):
- O codec é **estático/global** e não há `InjectableValues`/contexto de
  desserialização hoje; `NGridNode.maps` é privado. (C) exige um *resolver*
  **node-scoped** (codec/mapper por-nó ou `InjectableValues`) para a resolução lazy.
- Serializer custom de `DistributedRef` encaixa no ponto de extensão #110
  (`MapReplicationCodec.registerModule`).
- Semântica de `ref.get()` (nível de consistência do mapa dono) e ciclo de vida das
  entradas referenciadas.
- Custo no consumidor: trocar campos de referência por `DistributedRef` (foreign
  keys tipadas) — direção definitiva/para frente.
