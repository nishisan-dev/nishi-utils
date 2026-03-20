# Elevar Maturidade do NMap Distribuído + Atualizar Documentação

O NMap distribuído (`DistributedMap` + `MapClusterService`) evoluiu significativamente desde o baseline de 07/02/2026, mas a documentação ficou para trás e a API pública tem operações faltantes comuns em mapas (`containsKey`, `size`, `isEmpty`, `putAll`). Este plano cobre atualização de docs, expansão de API e testes de resiliência específicos para Map.

## Proposed Changes

### Bloco 1 — Atualização de Documentação (6 arquivos)

> [!IMPORTANT]
> A documentação atual passa a impressão de um sistema menos maduro do que realmente é. Vários gaps apontados já foram resolvidos.

---

#### [MODIFY] [map-design.md](file:///home/lucas/Projects/nishisan/nishi-utils/doc/ngrid/map-design.md)

Atualizar com estado real do código. Adicionar:
- **Consistency Levels**: `STRONG`, `EVENTUAL`, `BOUNDED` — já implementados em `DistributedMap.get(key, consistency)`
- **Leader Lease**: `DistributedMap.put()` e `remove()` verificam `hasValidLease()` — writes rejeitados se lease expirou
- **Retry com Backoff**: `invokeLeader()` usa 5 attempts com backoff exponencial (200ms→2s)
- **`keySet()`**: exposto na API pública (leitura local, eventually-consistent)
- **`removeByPrefix()`**: operação local-only para snapshot install
- **Monotonic Offsets**: `apply()` usa `max(stored, new)` para topic `_ngrid-queue-offsets`
- **Snapshot/Catch-up**: `getSnapshotChunk()` implementado com paginação de 1000 itens/chunk
- **Health Check**: `isHealthy()` e `persistenceFailureCount()` + integração com `NGridAlertEngine`
- Remover seção "Limitações" que já não se aplicam (leitura apenas no líder → agora há 3 níveis)

---

#### [MODIFY] [RESILIENCE_RECOMMENDATIONS.md](file:///home/lucas/Projects/nishisan/nishi-utils/RESILIENCE_RECOMMENDATIONS.md)

Atualizar status de cada lacuna:
- "Catch-up de fila ausente" → ✅ Implementado (snapshot chunked)
- "Reenvio de sequência ausente" → ✅ Implementado (`SEQUENCE_RESEND_REQUEST/RESPONSE`)
- "Semântica de consumo inconsistente" → 🟡 Decisão pendente (manter nota)
- "Cobertura de testes insuficiente" → 🟡 Parcial (failover testado, partição longa sem cobertura)

---

#### [MODIFY] [REPLICATION_DEADLOCK_BUG.md](file:///home/lucas/Projects/nishisan/nishi-utils/REPLICATION_DEADLOCK_BUG.md)

Atualizar status de "🟡 Fix aplicado" para "✅ Resolvido — estável desde 2026-01-18". Atualizar timeline e data de última atualização.

---

#### [MODIFY] [improvements-and-review-1.md](file:///home/lucas/Projects/nishisan/nishi-utils/fixes/improvements-and-review-1.md)

Atualizar cada item com status de resolução:
1. "Operações bloqueadas para sempre" → ✅ Timeout configurável + `QuorumUnreachableException`
2. "Futuro pendente vazado" → ✅ `handleDisconnect` limpa pendências

---

#### [MODIFY] [go-live-checklist.md](file:///home/lucas/Projects/nishisan/nishi-utils/docs/go-live-checklist.md)

Marcar como ☑ os critérios que já são atendidos por testes verdes:
- F1, F2, F5, F6, F7, F9, F10, F11, F12, F13, F15, F16 (baseado nos 211 testes verdes)
- F3, F4, F14 (depende de `LeaderLeaseIntegrationTest` e `LeaderLeaseStepDownTest` — verificar se cobrem)
- O5, O6, O7 (testes de alert, dashboard, snapshot existem e passam)

---

#### [MODIFY] [ngrid-baseline.md](file:///home/lucas/Projects/nishisan/nishi-utils/doc/planning/ngrid-baseline.md)

Adicionar seção de atualização (20/03/2026) com status dos gaps:
- G1 → ✅ Leader lease implementado
- G3 → ✅ HealthCheck + AlertEngine
- G4 → ✅ Leader lease step-down
- G2, G5, G6, G7 → status atualizado

---

### Bloco 2 — Expansão da API DistributedMap (2 arquivos)

#### [MODIFY] [DistributedMap.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedMap.java)

Adicionar 4 operações que leem do estado local (sem replicação):
- `containsKey(K key)` → delega para `mapService.get(key).isPresent()` (leitura local)
- `size()` → novo método em `MapClusterService` que retorna `data.size()`
- `isEmpty()` → delega para `size() == 0`
- `putAll(Map<K,V> entries)` → itera e chama `put(k, v)` para cada entrada (replicado)

#### [MODIFY] [MapClusterService.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/map/MapClusterService.java)

Adicionar:
- `int size()` → retorna `data.size()`

---

#### [NEW] [DistributedMapApiTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/test/java/dev/nishisan/utils/ngrid/map/DistributedMapApiTest.java)

Teste de integração em cluster de 3 nós validando:
- `containsKey()` retorna `true` após `put()` e `false` após `remove()`
- `size()` reflete operações em follower (eventually-consistent)
- `isEmpty()` é `true` em mapa novo, `false` após `put()`
- `putAll()` replica todas as entradas para followers

---

### Bloco 3 — Testes de Resiliência de Map (1 arquivo)

#### [NEW] [MapPartitionResilienceTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/test/java/dev/nishisan/utils/ngrid/map/MapPartitionResilienceTest.java)

Dois cenários:

1. **Partição longa com divergência**: Isolar líder, escrever no líder isolado (deve falhar por lease expirado), validar que followers elegem novo líder e aceitam writes, validar que o líder antigo reconecta e converge
2. **Writes concorrentes durante failover**: 3 threads fazendo `put()` enquanto líder é derrubado, validar que após reeleição os dados convergem sem perda

---

## Verification Plan

### Automated Tests

```bash
# Executar todos os testes do módulo principal
mvn test -pl . -q

# Executar apenas os testes de Map em cluster
mvn test -pl . -Dtest="MapNodeFailoverIntegrationTest,NGridMapPersistenceIntegrationTest,MapClusterServiceConcurrencyTest,DistributedMapApiTest,MapPartitionResilienceTest"

# Executar apenas os testes novos
mvn test -pl . -Dtest="DistributedMapApiTest,MapPartitionResilienceTest"
```

### Manual Verification

- Revisar cada documento atualizado para confirmar que reflete o estado real do código
- Validar que os diagramas mermaid em `map-design.md` renderizam corretamente
