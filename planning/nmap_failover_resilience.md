# NMap Failover Resilience — Plano de Implementação

Corrigir os 4 problemas identificados no `DistributedMap` durante queda do leader no NGrid: retry insuficiente, leituras STRONG falhando, falta de validação de lease, e ausência de `keySet()`.

## User Review Required

> [!IMPORTANT]
> A abordagem é **TDD**: primeiro escrever testes in-process que falham (expondo os bugs), depois implementar as correções. Não usamos Testcontainers neste nível porque os testes in-process com 3 nós JVM (padrão já estabelecido em `QueueNodeFailoverIntegrationTest`, `NGridMapPersistenceIntegrationTest`) são suficientes e muito mais rápidos.

> [!WARNING]
> O auto-downgrade de STRONG para EVENTUAL no `get()` durante failover é uma decisão de design. Se preferir que reads STRONG sejam **sempre** rejeitados sem leader (fail-fast), posso ajustar.

---

## Proposed Changes

### Testes — `MapNodeFailoverIntegrationTest`

#### [NEW] [MapNodeFailoverIntegrationTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/test/java/dev/nishisan/utils/ngrid/map/MapNodeFailoverIntegrationTest.java)

Teste in-process com cluster de 3 nós, seguindo exatamente o padrão de `QueueNodeFailoverIntegrationTest`:

**Teste 1 — `shouldRecoverMapPutAfterLeaderFailover`**
- Cluster estável (3 nós, leader = node-3 por Highest ID)
- Put 50 pares `(key-i, value-i)` via leader
- `closeQuietly(leader)` → simula crash
- Aguardar novo leader (polling com timeout)
- Validar: `get("key-0")` no novo leader retorna `value-0`

**Teste 2 — `shouldRecoverMapGetStrongAfterLeaderFailover`**
- Populate map via leader
- Kill leader
- Aguardar novo leader
- `get("key-0", Consistency.STRONG)` no follower → deve obter o valor sem `IllegalStateException`

**Teste 3 — `shouldRejectWriteOnExpiredLease`**
- Cluster com lease curto (1s) e heartbeat alto (500ms)
- Isolar leader (close dos outros 2 nós)
- Aguardar lease expirar (~2s)
- `put()` no leader isolado → deve lançar exceção (lease expirado, sem quórum)

**Teste 4 — `shouldExposeKeysAcrossCluster`**
- Put 10 chaves via leader
- Chamar `keySet()` a partir de um follower
- Validar: retorno contém as 10 chaves

---

### Correções — `DistributedMap`

#### [MODIFY] [DistributedMap.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedMap.java)

**1. Retry resiliente no `invokeLeader()`** (linhas 242-287)
- Retry com backoff exponencial: 200ms → 400ms → 800ms → ...
- Max retries: 5 (configurable)
- Total timeout: ~6s (cobre ciclo de failover realista)
- Ao receber `No leader available`, aguardar com backoff em vez de retry imediato

```diff
-    private Serializable invokeLeader(String command, Serializable body) {
-        int attempts = 0;
-        while (attempts < 3) {
-            attempts++;
+    private Serializable invokeLeader(String command, Serializable body) {
+        int maxAttempts = 5;
+        long backoffMs = 200;
+        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
             NodeInfo leaderInfo = coordinator.leaderInfo()
-                    .orElseThrow(() -> new IllegalStateException("No leader available"));
+                    .orElse(null);
+            if (leaderInfo == null) {
+                if (attempt >= maxAttempts) {
+                    throw new IllegalStateException("No leader available after " + maxAttempts + " attempts");
+                }
+                sleep(backoffMs);
+                backoffMs = Math.min(backoffMs * 2, 2000);
+                continue;
+            }
```

**2. Validação de lease nas escritas** (linhas 150-158)
```diff
     public Optional<V> put(K key, V value) {
         recordIngressWrite();
         if (coordinator.isLeader()) {
+            if (!coordinator.hasValidLease()) {
+                throw new IllegalStateException("Leader lease expired, cannot accept writes");
+            }
             recordMapPut();
             return mapService.put(key, value);
         }
```

Mesma validação para `remove()`.

**3. Adicionar `keySet()`** — método local que retorna as chaves da réplica local:
```java
public Set<K> keySet() {
    return mapService.keySet();
}
```

#### [MODIFY] [MapClusterService.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/map/MapClusterService.java)

Expor `keySet()` a partir do `ConcurrentMap` interno:
```java
public Set<K> keySet() {
    return java.util.Collections.unmodifiableSet(data.keySet());
}
```

---

## Verification Plan

### Automated Tests

**Novo teste de failover do NMap:**
```bash
mvn test -pl . -Dtest="dev.nishisan.utils.ngrid.map.MapNodeFailoverIntegrationTest" -Dngrid.test.fast=true
```

**Regressão completa do NGrid (testes in-process):**
```bash
mvn test -pl . -Dtest="**/ngrid/**Test" -Dtest.excludes="**/testcontainers/**"
```

### Manual Verification
Não necessária — os testes in-process cobrem todos os cenários de forma determinística.
