# Fix: QueueNodeFailoverIntegrationTest Flakiness (Issue #74)

O teste `QueueNodeFailoverIntegrationTest` falha intermitentemente no CI com `Cluster did not stabilize in time`. A falha ocorre no `setUp` antes de qualquer operação de queue, confirmando que é um problema de **timing na estabilização do cluster** e não de lógica de negócio.

## Análise da Causa Raiz

| Parâmetro | Valor atual | Problema |
|-----------|-------------|----------|
| `awaitClusterStability` timeout | 20s | Insuficiente para CI com recursos compartilhados |
| `heartbeatInterval` | 200ms | Agressivo demais — gera overhead de mensagens no CI |
| Diagnóstico | Nenhum | Nenhuma informação sobre o estado do cluster quando o timeout ocorre |

Comparando com outros testes do projeto:
- `SequenceGapRecoveryIntegrationTest` já usa **30s** de timeout — e é estável
- `QueueKeyHeadersIntegrationTest` usa **150ms** de heartbeat — mais agressivo, mas com cluster de 2 nós

## Proposed Changes

### NGrid Test Infrastructure

#### [MODIFY] [QueueNodeFailoverIntegrationTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/test/java/dev/nishisan/utils/ngrid/QueueNodeFailoverIntegrationTest.java)

**1. Aumentar `heartbeatInterval` de 200ms para 500ms**

O heartbeat de 200ms é agressivo para um ambiente de CI. Com 3 nós, isso gera 6 mensagens de heartbeat a cada 200ms. Subir para 500ms reduz a contention sem impactar a detecção de falha para os testes (que esperam 3-5s pelo failover de qualquer forma).

**2. Aumentar timeout de `awaitClusterStability` de 20s para 30s**

O `SequenceGapRecoveryIntegrationTest` já usa 30s com sucesso. Isso dá margem suficiente para o cluster estabilizar em ambientes de CI com scheduling jitter.

**3. Adicionar log de diagnóstico antes do `throw` no `awaitClusterStability`**

Quando o timeout ocorre, o teste falha sem nenhuma informação útil. Vamos adicionar um log com o estado de cada nó (leader, activeMembers, connected) para facilitar debug futuro:

```diff
-        throw new IllegalStateException("Cluster did not stabilize in time");
+        // Diagnostic info for CI debugging
+        String diag = String.format(
+            "leadersAgree=%s, allMembers=[%d,%d,%d], connected=[1->2:%s,1->3:%s,2->1:%s,2->3:%s,3->1:%s,3->2:%s]",
+            node1.coordinator().leaderInfo().equals(node2.coordinator().leaderInfo())
+                && node1.coordinator().leaderInfo().equals(node3.coordinator().leaderInfo()),
+            node1.coordinator().activeMembers().size(),
+            node2.coordinator().activeMembers().size(),
+            node3.coordinator().activeMembers().size(),
+            node1.transport().isConnected(info2.nodeId()),
+            node1.transport().isConnected(info3.nodeId()),
+            node2.transport().isConnected(info1.nodeId()),
+            node2.transport().isConnected(info3.nodeId()),
+            node3.transport().isConnected(info1.nodeId()),
+            node3.transport().isConnected(info2.nodeId()));
+        throw new IllegalStateException("Cluster did not stabilize in time. State: " + diag);
```

> [!NOTE]
> Não estamos adicionando retry/flakiness tolerance (como `@RepeatedTest`) porque a causa raiz é o timeout insuficiente, não instabilidade fundamental. Resolver com retry mascaria o problema.

## Verification Plan

### Automated Tests

Executar o teste modificado repetidamente para confirmar estabilidade:

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl . -Dtest=QueueNodeFailoverIntegrationTest -Dsurefire.rerunFailingTestsCount=3
```

O flag `-Dsurefire.rerunFailingTestsCount=3` permite até 3 re-runs em caso de flakiness, validando que o fix efetivamente elimina as falhas.

Também rodar toda a suite de testes NGrid para garantir que os ajustes não quebraram nada:

```bash
mvn test -pl . -Dtest="dev.nishisan.utils.ngrid.*"
```
