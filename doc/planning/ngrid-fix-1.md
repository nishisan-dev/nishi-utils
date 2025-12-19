# Plano de Correção e Melhoria - NGrid (Fix 1)

**Data:** 18/12/2025
**Status:** Planejado
**Objetivo:** Mitigar riscos de perda de dados, inconsistência de réplicas e falhas silenciosas identificados na revisão de código.

---

## 1. Prioridade Alta: Integridade e Consistência de Dados

### 1.1. Correção do Falso Positivo na Replicação (ReplicationManager)
**O Problema:** O `ReplicationManager` marca uma operação como "aplicada" (`applied.add`) *antes* da execução real. Se a execução falhar, uma retentativa do líder encontrará o ID marcado como aplicado e receberá um ACK falso, corrompendo o estado do cluster.
**Plano de Correção:**
*   **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java`
*   **Ação:**
    1. Remover a chamada `applied.add(payload.operationId())` do bloco síncrono de recebimento da mensagem.
    2. Implementar um mecanismo de rastreamento de operações em andamento (`processing`) para evitar submissão duplicada durante a execução.
    3. Mover a lógica de marcação (`applied.add`) e envio de ACK para o callback de sucesso da thread de execução (`executor`).
    4. Em caso de falha na execução, garantir que o ID não seja marcado, permitindo que o mecanismo de *retry* do líder funcione corretamente.

### 1.2. Correção na Recuperação de Persistência do Mapa (MapPersistence)
**O Problema:** Durante a rotação de logs, se o sistema falhar após renomear `wal.log` para `wal.log.old` mas antes de completar o snapshot, o arquivo `.old` é ignorado na reinicialização, causando perda de dados.
**Plano de Correção:**
*   **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/map/MapPersistence.java`
*   **Ação:**
    1. Alterar o método `load()`.
    2. Adicionar verificação explícita pela existência de `wal.log.old`.
    3. Se `wal.log.old` existir, ele deve ser processado/reproduzido **antes** do `wal.log` atual, garantindo que operações pendentes antes do crash sejam recuperadas.

### 1.3. Prevenção de Inconsistência na Fila (QueueClusterService)
**O Problema:** Ao detectar divergência entre o valor esperado e o valor real na cabeça da fila (`POLL`), o sistema apenas loga um aviso e consome o item errado, agravando a dessincronização.
**Plano de Correção:**
*   **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java`
*   **Ação:**
    1. No método `applyReplication`, caso `POLL`:
    2. Se o valor local diferir do valor esperado (comando de replicação), lançar uma `IllegalStateException` (ou uma exceção específica de inconsistência).
    3. Isso impedirá o consumo do item errado e sinalizará ao `ReplicationManager` que a operação falhou nesta réplica.

---

## 2. Prioridade Média: Robustez e Configuração

### 2.1. Robustez de Tarefas Agendadas (ClusterCoordinator e outros)
**O Problema:** Exceções não tratadas (`RuntimeException`) dentro de tarefas agendadas (`ScheduledExecutorService`) interrompem silenciosamente a execução periódica, matando heartbeats e monitores.
**Plano de Correção:**
*   **Arquivos:**
    *   `src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java`
    *   `src/main/java/dev/nishisan/utils/ngrid/metrics/RttMonitor.java`
    *   `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java` (tarefas de timeout/retry)
*   **Ação:**
    1. Envolver o corpo das tarefas executadas periodicamente (ex: `sendHeartbeat`, `evictDeadMembers`, `probePeers`) em blocos `try-catch (Throwable t)`.
    2. Logar o erro com nível `SEVERE` ou `WARNING`.
    3. Garantir que a exceção não suba para o `ScheduledExecutorService`.

### 2.2. Mitigação de Split-Brain (Configuração de Consistência)
**O Problema:** O cálculo de quórum dinâmico baseado em membros ativos favorece disponibilidade (AP) mas permite *split-brain*.
**Plano de Correção:**
*   **Arquivos:**
    *   `src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java`
    *   `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationConfig.java`
    *   `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java`
*   **Ação:**
    1. Adicionar uma flag de configuração `strictConsistency` (padrão `false` para manter compatibilidade, ou a critério do usuário).
    2. No `ReplicationManager`, alterar o cálculo de `requiredQuorum`:
        *   Se `strictConsistency == true`: O quórum é calculado com base no número total de **peers configurados** (conhecidos estaticamente ou via discovery inicial), ignorando quantos estão ativos no momento.
        *   Se `strictConsistency == false`: Mantém o comportamento atual (baseado em `activeMembers`).

---

## 3. Resumo da Execução

A execução seguirá a ordem de prioridade acima:
1.  Correção Crítica do `ReplicationManager` (evitar corrupção imediata).
2.  Correção da Persistência (evitar perda de dados em restart).
3.  Correção da Fila (parar propagação de erro).
4.  Blindagem das Threads (estabilidade).
5.  Melhoria do Quórum (arquitetura).
