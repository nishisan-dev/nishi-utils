# Recomendações de Resiliência e Revisões Pendentes

> **Última atualização:** 2026-03-20

Este documento resume o estado atual do NGrid (fila e mapa distribuídos) e as ações recomendadas para garantir resiliência real em cenários de falha, partição e recuperação.

## Status Atual (resumo)
- Multi-queue funcionando com roteamento correto por fila.
- Multi-producer / multi-consumer ok nos cenários cobertos.
- Leader lease implementado — líder isolado faz step-down.
- Epoch fencing protege contra split-brain write.
- 211 testes passando (0 falhas, 0 erros).

## Lacunas — Status Atualizado

### ✅ Resolvido: Catch-up de fila
- `QueueClusterService.getSnapshotChunk()` implementado com paginação (1000 itens/chunk).
- `MapClusterService.getSnapshotChunk()` também implementado.
- Followers atrasados sincronizam via snapshot chunked quando lag > 500 ops.
- **Testes:** `QueueCatchUpIntegrationTest`, `CatchUpIntegrationTest` ✅

### ✅ Resolvido: Reenvio de sequência
- `SEQUENCE_RESEND_REQUEST/RESPONSE` implementado no `ReplicationManager`.
- `checkForMissingSequences()` agora aciona reenvio pontual antes de fallback para snapshot completo.
- **Testes:** `SequenceResendProtocolTest`, `SequenceGapRecoveryIntegrationTest` ✅

### 🟡 Pendente: Semântica de consumo inconsistente
- Legacy (`queueDirectory`) usa `DELETE_ON_CONSUME`.
- Novo (`dataDirectory`) usa `TIME_BASED` + offsets.
- **Ação:** Decisão de produto sobre qual será o default.

### 🟡 Parcial: Cobertura de testes de resiliência
- ✅ Failover de líder durante escrita: `QueueNodeFailoverIntegrationTest`, `MapNodeFailoverIntegrationTest`
- ✅ Replay/consistência após failover: `NGRID_REPLAY_ANALYSIS.md` — resolvido com Hybrid Offset Sync
- ❌ Partição longa com divergência de dados: sem cobertura automatizada
- ❌ Writes concorrentes durante failover no Map: sem cobertura automatizada

## Recomendações Prioritárias Atualizadas

1. ~~Implementar Snapshot/Catch-up para filas~~ → ✅ Implementado
2. ~~Implementar Reenvio de Sequências Faltantes~~ → ✅ Implementado
3. **Definir Semântica de Consumo (Decisão de Produto)** — `DELETE_ON_CONSUME` vs `TIME_BASED` como default
4. **Adicionar Testes de Partição Longa** — split-brain write com divergência e reconexão
5. **Adicionar Testes de Map sob Failover Concorrente** — writes simultâneos durante troca de líder
