# Recomendações de Resiliência e Revisões Pendentes

Este documento resume o estado atual do NGrid (fila distribuída) e as ações recomendadas para garantir resiliência real em cenários de falha, partição e recuperação.

## Status Atual (resumo)
- Multi-queue funcionando com roteamento correto por fila.
- Testes atuais passam (`mvn test`).
- Multi-producer / multi-consumer ok nos cenários cobertos.

## Lacunas Críticas
1. **Catch-up de fila ausente**
   - `ReplicationHandler.getSnapshotChunk()` para filas retorna `null`.
   - Followers atrasados não conseguem sincronizar via snapshot.

2. **Reenvio de sequência ausente**
   - `checkForMissingSequences()` apenas registra o gap.
   - Não há mecanismo de reenvio efetivo.

3. **Semântica de consumo inconsistente**
   - Legacy (`queueDirectory`) usa `DELETE_ON_CONSUME`.
   - Novo (`dataDirectory`) usa `TIME_BASED` + offsets.
   - Isso muda comportamento em restart/falhas.

4. **Cobertura de testes insuficiente**
   - Não há testes de falha com partition/reconnect focados em fila.
   - Não há validação de replay/consistência após failover de líder.

## Recomendações Prioritárias
1. **Implementar Snapshot/Catch-up para filas**
   - `QueueClusterService.getSnapshotChunk()` e ingestão no follower.
   - Garantir compatibilidade com offsets e retenção.

2. **Implementar Reenvio de Sequências Faltantes**
   - Mensagens `SEQUENCE_RESEND_REQUEST/RESPONSE` (já existem no código base).
   - Quando `checkForMissingSequences()` detectar gap, acionar reenvio.

3. **Definir Semântica de Consumo (Decisão de Produto)**
   - Decidir se o comportamento padrão será `DELETE_ON_CONSUME` ou `TIME_BASED`.
   - Se `TIME_BASED`, formalizar API de consumo por offset.

4. **Adicionar Testes de Resiliência**
   - Failover de líder durante escrita.
   - Partição entre nós e reconexão.
   - Restart com filas parcialmente consumidas.

## Próximos Passos Sugeridos
1. Implementar snapshot/catch-up para filas.
2. Implementar reenvio de sequência faltante.
3. Adicionar testes de falha (failover + partition).

