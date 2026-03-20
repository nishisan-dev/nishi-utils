# Revisão de código - nishi-utils (NGrid)

> **Última atualização:** 2026-03-20

## Problemas encontrados e status de resolução

### ✅ 1. Operações de replicação bloqueadas indefinidamente
**Problema original:** `MapClusterService` invocava `future.join()` sem timeout, bloqueando a thread do cliente se o líder perdesse quorum.

**Resolução:** 
- `MapClusterService.waitForReplication()` agora usa `future.get(timeoutMs, TimeUnit.MILLISECONDS)` com timeout configurável via `ReplicationManager.operationTimeout()`.
- `QuorumUnreachableException` é lançada quando quorum é inalcançável.
- Quorum dinâmico adapta em `onPeerDisconnected()` quando `strictConsistency=false`.
- Leader lease impede writes em líder isolado.

### ✅ 2. Futuro pendente vazado em chamadas request/response
**Problema original:** `TcpTransport.sendAndAwait` não completava futuros quando peer desconectava, acumulando memória.

**Resolução:**
- `handleDisconnect` agora completa excepcionalmente todos os `pendingResponses` associados ao peer desconectado.
- Request timeout configurável garante que futuros são limpos mesmo em cenários de falha silenciosa.

## Melhorias recomendadas — Status

- ~~Adicionar timeouts configuráveis e retry/backoff~~ → ✅ Implementado
- ~~Limpar pendingResponses quando conexão encerra~~ → ✅ Implementado
- ~~Propagar eventos de desconexão para reavaliar quorum/liderança~~ → ✅ Implementado (`onPeerDisconnected` em `ReplicationManager`)
- Otimizar pool de threads no `TcpTransport` → 🟡 Sem urgência (Virtual Threads considerar futuramente)
