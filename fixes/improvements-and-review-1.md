# Revisão de código - nishi-utils (NGrid)

## Problemas encontrados

1. **Operações de replicação podem ficar bloqueadas para sempre**: `MapClusterService` invoca `future.join()` para cada operação de escrita, mas o `ReplicationManager` não possui qualquer timeout ou tratamento em `onPeerDisconnected`, apesar do comentário indicar o contrário. Se o líder perder quorum (ex.: follower cai após ser considerado ativo), o futuro nunca é concluído e a thread do cliente fica bloqueada indefinidamente. É necessário adicionar timeouts ou cancelamento/rollback quando o quorum se torna inalcançável.
2. **Futuro pendente vazado em chamadas request/response**: `TcpTransport.sendAndAwait` registra a `pendingResponses` e completa o futuro apenas quando a resposta chega ou na chamada explícita a `close()`. Se o peer desconectar sem responder, esse futuro nunca é completado/cancelado, podendo acumular memória e bloquear chamadas dependentes. O `handleDisconnect` também não limpa pendências associadas ao peer desconectado.

## Melhorias recomendadas

- Adicionar timeouts configuráveis e retry/backoff nas operações de replicação para evitar blocos indefinidos e dar feedback aos chamadores quando o quorum não puder ser atingido.
- Limpar e completar excepcionalmente `pendingResponses` quando uma conexão é encerrada ou quando um timeout expirar, evitando vazamentos e chamadas que nunca retornam.
- Considerar uma política de reconexão/backoff exponencial e limite de conexões simultâneas para evitar tempestades de threads no `TcpTransport`, dado o uso de `newCachedThreadPool` e um writer dedicado por conexão.
- Propagar eventos de desconexão para reavaliar quorum/liderança imediatamente, reduzindo janelas onde a aplicação acredita ter quorum enquanto peers já caíram.
