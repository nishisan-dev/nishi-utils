# Matriz de Gap — NQUEUE e NMAP no NGrid

## NQUEUE

| Capacidade | Estado atual | Evidência | Alvo v1 | Gap/decisão |
|---|---|---|---|---|
| Log ordenado por fila | Implementado | `NQueue`, `QueueClusterService`, `NQueueTest` | Manter | Sem gap funcional |
| Retenção temporal | Implementada no caminho moderno | `QueueConfig`, `NGridConfigLoader`, `doc/ngrid/configuracao.md` | Padrão do produto | Canonizado como caminho principal |
| Key + headers | Implementado | `OfferPayload`, `QueueKeyHeadersIntegrationTest` | Manter | Sem gap funcional |
| Replay por offset | Implementado | `QueueClusterService`, `QueueRestartConsistencyTest` | Manter | Sem gap funcional |
| Consumer lógico explícito | Parcial | Antes: acoplado a `NodeId` | `openConsumer(groupId, consumerId)` | Implementado nesta rodada |
| Seek/replay explícito | Ausente na API pública | Offset store existia, mas sem cursor explícito | Disponível no consumer lógico | Implementado nesta rodada |
| Catch-up snapshot/reenvio | Implementado | `QueueCatchUpIntegrationTest`, `SequenceGapRecoveryIntegrationTest` | Manter | Sem gap funcional |
| Work queue destrutiva | Legado | `DELETE_ON_CONSUME` | Compatibilidade apenas | Fora do roadmap principal |
| Partições Kafka | Ausente | N/A | Fora do v1 | Não implementar agora |

## NMAP

| Capacidade | Estado atual | Evidência | Alvo v1 | Gap/decisão |
|---|---|---|---|---|
| KV distribuído básico | Implementado | `DistributedMap`, `DistributedMapApiTest` | Manter | Sem gap funcional |
| Persistência WAL + snapshot | Implementada quando habilitada | `NMapPersistence`, `NMapTest`, `NGridMapPersistenceIntegrationTest` | Manter | Sem gap funcional |
| Leituras com semântica explícita | Implementadas | `Consistency`, `ConsistencyIntegrationTest` | Manter e documentar | Contrato passa a ser parte formal do produto |
| Failover com convergência | Parcialmente coberto | `MapPartitionResilienceTest`, `MapNodeFailoverIntegrationTest` | Endurecer | Continuar investindo em ITs reais |
| Health/readiness de persistência | Implementado no core | `MapClusterService.isHealthy()`, `persistenceFailureCount()` | Expor como critério operacional | Cobertura de runtime ainda pode crescer |
| API ampla estilo Hazelcast | Ausente | N/A | Fora do v1 | Não ampliar agora |

## Gate mínimo de maturidade
- `QueueCatchUpIntegrationTest`
- `QueueNodeFailoverIntegrationTest`
- `QueueRestartConsistencyTest`
- `LeaderLeaseStepDownTest`
- `NGridQueueConcurrentFailoverIT`
- `NGridMapConcurrentWriteFailoverIT`
- `NGridPartitionResilienceIT`
