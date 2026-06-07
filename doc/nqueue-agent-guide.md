# NQueue Agent Guide

Guia canônico para agentes integrarem a `NQueue` em outros projetos sem copiar código-fonte.

## Quando usar NQueue

Use `NQueue` quando o projeto precisar de:

- fila FIFO local (single process) com persistência em disco;
- concorrência com múltiplos produtores/consumidores;
- retomada após restart com estado reconstruído;
- compactação de log para controlar crescimento de disco.

Evite `NQueue` quando a necessidade for broker distribuído nativo (Kafka/Rabbit/SQS etc.).

## Dependência

Coordenadas atuais neste repositório (`pom.xml`):

- `groupId`: `dev.nishisan`
- `artifactId`: `nishi-utils`
- `version`: `3.7.0`

Maven:

```xml
<dependency>
  <groupId>dev.nishisan</groupId>
  <artifactId>nishi-utils</artifactId>
  <version>3.7.0</version>
</dependency>
```

## API mínima que o agente deve conhecer

- `NQueue.open(baseDir, queueName)`
- `NQueue.open(baseDir, queueName, options)`
- `offer(T value)` -> `long offset`
- `poll()` / `poll(timeout, unit)` -> `Optional<T>`
- `peek()` -> `Optional<T>`
- `flushExpired()` -> `long` (descarta o prefixo expirado, retorna a quantidade)
- `size()`, `isEmpty()`, `close()`

Observação importante:

- `NQueue.OFFSET_HANDOFF` (`-2`) indica entrega direta para consumidor em espera (sem persistir no disco naquele caminho), quando short-circuit está ativo.

## Opções mais importantes (`NQueue.Options`)

Base:

```java
NQueue.Options options = NQueue.Options.defaults();
```

Controles principais:

- `withFsync(boolean)`: durabilidade mais forte vs throughput.
- `withRetentionPolicy(DELETE_ON_CONSUME|TIME_BASED)`: política de retenção.
- `withRetentionTime(Duration)`: retenção em `TIME_BASED`.
- `withExpireAfterWrite(Duration)`: expiração por tempo de escrita (ortogonal à política; `0` desabilita). Itens expirados são descartados no `poll`/`peek` ou via `flushExpired()`.
- `withCompactionWasteThreshold(double)` e `withCompactionInterval(Duration)`: compactação.
- `withMemoryBuffer(boolean)` e `withMemoryBufferSize(int)`: buffering em memória para burst.
- `withShortCircuit(boolean)`: handoff direto para consumidor bloqueado.
- `withOrderDetection(boolean)`: detector interno de violação de ordem.
- `withResetOnRestart(boolean)`: ignora `queue.meta` e reconstrói do log.

Defaults atuais relevantes:

- `withFsync = true`
- `retentionPolicy = DELETE_ON_CONSUME`
- `allowShortCircuit = true`
- `enableMemoryBuffer = false`
- `enableOrderDetection = false`

## Perfis recomendados

Confiabilidade alta:

- `withFsync(true)`
- `withShortCircuit(false)` se o fluxo exigir persistência estrita antes da entrega
- compaction conservadora

Baixa latência / alto throughput:

- `withFsync(false)` (aceitando risco de perda em crash abrupto)
- `withMemoryBuffer(true)`
- `withShortCircuit(true)`

Streaming local com replay por tempo:

- `withRetentionPolicy(TIME_BASED)`
- `withRetentionTime(Duration.ofHours(...))`

## Do / Don't para agentes

Do:

- fechar com `try-with-resources`;
- garantir que o tipo de payload é `Serializable`;
- versionar explicitamente as coordenadas da dependência;
- criar teste de integração com restart real para fluxos críticos.

Don't:

- não assumir semântica exatamente-once (do ponto de vista da app);
- não compartilhar o mesmo diretório de fila entre processos diferentes sem desenho explícito;
- não ativar `withFsync(false)` em caminhos críticos sem aceite de risco;
- não esperar que `expireAfterWrite` afete itens no `MemoryBuffer` ou em handoff (só o segmento durável expira), nem que `size()`/`getRecordCount()` disparem a varredura de expirados — chame `flushExpired()` para forçar.

## Checklist de validação em projeto consumidor

- enqueue/dequeue básico (`offer`/`poll`) passa;
- concorrência com múltiplas threads passa;
- restart mantém consistência esperada;
- retenção/compactação funciona para o perfil configurado.

## Referências internas deste repositório

- `README.md` (visão geral e exemplos)
- `doc/nqueue-readme.md` (documentação técnica da NQueue)
- `doc/nqueue-examples.md` (receitas prontas)
- `src/test/java/dev/nishisan/utils/queue/NQueueTest.java` (comportamento base)
- `src/test/java/dev/nishisan/utils/queue/NQueueCompactionTest.java` (compactação)
