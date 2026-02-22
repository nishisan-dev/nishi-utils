# Spec: ConsumerSession — Commit Explícito de Offset

## Objetivo

Separar o ato de consumir (`poll`) do ato de confirmar processamento (`commit`).
Isso habilita **at-least-once delivery**: se o consumidor falhar antes do commit, o próximo poll retorna o mesmo registro.

---

## Decisão de Design

| Aspecto | Decisão |
|---|---|
| **Modo padrão** | `AUTO_COMMIT` (compatível com `poll()` legado) |
| **Modo manual** | `MANUAL_COMMIT` — offset só avança em `commit()` |
| **Seek** | Incluir desde o início: `seekToBeginning`, `seekToEnd`, `seek(long)` |
| **Thread-safety** | `ConsumerSession` é **single-threaded** — um consumidor por instância |
| **Ciclo de vida** | `Closeable` — fechar libera registro de offsets |
| **Key/Headers** | Retornados via `ConsumedRecord` desde a Fase 1 |

---

## Interfaces e Classes

### `ConsumedRecord<T>` (record)

```java
package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.queue.NQueueHeaders;
import java.io.Serializable;

/**
 * A single record fetched by a {@link ConsumerSession}.
 *
 * @param offset    zero-based monotonic offset of this record in the queue log
 * @param key       optional routing/partitioning key, may be null or empty
 * @param headers   metadata headers attached to this record, never null
 * @param value     deserialized record value
 * @param timestamp epoch millis at which the record was produced
 * @param <T>       serializable element type
 */
public record ConsumedRecord<T extends Serializable>(
        long offset,
        byte[] key,
        NQueueHeaders headers,
        T value,
        long timestamp
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
```

### `ConsumerSession<T>` (interface pública)

```java
package dev.nishisan.utils.ngrid.queue;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Stateful consumer session over a distributed queue.
 *
 * <p>Each session tracks its own offset independently, allowing multiple
 * consumers to read from the same queue at different paces. The session
 * must be closed when no longer needed.
 *
 * <h3>Commit Modes</h3>
 * <ul>
 *   <li>{@link CommitMode#AUTO_COMMIT} – offset advances automatically after
 *       each successful {@link #poll()} call. Equivalent to legacy behavior.</li>
 *   <li>{@link CommitMode#MANUAL_COMMIT} – offset is only advanced when the
 *       caller explicitly invokes {@link #commit()} or {@link #commit(long)}.
 *       If the process crashes before commit, the same record is redelivered
 *       on the next poll (at-least-once delivery).</li>
 * </ul>
 *
 * <h3>Usage example (manual commit)</h3>
 * <pre>{@code
 * try (var session = queue.createConsumer("worker-1", CommitMode.MANUAL_COMMIT)) {
 *     Optional<ConsumedRecord<Order>> rec = session.poll(1, TimeUnit.SECONDS);
 *     rec.ifPresent(r -> {
 *         process(r.value());
 *         session.commit();     // advance offset only on success
 *     });
 * }
 * }</pre>
 */
public interface ConsumerSession<T extends Serializable> extends Closeable {

    /** Defines when the consumer offset is advanced. */
    enum CommitMode {
        /** Offset advances automatically after each poll. */
        AUTO_COMMIT,
        /** Offset advances only when {@link #commit()} is called. */
        MANUAL_COMMIT
    }

    /**
     * Fetches the next available record, returning immediately.
     *
     * @return the record, or empty if none is currently available
     */
    Optional<ConsumedRecord<T>> poll();

    /**
     * Fetches the next available record, waiting up to the given timeout.
     *
     * @param timeout maximum wait time
     * @param unit    time unit for the timeout
     * @return the record, or empty if the timeout elapsed
     */
    Optional<ConsumedRecord<T>> poll(long timeout, TimeUnit unit);

    /**
     * Commits the offset of the last record returned by {@link #poll()}.
     * <p>Only meaningful in {@link CommitMode#MANUAL_COMMIT}; a no-op in
     * {@code AUTO_COMMIT}.
     *
     * @throws IllegalStateException if no record has been fetched since the last commit
     */
    void commit();

    /**
     * Commits a specific offset, advancing the consumer position to {@code offset + 1}.
     *
     * @param offset the offset to commit
     */
    void commit(long offset);

    /**
     * Repositions the consumer to the oldest available record.
     */
    void seekToBeginning();

    /**
     * Repositions the consumer past all currently available records,
     * so the next poll will only return future records.
     */
    void seekToEnd();

    /**
     * Repositions the consumer to the given absolute offset.
     *
     * @param offset target offset
     */
    void seek(long offset);

    /**
     * Returns the last committed offset for this session.
     *
     * @return committed offset, or {@code 0} if no commit has occurred
     */
    long committedOffset();

    /**
     * Returns the consumer ID assigned to this session.
     *
     * @return consumer string identifier
     */
    String consumerId();

    /**
     * Closes this session, releasing any associated resources.
     */
    @Override
    void close();
}
```

---

## Modificações Necessárias na Camada Existente

### `QueueClusterService`

Adicionar dois métodos públicos:

```java
/**
 * Fetches a record at the given offset WITHOUT advancing the committed offset.
 * Used internally by ConsumerSession in MANUAL_COMMIT mode.
 */
Optional<T> fetchAt(NodeId consumerId, long offset);

/**
 * Explicitly commits the offset for a consumer, advancing its position.
 * Safe to call multiple times with the same offset (idempotent).
 */
void commitOffset(NodeId consumerId, long newOffset);
```

`fetchAt` é basicamente o bloco de leitura do `poll()` atual para `TIME_BASED` retention,
removendo a chamada a `safeUpdateOffset`.

### `DistributedQueue`

Adicionar factory:

```java
/**
 * Creates a new consumer session for this queue.
 *
 * @param consumerId unique identifier for this consumer
 * @param mode       auto or manual commit
 * @return a new session, must be closed by the caller
 */
public ConsumerSession<T> createConsumer(String consumerId, ConsumerSession.CommitMode mode);
```

---

## Considerações de Implementação

- `ConsumerSessionImpl` é package-private em `dev.nishisan.utils.ngrid.queue`
- Seek deve respeitar o limite inferior da retenção (não pode buscar registros já expirados)
- `commit()` sem argumento deve lançar `IllegalStateException` se `poll()` ainda não foi chamado
- Em `AUTO_COMMIT`, `commit()` e `commit(long)` são no-ops silenciosos
- `seekToEnd` = `offsetStore.getOffset().max(queue.producerIndex())`

---

## Estimativa de Esforço

| Componente | Esforço |
|---|---|
| `ConsumedRecord<T>` | Trivial — novo record |
| `ConsumerSession<T>` interface | Baixo — só interface |
| `ConsumerSessionImpl` | Médio — lógica de fetch + commit state |
| `QueueClusterService.fetchAt` + `commitOffset` | Baixo — extração de código existente |
| `DistributedQueue.createConsumer` | Baixo — factory + roteamento para líder |
| Testes | Médio — cobertura de AUTO_COMMIT, MANUAL_COMMIT, seek, crash recovery |
