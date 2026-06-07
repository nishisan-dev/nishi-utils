# Plano: `expireAfterWrite` na NQueue

## Context

A `NQueue` (FIFO persistente em disco, módulo `nishi-utils-core`, pacote
`dev.nishisan.utils.queue`) hoje só remove itens por consumo
(`DELETE_ON_CONSUME`) ou por idade em modo stream (`TIME_BASED`, leitura não
destrutiva). Faltava um mecanismo de **expiração por tempo de escrita**: descartar
itens que envelheceram além de um limite, independentemente de serem consumidos,
no modo fila normal.

O objetivo foi adicionar um `expireAfterWrite` configurável por duração, com
descarte **oportunista** (validado no `poll`/`peek` — item expirado é descartado
sem ser entregue) e um método manual `flushExpired()` para forçar a varredura.

### Por que é barato e seguro implementar

- Cada registro **já grava** `timestamp` (epoch millis, `System.currentTimeMillis()`
  no `offer` — `NQueue.java:950`), acessível por `meta.getTimestamp()`
  (`NQueueRecordMetaData.java:290`). **Nenhuma mudança no formato binário (V3) é
  necessária.**
- Como writes são FIFO e o timestamp é monotônico, **itens expirados formam
  sempre um prefixo contíguo na cabeça** da fila. Expirar = avançar o
  `consumerOffset` sobre esse prefixo.
- Já existe o padrão de referência: `CompactionEngine.findTimeBasedCutoff()`
  (`CompactionEngine.java:280`) varre o head lendo só o header e comparando
  `meta.getTimestamp()` com um cutoff.

### Decisões de design (confirmadas com o usuário)

1. **poll/peek pulam o prefixo expirado** e entregam o primeiro item válido; só
   retornam vazio quando a fila esvazia. `peek` pode mutar o estado descartando
   expirados.
2. **Config ortogonal** `withExpireAfterWrite(Duration)` (default 0 =
   desabilitado), convive com qualquer `RetentionPolicy`. **Não** é uma nova
   policy.
3. Método manual **`flushExpired()`** → `long` (qtde descartada).
4. Descarte (oportunista e manual) **apenas avança o cursor** (O(1) por item); o
   espaço físico é recuperado pela compaction automática já existente — **sem
   compaction síncrona forçada**.

---

## Arquivos modificados

| Arquivo | Mudança |
|---|---|
| `nishi-utils-core/.../queue/NQueue.java` | Option + builder + `copy()` + `Snapshot`; helper `skipExpiredRecordsLocked()`; chamadas em poll/peek; método público `flushExpired()` |
| `nishi-utils-core/.../queue/NQueueMetrics.java` | nova constante `EXPIRED_EVENT` |
| `nishi-utils-core/.../test/queue/NQueueExpireTest.java` | novo arquivo de testes |
| `doc/nqueue-readme.md`, `doc/nqueue-examples.md`, `doc/nqueue-agent-guide.md` | documentação |

`CompactionEngine.java` e `MemoryStager.java` **não** mudam — servem só de
referência de padrão e de justificativa de escopo.

---

## 1. Options (`NQueue.java`)

- **Campo** após `retentionTimeNanos`: `long expireAfterWriteNanos = 0; // 0 = desabilitado`
- **Builder** `withExpireAfterWrite(Duration)` validando não-nulo/não-negativo.
- **`copy()`** — OBRIGATÓRIO: `copy.expireAfterWriteNanos = this.expireAfterWriteNanos;`
- **`Snapshot`** — OBRIGATÓRIO: campo `final long expireAfterWriteNanos;` + atribuição.

Conversão para comparar com o timestamp (epoch millis):
`TimeUnit.NANOSECONDS.toMillis(expireAfterWriteNanos)`.

## 2. Mecanismo central: `skipExpiredRecordsLocked()`

Método privado, chamado **sempre sob `lock`**. Lê **só o header** em
`consumerOffset` (`readPrefix` + `fromBuffer`), nunca o payload. Avança o cursor
sobre o prefixo expirado, decrementa `recordCount`/`approximateSize`, emite
`EXPIRED_EVENT` por item. Ao final (se descartou): `recordCount == 0 ⇒
consumerOffset = producerOffset`; persiste **uma vez**; chama `maybeCompact` **uma
vez**. Retorna a quantidade descartada. Tolerante a header corrompido (`break`).

## 3. Integração em poll / peek

- `poll()` / `poll(timeout, unit)`: chamar após o drain do stager e antes do
  handoff/loop de espera. Varrer antes do loop preserva o orçamento `nanos`.
- `peek()` / `peekRecord()`: chamar no início, sob lock.

## 4. `flushExpired()` (público)

Adquire o lock e delega ao helper. Retorna a qtde descartada; `0` quando
desabilitado.

## 5. Métricas

`NQueueMetrics.EXPIRED_EVENT = "EXPIRED_EVENT"`. (Observação: `StatsUtils` usa a
string exata como chave; testes consultam o literal idêntico à constante.)

## Escopo / casos de borda

- `handoffItem` — sem expiração (entregue no instante do offer, idade ~0).
- MemoryStager / itens em memória — fora de escopo (usam `nanoTime`; passam a
  expirar ao virar duráveis).
- `readRange`/`readAt`/`readRecordAt`/`readRecordAtIndex` — não aplicam expiração.
- `size()`/`getRecordCount()` — não disparam varredura.

## Testes — `NQueueExpireTest.java`

10 casos: skip no poll; timeout com tudo expirado; peek mutando estado;
`flushExpired()` retornando contagem; desabilitado por default; FIFO dos válidos;
métrica; ortogonalidade com `TIME_BASED`; rejeição de Duration negativa.

## Documentação

- `doc/nqueue-readme.md` — subseção "Expiracao por tempo de escrita" + API.
- `doc/nqueue-examples.md` — exemplo 8.
- `doc/nqueue-agent-guide.md` — API mínima, opções e Do/Don't.

## Verificação

```bash
mvn -pl nishi-utils-core test -Dtest=NQueueExpireTest
mvn -pl nishi-utils-core test
mvn -pl nishi-utils-core verify -Pvalidate-javadoc -DskipTests
```

Resultado: nova suite 10/10; suite completa do módulo 339 testes (0 falhas/erros,
8 skipped pré-existentes); validação de Javadoc OK.

## Riscos e armadilhas

1. Esquecer `copy()`/`Snapshot` zera a config silenciosamente.
2. Persistir/`maybeCompact` só uma vez ao final do helper.
3. Varrer lendo só header, nunca `readAtInternal`/`safeDeserialize`.
4. `recordCount == 0 ⇒ consumerOffset = producerOffset`.
5. Ortogonalidade com TIME_BASED coberta por teste.
6. Helper assume lock adquirido; `flushExpired()` é o único ponto que adquire.
7. Métrica inconsultável por divergência de caixa — teste usa literal exato.
8. Não tocar handoff/memory buffer.
9. Timeout do poll(timeout): varrer antes do loop; não recalcular `nanos`.
