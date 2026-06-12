# Issue 146 — Contrato de leitura concorrente com escrita no mesmo NgrrdHandle

## Diagnóstico

O formato `.ngrr` é single-writer in-place. Dentro de um mesmo `NgrrdHandle`:

- `write()` é assíncrono (enfileira para a worker thread do `NgrrdWriter`);
  `checkpoint()/flush()` são síncronos (drenam a fila).
- `read()` (via `NgrrdReader`/`ViewExecutor`) abre um `SeriesChannel` próprio a
  cada chamada e lê header → live-state → ring em operações separadas.

### Por que 1 writer + N readers NÃO era seguro (localDisk)

1. **Torn read físico do live-state** — o reader pode ler a região do
   live-state enquanto o checkpoint a regrava. O CRC32 detecta e o `read()`
   degrada para série vazia (leitura espúria intermitente, não silenciosa).
2. **Inconsistência lógica live-state ↔ ring (silenciosa)** — o writer grava
   células do ring imediatamente quando um passo fecha (`emit` em
   `advanceColumn`), mas só persistia `curRow`/`curRowEpochSec` no checkpoint.
   Um reader concorrente interpretava células novas com o anchor antigo →
   pontos com timestamps deslocados, sem nenhum erro detectável.
3. Serializar `read()` × `write()` no caller não basta sozinho: `write()` é
   assíncrono — só a sequência `writes → checkpoint() → reads` (sem writes
   concorrentes ao read) era segura, e era isso que o TEMS assumia.

No **S3** já era seguro por construção: o canal do reader é um GET (snapshot
imutável em memória) e o writer só publica via PUT atômico no checkpoint.

## Decisão (opção 1 da issue)

Garantir na lib o contrato **1 writer lógico + N readers no mesmo handle** e
documentar — permitindo ao ngrrd-consumer relaxar a serialização por série e
ganhar paralelismo de leitura na série quente.

## Implementação

1. **Lock compartilhado por handle** (`ReentrantReadWriteLock`, criado em
   `Ngrrd.fromYaml` e injetado em `NgrrdWriter`, `NgrrdReader` e
   `ViewExecutor`):
   - worker thread do writer adquire o write-lock em volta de qualquer mutação
     do canal (`handleWrite` que toca o ring, parte mutativa do
     `checkpointAndForce`); `force()` (fsync/PUT) fica **fora** do lock;
   - readers adquirem o read-lock em volta da sequência header → live-state →
     ring (canal aberto fora do lock; no S3 o GET não bloqueia o writer);
   - N readers correm em paralelo entre si; só excluem o writer.
2. **Coerência lógica live-state ↔ ring**: o writer regrava a região do
   live-state sempre que o ring avança (flag interna marcada em `writeCell`),
   sem `force()` — durabilidade continua sendo só no checkpoint; o par
   (anchor, células) persistido fica sempre coerente sob o mesmo write-lock.
3. **Construtores existentes preservados**: uso standalone de
   `NgrrdWriter`/`NgrrdReader`/`ViewExecutor` cria lock próprio (sem efeito).
4. **Teste de stress** `NgrrdConcurrentReadWriteTest`: GAUGE passthrough com
   `value == tsSec` (qualquer deslocamento de timestamp quebra a invariante
   `p.value() == p.tsEpochMs()/1000`), 1 writer com checkpoints intercalados ×
   N readers contínuos sobre a janela do ring inteiro.
5. **Documentação**: javadoc de contrato no `NgrrdHandle` + seção de
   concorrência em `doc/oss/ngrrd.md`.

## Fora de escopo

- Leitura por um segundo handle/processo (cross-process) continua sem
  garantia no localDisk — o invariante "um writer por série" permanece.
- Crash-consistency página-a-página entre checkpoints (pré-existente, não
  afeta leitura em runtime).
