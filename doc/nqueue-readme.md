# NQueue

## Visão geral
O `NQueue` é uma fila persistente baseada em arquivos que armazena objetos `Serializable` em disco. Cada fila possui um diretório com dois arquivos principais: `data.log`, que guarda os registros binários, e `queue.meta`, que mantém os ponteiros e contadores necessários para continuar a leitura mesmo após reinicializações.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L41-L104】

A implementação utiliza um único `ReentrantLock` e uma `Condition` para coordenar produtores e consumidores, garantindo exclusão mútua e suporte a operações bloqueantes quando a fila está vazia.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L47-L220】

## Estrutura em disco
### Arquivo `queue.meta`
O arquivo de metadados contém um cabeçalho compacto com um identificador mágico (`NQMT`), versão e quatro valores de 64 bits: `consumerOffset`, `producerOffset`, `recordCount` e `lastIndex`. A escrita ocorre de forma atômica usando um arquivo temporário e `Files.move` com `ATOMIC_MOVE`, reduzindo riscos de corrupção em quedas de energia.【F:src/main/java/dev/nishisan/utils/queue/NQueueQueueMeta.java†L29-L104】

### Arquivo `data.log`
Os registros são escritos sequencialmente. Cada registro possui um cabeçalho e um payload:

```
[MAGIC 4B][VERSION 1B][HEADER_LEN 4B]
[INDEX 8B][PAYLOAD_LEN 4B][CLASSNAME_LEN 2B][CLASSNAME N bytes]
[payload serializado]
```

O cabeçalho é serializado/deserializado pela classe `NQueueRecordMetaData`, que também informa o comprimento total do header e do payload a serem lidos.【F:src/main/java/dev/nishisan/utils/queue/NQueueRecordMetaData.java†L39-L148】

## Fluxo das operações principais
### Abertura (`open`)
Ao abrir uma fila, o código garante a criação do diretório, carrega `queue.meta` (se existir) e valida os offsets em relação ao tamanho atual de `data.log`. Quando detecta inconsistências ou quando o arquivo de metadados não existe, ele reconstrói o estado percorrendo o arquivo de dados e trunca eventuais resíduos corrompidos antes de persistir o novo metadado.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L67-L104】【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L291-L347】

### Escrita (`offer`)
Para inserir um elemento, `offer` serializa o objeto, monta o cabeçalho `NQueueRecordMetaData`, escreve cabeçalho e payload diretamente no `FileChannel`, atualiza os offsets e contadores em memória e persiste o novo estado no `queue.meta`. A condição `notEmpty` é sinalizada para liberar consumidores bloqueados.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L106-L144】

### Leitura pontual
* `readRecordAt(offset)` devolve `NQueueReadResult`, que contém o registro bruto e o próximo offset a ser lido, permitindo inspeções sem consumir a fila.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L162-L169】【F:src/main/java/dev/nishisan/utils/queue/NQueueReadResult.java†L20-L36】
* `peekRecord()` bloqueia até que haja dados e retorna o registro em `consumerOffset` sem avançar o ponteiro do consumidor.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L171-L179】

A leitura interna verifica se há bytes suficientes para cabeçalho e payload antes de montar o `NQueueRecord`, prevenindo leituras parciais.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L260-L288】【F:src/main/java/dev/nishisan/utils/queue/NQueueRecord.java†L20-L35】

### Consumo (`poll`)
`poll()` e `poll(timeout, unit)` esperam até que `recordCount` seja maior que zero (ou que o tempo se esgote). O consumo real acontece em `consumeNextRecordLocked`, que lê o registro atual, avança `consumerOffset`, decrementa `recordCount` e persiste os novos offsets. Quando a fila fica vazia, `consumerOffset` passa a coincidir com `producerOffset` para evitar releituras.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L181-L220】【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L368-L382】

## Tratamento de falhas
A rotina `rebuildState` percorre `data.log` para reconstruir os offsets, truncando o arquivo quando encontra fragmentos incompletos. Ao final, força (`force(true)`) o canal para garantir que o estado recuperado esteja sincronizado em disco antes de atualizar `queue.meta`. Essa estratégia permite recuperar a fila mesmo com metadados corrompidos.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L291-L347】

## Recomendações de uso
* Sempre feche a fila com `close()` para garantir que os canais sejam liberados e que as últimas alterações sejam persistidas.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L249-L258】
* Utilize `readRecordAt` ou `peekRecord` quando precisar inspecionar o conteúdo sem removê-lo; consuma via `poll` para respeitar a ordem FIFO.
* Evite compartilhar a mesma instância entre processos distintos — a sincronização usa `ReentrantLock`, o que pressupõe uso dentro do mesmo processo.【F:src/main/java/dev/nishisan/utils/queue/NQueue.java†L47-L220】

## Glossário rápido
* **consumerOffset** – posição em `data.log` do próximo registro a ser entregue ao consumidor.【F:src/main/java/dev/nishisan/utils/queue/NQueueQueueMeta.java†L33-L55】
* **producerOffset** – posição onde o próximo registro será escrito.【F:src/main/java/dev/nishisan/utils/queue/NQueueQueueMeta.java†L33-L55】
* **recordCount** – quantidade de registros disponíveis para consumo.【F:src/main/java/dev/nishisan/utils/queue/NQueueQueueMeta.java†L33-L55】
* **lastIndex** – índice sequencial do último registro gravado, útil para auditoria e monitoração.【F:src/main/java/dev/nishisan/utils/queue/NQueueQueueMeta.java†L33-L55】
