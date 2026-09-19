# Checkpoint — NGRRD com Kafka local em dev/teste

Data: 19/09/2026. Marcador Git: `checkpoint/ngrrd-kafka-local-20260919`.
Checkpoint correspondente no repositório TEMS:
`ngrrd-consumer/deployment/cluster-bench/CHECKPOINT-20260919.md`.

## Alterações registradas

- `WriteDispatcher`: a barreira ACK percorre rotas pendentes e acorda com sinais
  de conclusão/erro; falhas anteriores não são apagadas por escritas posteriores.
  O shutdown continua verificando admissões em andamento.
- `TransportRetry`: reconhece desconexões e IOException na cadeia de causas,
  sem percorrer ciclos de exceções indefinidamente.
- `SeriesHandleRegistry`: ordenação LRU captura os horários antes de comparar,
  preservando a transitividade sob acessos concorrentes; fechamento revalida a
  entrada sob lock.
- `GeometryReconciler`: reabertura com geometria igual lê apenas o cabeçalho nos
  backends com canal, evitando ler todo o histórico. Migração continua usando
  a imagem completa.
- `NgrrdWriter`: falha assíncrona deixa o handle em erro e é propagada a novas
  escritas/checkpoints, evitando confirmar um prefixo com falha anterior.
- Testes de barreira, retry, LRU, leitura de geometria e falha de escrita.

## Estado do ensaio

Consumer .217 e storages .217/.218 consomem o Kafka local .219 após limpeza
expressamente autorizada. Carga contínua; timers do Codex pausados e apenas
proteções locais preservadas. Não houve implantação ou reinício para criar
este checkpoint. Não usar PIDs de relatórios antigos: consultar o manifesto
atual `.217:/app/ngrrd-cluster/run.json` antes de operar.

O último binário registrado nos três processos é o JAR do consumer SHA-256
`19a2047736723f5535d53734ace30c03dd3816ef6afd48c2b9b712e45cbeda7f`.
O checkpoint TEMS também contém a melhoria local da linha `Synth` no log,
ainda não implantada. Os commits não representam uma nova versão instalada.

Às 14:00, a consulta pontual mediu 6.193,88 registros confirmados/s em 1 min,
5.522,36/s em 5 min e 4.389,31/s em 15 min, com 348.370 handles ao final.
Janelas sobrepostas e parcialmente em aquecimento; não isolam causa de rede.
ACK precede commit Kafka, mas não implica FSYNC de toda a cauda.

## Validação

Os resultados dos testes locais deste checkpoint estão em
`CHECKPOINT-20260919-validation.json`. Os testes de cluster/integração dos
ensaios anteriores e a evidência operacional estão documentados no TEMS.
O checkpoint não autoriza nova intervenção nos hosts.
