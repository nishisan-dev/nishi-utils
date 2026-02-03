# NGrid Playbook de Resiliencia

Este playbook descreve como validar resiliencia do NGrid em cenarios reais: falhas de nos, particoes, catch-up por snapshot, gaps de sequencia e offsets de consumo. O foco e operacional: o que medir, como executar e o que esperar.

## Objetivos

- Confirmar que o cluster se recupera de falhas sem perder consistencia.
- Validar catch-up por snapshot para filas e mapas.
- Validar reenvio de sequencias quando ha gaps curtos.
- Verificar comportamento de quorum em modos AP (default) e CP (strict).
- Garantir que offsets em log mode (TIME_BASED) avancam corretamente por NodeId.

## Pre-requisitos

- Java 21
- Maven
- Portas livres para executar nos locais
- Diretorio de dados gravavel por no

## Topologia de referencia

Cluster de 3 nos (seed, producer, consumer):

- seed: lider inicial
- producer: escreve em filas/mapas
- consumer: le eventos e simula lag

Use IDs estaveis:

- seed-node
- producer-node
- consumer-node

## Setup rapido (programatico)

```java
NodeInfo seed = new NodeInfo(NodeId.of("seed-node"), "127.0.0.1", 9101);
NodeInfo producer = new NodeInfo(NodeId.of("producer-node"), "127.0.0.1", 9102);
NodeInfo consumer = new NodeInfo(NodeId.of("consumer-node"), "127.0.0.1", 9103);

QueueConfig orders = QueueConfig.builder("orders").build();

NGridNode seedNode = new NGridNode(NGridConfig.builder(seed)
    .addPeer(producer).addPeer(consumer)
    .dataDirectory(Path.of("/tmp/ngrid/seed"))
    .addQueue(orders)
    .replicationFactor(2)
    .build());

NGridNode producerNode = new NGridNode(NGridConfig.builder(producer)
    .addPeer(seed)
    .dataDirectory(Path.of("/tmp/ngrid/producer"))
    .addQueue(orders)
    .replicationFactor(2)
    .build());

NGridNode consumerNode = new NGridNode(NGridConfig.builder(consumer)
    .addPeer(seed)
    .dataDirectory(Path.of("/tmp/ngrid/consumer"))
    .addQueue(orders)
    .replicationFactor(2)
    .build());
```

## Observabilidade minima

- Lider atual: `node.coordinator().leaderInfo()`
- Membros ativos: `node.coordinator().activeMembers()`
- Watermark do lider: `coordinator.getTrackedLeaderHighWatermark()`
- Ultima sequencia aplicada: `replicationManager.getLastAppliedSequence()`

> Dica: logue esses valores a cada 1s durante os testes.

## Cenarios de resiliencia

### 1) Failover de lider durante escrita

**Objetivo:** verificar se o cluster troca de lider e se o produtor recupera a operacao (ou falha de forma clara).

**Passos**

1. Inicie o cluster com 3 nos e determine o lider.
2. Dispare um loop de `offer` (ex.: 500 mensagens).
3. Mate o processo do lider no meio do loop.
4. Aguarde reeleicao e continue escrevendo.

**Esperado**

- O produtor pode observar falhas temporarias ("Not the leader" ou timeout), mas volta a escrever apos eleicao.
- O numero final de itens deve refletir o total de `offer` confirmados.

**Referencias**

- `src/test/java/dev/nishisan/utils/ngrid/QueueNodeFailoverIntegrationTest.java`
- `src/test/java/dev/nishisan/utils/ngrid/LeaderReelectionIntegrationTest.java`

---

### 2) Particao de rede (AP vs CP)

**Objetivo:** validar diferenca entre `strictConsistency(false)` e `strictConsistency(true)`.

**Passos**

1. Configure o cluster em AP (default).
2. Simule a desconexao entre producer e seed (ou pare um no).
3. Tente escrever no producer.
4. Repita o teste com `strictConsistency(true)`.

**Esperado**

- AP: quorum efetivo se ajusta e as escritas continuam se houver membros alcancaveis.
- CP: escritas falham se nao houver quorum fixo.

---

### 3) Catch-up por snapshot (fila)

**Objetivo:** validar sincronizacao de follower atrasado via snapshot em chunks.

**Passos**

1. Inicie o lider sozinho.
2. Enfileire mais de 500 operacoes em uma fila (ex.: 1200).
3. Inicie o follower com a mesma fila.
4. Aguarde `lastAppliedSequence` atingir o watermark do lider.

**Esperado**

- O follower dispara sync e instala snapshot em chunks.
- `lastAppliedSequence` atinge o valor esperado.
- `peek()` no follower retorna os dados do leader.

**Referencias**

- `src/test/java/dev/nishisan/utils/ngrid/QueueCatchUpIntegrationTest.java`

---

### 4) Catch-up por snapshot (mapa)

**Objetivo:** validar sincronizacao de mapa quando o follower esta muito atrasado.

**Passos**

1. Inicie o lider e insira muitas chaves no mapa.
2. Inicie o follower depois do limite de lag.
3. Aguarde a instalacao de snapshot.

**Esperado**

- O follower reconstrui o mapa localmente.

**Referencias**

- `src/test/java/dev/nishisan/utils/ngrid/CatchUpIntegrationTest.java`

---

### 5) Gap curto e reenvio de sequencia

**Objetivo:** confirmar que gaps curtos sao resolvidos sem snapshot.

**Passos**

1. Gere trafego com latencia simulada entre follower e lider.
2. Force um gap curto (ex.: drop de 1 ou 2 mensagens).
3. Observe log do follower solicitando reenvio.

**Esperado**

- O follower solicita `SEQUENCE_RESEND_REQUEST`.
- O lider responde e o follower aplica as operacoes faltantes.
- `nextExpectedSequence` avanca sem snapshot.

---

### 6) Offset e retention (log mode)

**Objetivo:** validar comportamento de offsets quando o retention expira.

**Passos**

1. Configure fila com `TIME_BASED` e retention curto (ex.: 10s).
2. Escreva mensagens e aguarde expirar.
3. Consumidor tenta ler com offset atrasado.

**Esperado**

- Offset eh ajustado para o item mais antigo disponivel.
- O consumidor nao trava em `readRecordAtIndex`.

---

### 7) Multi-queue isolacao de sequencias

**Objetivo:** verificar que filas nao bloqueiam umas as outras.

**Passos**

1. Configure 3 filas: orders, events, logs.
2. Escreva em todas simultaneamente.
3. Verifique que as filas progridem independentemente.

**Esperado**

- Nenhuma fila fica parada aguardando sequencias globais.

**Referencias**

- `src/test/java/dev/nishisan/utils/ngrid/MultiQueueClusterIntegrationTest.java`

---

## Checklist de aceite (rapido)

- [ ] Lider troca sem travar o cluster.
- [ ] AP escreve com quorum ajustado; CP falha corretamente.
- [ ] Catch-up de fila e mapa funciona com lag alto.
- [ ] Gaps curtos resolvidos via reenvio de sequencia.
- [ ] Offsets em TIME_BASED sao persistentes e ajustados no retention.
- [ ] Multi-queue nao apresenta bloqueio logico.

## Notas operacionais

- Use IDs estaveis por no para garantir offsets consistentes.
- Em producao, configure `minClusterSize` para evitar split-brain.
- Logue sequencias e watermark do lider durante testes.

## Scripts e testes

Se preferir automatizar, use os scripts em `doc/ngrid/playbook-automation/`:

```bash
bash doc/ngrid/playbook-automation/run-ci-resilience.sh
```

Esse script:

- executa o conjunto minimo de testes de resiliencia
- grava logs em `doc/ngrid/playbook-automation/logs/`
- usa `java.util.logging` com configuracao dedicada

Para rodar manualmente, use os testes de integracao:

```bash
mvn test -Dtest=QueueCatchUpIntegrationTest
mvn test -Dtest=CatchUpIntegrationTest
mvn test -Dtest=QueueNodeFailoverIntegrationTest
mvn test -Dtest=LeaderReelectionIntegrationTest
```

---

## Troubleshooting

### Duplicação de mensagens após failover

**Sintoma:** Após failover, consumidores recebem mensagens que já haviam consumido anteriormente.

**Causas identificadas:**

1. **Duplicação no buffer de replicação (corrigido)**
   - Quando uma mensagem chega com sequência futura (ex: seq=31 quando esperando seq=21), ela vai para o buffer.
   - O problema: se a mesma mensagem chegar novamente (via retransmissão TCP ou retry do líder), ela era adicionada novamente ao buffer.
   - **Correção:** Verificar se `opId` já existe no buffer antes de adicionar (em `ReplicationManager.handleReplicationRequest`).

2. **RetentionPolicy não configurada como TIME_BASED**
   - O offset-based consumption só funciona se `RetentionPolicy == TIME_BASED`.
   - Se a fila usa `DELETE_ON_CONSUME` (padrão), o `consumerId` é ignorado e poll é destrutivo.
   - Após snapshot install, todos os itens são restaurados e reconsumeidos.
   - **Diagnóstico:** Log adicionado em `QueueClusterService.poll()`:
     ```
     poll: consumerId=client-1, retentionPolicy=TIME_BASED, queueName=global-events
     ```
   - **Correção:** Configurar fila com `policy: TIME_BASED` no YAML e verificar se a config está sendo aplicada.

3. **Snapshot restaura itens já consumidos**
   - Em modo destrutivo, o snapshot do líder pode conter itens que já foram consumidos pelos followers.
   - **Correção:** Usar offset-based consumption (TIME_BASED) para pular itens já consumidos.

4. **Volatilidade do DistributedOffsetStore para Clientes**
   - Se os offsets de consumidores (clientes) estiverem armazenados apenas no `DistributedOffsetStore` e o cluster perder dados (ex: restart de seed sem persistência em disco garantida para o mapa), o offset reseta para 0.
   - **Sintoma:** Cliente reinicia o consumo do zero após falha do cluster, gerando duplicatas massivas.
   - **Correção (Implementada):** Mecanismo de "Offset Hint". O cliente mantém um cache local do offset e o envia para o líder a cada `poll`. O líder usa `max(stored, hint)`.
   - **Verificação:** Buscar logs "Forwarding offset for {client} from {stored} to hint {hint}".


- [ ] Verificar se logs mostram `retentionPolicy=TIME_BASED` no poll
- [ ] Verificar se `consumerId` não é `null` no poll
- [ ] Verificar se `Buffered future sequence` aparece apenas 1x por opId (dedup funcionando)
- [ ] Verificar se `Applied replication` aparece apenas 1x por nó por opId

### Logs úteis para debugging

```bash
# Contar duplicações de replicação por nó
grep -E "\[client-1\].*Replication request" /tmp/ngrid-scenario.log | sort | uniq -c | sort -rn | head -20

# Verificar se buffer está deduplicando
grep "Buffered future sequence" /tmp/ngrid-scenario.log | wc -l

# Verificar aplicações duplicadas
grep -E "\[client-1\].*Applied replication" /tmp/ngrid-scenario.log | sort | uniq -c | sort -rn | head -10

# Verificar RetentionPolicy em uso
grep "poll: consumerId" /tmp/ngrid-scenario.log | head -5
```
