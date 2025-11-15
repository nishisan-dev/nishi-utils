# **map-design.md**

## **NGrid – Design do Mapa Distribuído**

O mapa distribuído do NGrid tem como objetivo fornecer uma estrutura compartilhada entre múltiplos nodes, com consistência garantida por meio de liderança, quorum e replicação. Todas as operações mutáveis (`put`, `remove`) devem passar pelo líder. Operações de leitura (`get`) seguem a política definida abaixo.

---

## **Objetivos do Mapa Distribuído**

- Manter um conjunto chave→valor replicado entre todos os nós ou entre um subconjunto configurável.
- Garantir consistência após falhas, trocas de líder ou isolamentos de rede.
- Evitar resultados duplicados ou inconsistentes utilizando IDs únicos de operação.
- Suportar as operações principais:
    - `put(K key, V value)`
    - `get(K key)`
    - `remove(K key)`

---

## **Modelo de Consistência**

### **Nível Básico: Consistência Forte no Líder**
O líder é o único responsável por:

- Determinar a ordem global das operações.
- Validar operações duplicadas via ID.
- Aplicar mudanças no seu mapa local.
- Replicar a mudança aos demais nós.
- Confirmar a operação ao cliente após atingir quorum.

Essa abordagem garante consistência semelhante ao modelo "single-writer, multi-reader".

### **Quorum**
- Para operações mutáveis (`put`, `remove`), a confirmação depende de:
    - Líder + N−1 confirmações (ou conforme configuração).
- Operações de leitura (`get`):
    - Podem ser atendidas pelo líder localmente (modelo simples inicial).
    - Opcionalmente podem ser atendidas por qualquer nó quando houver mecanismo de sincronização estável.

---

## **Estrutura Local de Armazenamento**

Cada nó mantém um mapa local em memória, com possibilidade de futura persistência.  
Elementos armazenados:

- Chave (`K`)
- Valor (`V`)
- Metadados opcionais, como:
    - Versão local
    - Tombstones (remoção lógica)
    - Timestamp lógico

Não há necessidade de modificar ou acoplar à `NQueue`, pois o mapa é independente.

---

## **Operações**

### **PUT**
Fluxo:

1. O nó que recebe a requisição encaminha ao líder (se não for líder).
2. O líder:
    - Gera/valida ID único.
    - Aplica a operação no mapa local (atualiza ou insere).
    - Replica a operação para os outros nós.
3. Cada nó replica aplicando a mesma operação localmente.
4. Ao atingir quorum, o líder confirma ao chamador.

Propriedades:

- Overwrite de valores anteriores.
- Deduplicação garantida via ID único.
- Ordem global definida pelo líder.

---

### **GET**
Fluxo simples inicial:

1. O nó recebe a requisição.
2. Encaminha para o líder.
3. O líder retorna diretamente o valor associado à chave.

Modo avançado opcional (para versão futura):

- Qualquer nó pode servir GET desde que:
    - Se mantenha sincronizado com o líder,
    - E haja registro de versão/timestamp lógico para validação.

---

### **REMOVE**
Operação equivalente ao PUT, porém aplicando um **tombstone**:

1. O nó encaminha ao líder.
2. O líder valida ID, aplica tombstone local.
3. Replica tombstone para os demais nós.
4. Após quorum, confirma remoção ao chamador.

Motivo para tombstone:

- Evita inconsistências em caso de mensagens atrasadas.
- Garante que réplicas atrasadas não reinstalem valores removidos.

---

## **Formato Interno de Operação**

Cada operação distribuída carrega:

- ID único da operação
- Tipo (`PUT`, `REMOVE`)
- Chave (`K`)
- Valor (`V`) ou tombstone
- Versão ou timestamp (opcional)
- Data/hora lógica da operação (opcional)
- Identificador do líder que decidiu a ordem

Esses metadados servem para:

- Deduplicação
- Replay após troca de líder
- Reconstrução de estado no cluster

---

## **Recuperação e Falhas**

### **Troca de Líder**
O novo líder precisa:

- Sincronizar o conjunto de operações commitadas.
- Adotar o estado de mapa mais atualizado.
- Garantir que operações antigas não sejam reexecutadas.

Isso pode ser feito com:

- Log replicado das operações (caso exista).
- Snapshots do mapa.
- Troca de estado inicial entre líder e seguidores.

### **Nós Atrasados**
Nós que estiverem offline durante parte da replicação devem:

- Sincronizar estado ao reconectar,
- Ser trazidos para a versão atual do mapa,
- Aplicar tombstones e put pendentes conforme necessário.

---

## **Simplicidade Inicial**
Na primeira versão, recomenda-se:

- Replicação completa do mapa entre todos os nós.
- GET atendido pelo líder.
- Tombstones simples para remoção.
- Sem persistência obrigatória.
- Reconstrução inteira pelo líder ao retomar o cluster.

Após isso, evoluções possíveis:

- Persistência local em disco,
- GET local com validação de versão,
- Particionamento / sharding de mapas,
- Cache inteligente,
- Replicação incremental.

---

## **Resumo Geral**

O mapa distribuído do NGrid deve:

- Ser simples na primeira fase,
- Usar líder + quorum para consistência,
- Replicar operações PUT e REMOVE,
- Guardar estados e versões para troca de líder,
- Suportar tombstones para evitar reinserção indevida,
- Atender GET inicialmente via líder para reduzir complexidade,
- Evoluir gradualmente para um modelo mais eficiente.
