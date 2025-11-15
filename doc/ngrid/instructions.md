# NGrid – Instruções Gerais do Projeto

## Objetivo

Implementar uma biblioteca Java chamada **NGrid** que fornece uma infraestrutura distribuída com:

1. **Fila distribuída** com operações `offer`, `poll` e `peek`, construída em cima de uma fila local já existente chamada `NQueue`.
2. **Mapa distribuído** com operações `put`, `get` e `remove`.

A biblioteca deve operar em um cluster de nós interconectados via TCP, com:

- Descoberta e formação progressiva de malha entre nós (tendendo a full mesh).
- Eleição de líder.
- Replicação com quorum configurável.
- Roteamento interno entre nós (qualquer nó pode encaminhar requisições ao líder).

A implementação deve ser 100% em Java.

---

## Pontos Importantes

1. **Reaproveitar a classe existente `NQueue<T extends Serializable>`** para a parte de fila local (persistência em filesystem).
2. Não alterar a semântica principal de `NQueue`, apenas utilizá-la como componente de armazenamento local em cada nó.
3. O foco deste projeto é:
    - Clusterização (conexão entre nós).
    - Coordenação (líder, quorum, roteamento).
    - Exposição de APIs de **fila distribuída** e **mapa distribuído**.

---

## Estruturas Distribuídas

### 1. Fila distribuída

- APIs desejadas (na camada distribuída):
    - `offer(T item)`
    - `poll()`
    - `peek()`
- Essas operações devem ser coordenadas via líder e replicadas no cluster, usando a `NQueue` local em cada nó como backend.

### 2. Mapa distribuído

- APIs desejadas (na camada distribuída):
    - `put(K key, V value)`
    - `get(K key)`
    - `remove(K key)`
- O mapa deve ser replicado entre os nós (inicialmente, pode ser full replication).

---

## Comportamento Esperado

- **Cluster**:
    - Cada nó é configurado com uma lista de peers iniciais.
    - A partir dessa configuração parcial, o sistema deve tentar formar uma malha entre todos os nós (descoberta progressiva).
    - Após a formação básica do cluster, deve existir um processo de eleição de líder.

- **Líder**:
    - Responsável por coordenar operações que exigem consistência e ordenação (fila e mapa).
    - Recebe operações de qualquer nó (diretamente ou via roteamento interno).
    - Realiza replicação nos demais nós antes de confirmar sucesso ao cliente, respeitando um quorum configurável.

- **Quorum e ID de operação**:
    - Cada operação distribuída deve ter um **ID único** (por exemplo, fornecido pela camada de alto nível).
    - O líder deve usar esse ID para **deduplicação** e controle de estado (`PENDING`, `COMMITTED`, etc.).
    - O líder só deve retornar sucesso ao cliente após receber confirmações suficientes para o quorum configurado.

- **Roteamento interno**:
    - Qualquer nó do cluster pode receber uma chamada de cliente (por exemplo, `offer`).
    - Se o nó for líder, processa diretamente.
    - Se não for líder, deve encaminhar a operação ao líder via malha interna e depois devolver a resposta ao cliente.

---

## Arquivos de Design Complementares

Este arquivo (`instructions.md`) é a visão geral.  
Mais detalhes de arquitetura, camadas e integração com `NQueue` estão descritos em:

- `architecture.md`
- `nqueue-integration.md`
