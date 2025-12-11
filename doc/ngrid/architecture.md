# NGrid – Arquitetura e Camadas

## Visão em Camadas

A biblioteca NGrid deve ser organizada em camadas lógicas, por exemplo:

1. **Camada de Transporte / Cluster**
    - Responsável por:
        - Conexão TCP entre nós.
        - Troca de mensagens entre nós.
        - Descoberta e formação de malha (mesh).
    - Abstrair conexões ponto-a-ponto e permitir envio de mensagens endereçadas a nós específicos (ID de nó).

2. **Camada de Coordenação de Cluster**
    - Responsável por:
        - Gerenciar a lista de membros do cluster.
        - Eleição de líder.
        - Identificar qual nó é o líder atual.
        - Fornecer mecanismo para qualquer nó encaminhar mensagens ao líder.
    - Deve expor uma API interna para:
        - Saber se o nó atual é líder.
        - Enviar uma “operação distribuída” ao líder e receber resposta.

3. **Camada de Replicação e Quorum**
    - Responsável por:
        - Receber operações distribuídas (com ID único).
        - Replicá-las para os outros nós (ou para o conjunto de nós relevantes).
        - Aguardar confirmações até atingir o quorum configurado.
        - Registrar o estado da operação (`PENDING`, `COMMITTED`, etc.) para deduplicação.
    - Essa camada será utilizada tanto pela fila quanto pelo mapa.

4. **Camada de Estruturas Distribuídas**
    - Expor APIs de alto nível para:
        - **Fila distribuída** (`offer`, `poll`, `peek`).
        - **Mapa distribuído** (`put`, `get`, `remove`).
    - Internamente, a fila distribuída usará:
        - A camada de replicação + `NQueue` como backend local.
    - O mapa distribuído usará:
        - A camada de replicação + alguma estrutura de dados local (por exemplo, `Map` com persistência opcional, se desejado).

---

## Cluster e Descoberta

- Cada nó possui um identificador único.
- Na inicialização, o nó recebe:
    - Uma lista de peers conhecidos (host/porta, por exemplo).
- Processo:
    - O nó tenta se conectar a esses peers.
    - Ao se conectar, os nós trocam informações sobre outros nós conhecidos.
    - Com isso, a malha vai sendo fechada até se aproximar de um full mesh.

---

## Eleição de Líder

- Deve haver um mecanismo de eleição de líder.
- Requisitos:
    - Todos os nós saibam, eventualmente, qual nó é o líder atual.
    - Em caso de falha do líder:
        - Uma nova eleição é disparada.
        - Um novo líder é eleito.
    - A camada de estruturas distribuídas interage sempre com o líder para operações que precisam de consistência (via APIs expostas pela coordenação).

---

## Replicação e Estado de Operações

- Toda operação distribuída (por exemplo, `offer` na fila, ou `put` no mapa) deve:
    - Ter um **ID único**.
    - Ser registrada na camada de replicação.
- Para cada operação:
    - O líder envia a operação aos outros nós.
    - Cada nó aplica a operação na sua instância local (fila/mapa).
    - O nó responde ao líder quando a operação local for considerada bem-sucedida.
    - Quando o líder recebe confirmações de um número suficiente de nós (quorum), marca a operação como `COMMITTED`.
    - Em seguida, o líder retorna sucesso ao chamador da operação.

- Em caso de tentativas repetidas com o mesmo ID:
    - A camada de replicação deve identificar o ID e devolver o resultado consistente (ou confirmar que já foi commitado).

---

## Fila Distribuída

### Comportamento esperado (alto nível)

- `offer`:
    - Deve enviar a operação ao líder.
    - O líder adiciona o item na fila local (usando `NQueue`).
    - O líder replica essa operação para os outros nós (cada nó também grava em sua `NQueue` local).
    - Após atingir o quorum, o líder confirma o `offer`.

- `poll`:
    - Deve ser coordenado pelo líder.
    - O líder decide qual item é o próximo.
    - Marca esse item como entregue (sem removê-lo imediatamente da persistência, se houver uma lógica de “in-flight”/confirmação futura).
    - Retorna o item para o chamador.

- `peek`:
    - Permite inspecionar o próximo item sem consumi-lo.

- A definição exata de estados como “in-flight” e a política de remoção definitiva da fila pode ser refinada, mas a coordenação deve partir do líder e ser consistente no cluster.

---

## Mapa Distribuído

### Comportamento esperado (alto nível)

- Modelo simples inicial: **replicação total** (todos os nós mantêm uma cópia completa do mapa).
- `put`:
    - Enviado ao líder.
    - O líder atualiza seu mapa local (associando chave -> valor).
    - O líder replica a operação aos outros nós.
    - Após quorum, confirma ao chamador.

- `get`:
    - Pode ser servido pelo líder.
    - Opcionalmente, pode ser servido por qualquer nó se garantida sincronização adequada (o projeto pode começar atendendo `get` pelo líder para manter a consistência simples).

- `remove`:
    - Similar ao `put`, mas remove a chave do mapa (ou aplica um “tombstone” lógico, se necessário para lidar com mensagens atrasadas).

---

## Tolerância a Falhas (conceito básico)

- Em caso de falha do líder:
    - A coordenação de cluster deve eleger um novo líder.
    - O novo líder deve ser capaz de:
        - Reconstruir o estado de quais operações foram `COMMITTED`.
        - Continuar atendendo operações de fila e mapa sem quebrar a consistência.

Os detalhes de como o log ou estado de operações é mantido podem ser definidos de forma pragmática, desde que garantam deduplicação baseada em ID e consistência razoável para o contexto do projeto.
