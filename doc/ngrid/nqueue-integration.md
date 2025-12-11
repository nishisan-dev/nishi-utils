# NGrid – Integração com NQueue

## Contexto

Já existe uma implementação de fila local persistente em filesystem chamada:

- `NQueue<T extends Serializable>` (implementa `Closeable`).

Essa classe:

- Já cuida da **persistência local** da fila em disco.
- Já oferece a semântica de fila local necessária.
- **Não deve ser reimplementada**, apenas reutilizada.

---

## Objetivo da Integração

Na parte de **fila distribuída** da NGrid:

- Cada nó do cluster deve ter uma instância local de `NQueue`.
- A fila distribuída deve usar essa `NQueue` como backend para armazenamento local das mensagens.

Em outras palavras:

- A lógica distribuída (cluster, líder, quorum, replicação) fica por conta da NGrid.
- A lógica de armazenamento local em disco já está resolvida pela `NQueue`.

---

## Regras de Uso da NQueue

1. **Não alterar a API pública existente de `NQueue`.**
2. **Não duplicar a lógica de persistência de fila que já existe.**
3. Utilizar `NQueue` apenas como:
    - Fila de armazenamento local (por nó),
    - Onde os itens já foram aprovados pela lógica de líder/quorum.

---

## Papel da NQueue em Cada Nó

Para cada nó do cluster:

- Haverá uma instância de `NQueue<T>` responsável por:
    - Guardar localmente os itens da fila distribuída que pertencem a aquele nó.
    - Garantir que, após uma replicação bem-sucedida, o item esteja salvo em disco.

A lógica de **ordenação global** e **decisão de entrega** deve vir do líder, não da NQueue.

---

## Fluxo de Operações com Integração

### `offer(item)`

1. Cliente chama `offer` em qualquer nó.
2. O nó:
    - Se for líder:
        - Registra a operação (com ID único) na camada de replicação.
        - Aplica a operação na sua `NQueue` local (enqueue).
        - Propaga a operação aos outros nós.
        - Cada nó replica:
            - Aplicando a mesma operação na sua `NQueue` local.
        - Quando o líder recebe confirmações suficientes (quorum), ele:
            - Marca a operação como `COMMITTED`.
            - Retorna sucesso ao chamador.
    - Se não for líder:
        - Encaminha a operação ao líder via camada de cluster.
        - Aguarda o resultado.
        - Retorna o resultado ao chamador.

### `poll()`

1. Cliente chama `poll` em qualquer nó.
2. O nó delega para o líder (diretamente ou via roteamento).
3. O líder:
    - Decide qual item é o próximo a ser entregue.
    - Pode ler esse item da sua `NQueue` local.
    - Opcionalmente, marcar o item como “in-flight” em alguma estrutura de controle.
    - Devolve o item para o chamador.
4. A remoção definitiva do item da `NQueue` pode:
    - Ser imediata, ou
    - Acontecer após algum tipo de confirmação adicional, dependendo da política de consumo (isso deve ser definido pela lógica de alto nível, não pela NQueue).

### `peek()`

1. Cliente chama `peek`.
2. Operação é encaminhada ao líder.
3. O líder inspeciona o próximo item da `NQueue` local sem removê-lo.
4. Retorna o item ao chamador.

---

## Persistência e Recuperação

- Em caso de reinício de um nó:
    - A `NQueue` local deve ser reaberta.
    - Os itens previamente persistidos em disco devem ser carregados.
- A camada de cluster/coordenação:
    - Deve sincronizar com o líder atual,
    - Para garantir que o estado da fila (inclusive mensagens entregues, pendentes e confirmadas) continue consistente com as outras réplicas.

Detalhes como:

- Como diferenciar itens apenas enfileirados de itens já consumidos,
- Como lidar com mensagens “in-flight” no momento da parada,

podem ser gerenciados por metadados adicionais na camada distribuída (acima da NQueue), sem alterar a implementação interna da NQueue.

---

## Resumo da Integração

- `NQueue` é o **backend local de fila em disco** em cada nó.
- A NGrid adiciona:
    - Cluster,
    - Líder,
    - Quorum,
    - Replicação,
    - Deduplicação por ID,
    - APIs de fila distribuída.
- A lógica distribuída **envolve/coordena** o uso da `NQueue`, mas não substitui a implementação local existente.
