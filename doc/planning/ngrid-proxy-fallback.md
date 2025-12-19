# Plano de Implementação: Proxy Fallback com Roteamento "Sticky"

**Objetivo:** Permitir comunicação entre nós que não possuem visibilidade direta de rede (ex: atrás de NAT/Firewall) usando nós intermediários como proxies transparentes.

**Estratégia:** "Sticky Fallback". Se a conexão direta falhar, usa-se o proxy indefinidamente. A volta para a conexão direta ocorre apenas via sondagem em background (Opportunistic Probe).

---

## 1. Arquitetura de Roteamento

### 1.1. Classe `NetworkRouter` (Nova)
Componente responsável por decidir o "próximo salto" (next hop) para um destino.
*   **Estrutura de Dados:** `Map<NodeId, RouteInfo>`
    *   `RouteInfo`: Enum `Strategy` (DIRECT, PROXY), `NodeId via` (se proxy), `Instant lastDirectFailure`.
*   **Responsabilidade:**
    *   Manter estado das rotas.
    *   Executar a lógica de "Probe" para tentar restaurar rotas diretas.

### 1.2. Alterações no `TcpTransport`
*   **Envio (`send`):**
    *   Antes de buscar a conexão, consulta o `NetworkRouter`.
    *   Se `DIRECT`: Tenta conexão direta. Se falhar, notifica o Router (que rebaixa para PROXY) e tenta reenviar via proxy imediatamente.
    *   Se `PROXY`: Busca a conexão do nó intermediário (`via`) e envia a mensagem encapsulada ou roteada.
*   **Recebimento (`handleMessage`):**
    *   Verificar `message.destination()`.
    *   Se `destination == localNode`: Processa normalmente.
    *   Se `destination != localNode`:
        *   Decrementa `TTL` (adicionar campo ou usar header customizado). Se zero, descarta.
        *   Chama `send(message)` para repassar (Forwarding).

---

## 2. Lógica de "Stickiness" e Recuperação

### 2.1. O Processo de Falha (Fallback)
1.  `TcpTransport` tenta conectar em `Target`.
2.  `IOException` ou `Timeout` ocorre.
3.  Transport marca `Target` como falho no `Router`.
4.  `Router` busca nos metadados de gossip (`PeerUpdatePayload`) quem conhece `Target`.
5.  `Router` elege um vizinho (ex: o com menor latência RTT conhecida ou aleatório) como Proxy.
6.  Rota atualizada para `PROXY (via Neighbor)`.

### 2.2. O Processo de Sonda (Opportunistic Probe)
*   **Task Agendada (ex: 30s ou 1min):**
*   Para cada rota `PROXY`:
    1.  Tenta abrir um `Socket` TCP para o `Target` (timeout curto).
    2.  Se Sucesso: Fecha socket, atualiza rota para `DIRECT`, loga "Route promoted to DIRECT".
    3.  Se Falha: Mantém como `PROXY`, atualiza timestamp da última tentativa.

---

## 3. Alterações no Protocolo (`ClusterMessage`)
*   **Hop Limit (TTL):** Adicionar um campo `byte ttl` (padrão 3 ou 5) no `ClusterMessage` para evitar loops de roteamento infinitos em topologias complexas.

## 4. Plano de Execução
1.  **Refatoração do ClusterMessage:** Adicionar TTL.
2.  **Criar `NetworkRouter`:** Lógica de tabela e seleção de rota.
3.  **Atualizar `TcpTransport`:** Integrar router no envio e implementar lógica de relay no recebimento.
4.  **Testes:**
    *   Teste de Relay: Nó A -> B -> C.
    *   Teste de Fallback: Derrubar link direto A->C e verificar tráfego passando por B.
    *   Teste de Recovery: Restaurar link A->C e verificar promoção de rota.
