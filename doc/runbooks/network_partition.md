# Runbook: Partição de Rede

## Visão Geral
Procedimento para diagnosticar e recuperar de uma partição de rede (network split)
que impede a comunicação entre nós do cluster NGrid.

---

## Sintomas

| Indicador | Valor Esperado | Valor em Alerta |
|:---|:---|:---|
| Alerta `LOW_QUORUM` | Ausente | Disparado |
| `reachableNodesCount` | == `totalNodesCount` | < maioria (N/2 + 1) |
| RTT failures | 0 | Crescente |
| `hasValidLease` (líder) | `true` | `false` |
| Writes no líder | OK | Timeout / falha de quorum |

---

## Diagnóstico

### 1. Verificar estado do cluster via dashboard

```bash
cat <data-dir>/dashboard.yaml
```

Avaliar a seção `health`:
```yaml
health:
  reachableNodesCount: 1   # ← deveria ser 3
  totalNodesCount: 3
  quorumRatio: 0.33        # ← abaixo de 0.5
```

### 2. Verificar conectividade de rede

```bash
# De cada nó, testar conexão TCP para os peers
nc -zv <peer-host> <peer-port>

# Verificar se há firewalls ou ACLs bloqueando
iptables -L -n | grep <peer-port>
```

### 3. Verificar logs do transport

Procurar por mensagens de desconexão no log do NGrid:
```
grep -i "disconnect\|unreachable\|connection refused" ngrid.log
```

### 4. Verificar se o líder perdeu a lease

No `dashboard.yaml`, confirmar:
```yaml
cluster:
  isLeader: true
  hasValidLease: false  # ← lease expirou
```

---

## Ação

### Cenário A: Partição parcial (1 nó isolado de 3)

1. **Identificar o nó isolado**: verificar qual nó tem `reachableNodesCount < maioria`.
2. **Restaurar conectividade**: corrigir firewall, rota ou interface de rede.
3. **Aguardar reconexão automática**: o transport do NGrid tenta reconectar no intervalo configurado (`reconnectInterval`).
4. **Monitorar convergência**: verificar se `replicationLag` volta a 0 e `reachableNodesCount == totalNodesCount`.

### Cenário B: Partição total (split-brain)

1. **Identificar partições**: cada lado pode eleger líderes diferentes se tiver maioria.
2. Se nenhum lado tem maioria, writes ficam bloqueados (comportamento correto com `strictConsistency=true`).
3. **Restaurar rede**: a prioridade é reestabelecer comunicação.
4. **Após restauração**: o líder de maior epoch será aceito. Followers de epoch menor farão fencing e sincronizam via resend ou snapshot.

### Cenário C: Partição intermitente (flapping)

1. Verificar taxa de RTT failures: se muito alta, a rede está instável.
2. Considerar aumentar `leaseTimeout` para tolerar jitter.
3. Verificar hardware de rede (cabo, switch, NIC).

---

## Validação Pós-Recuperação

- [ ] `reachableNodesCount == totalNodesCount` no dashboard
- [ ] `replicationLag == 0` em todos os followers
- [ ] `hasValidLease == true` no líder
- [ ] Alerta `LOW_QUORUM` não está mais ativo
- [ ] Novas operações de write/read completam sem timeout
- [ ] `gapsDetected` estabilizou (delta == 0)

---

## Métricas Relacionadas

| Métrica | Localização |
|:---|:---|
| `reachableNodesCount` | `NGridOperationalSnapshot.reachableNodesCount()` |
| `totalNodesCount` | `NGridOperationalSnapshot.totalNodesCount()` |
| `hasValidLease` | `NGridOperationalSnapshot.hasValidLease()` |
| `replicationLag` | `NGridOperationalSnapshot.replicationLag()` |
| RTT per-node | `NGridStatsSnapshot.rttMsByNode()` |
| RTT failures | `NGridStatsSnapshot.rttFailuresByNode()` |

---

## Alertas Automáticos

| Alerta | Condição |
|:---|:---|
| `LOW_QUORUM` | `reachableNodesCount < totalNodesCount / 2 + 1` |
| `LEADER_LEASE_EXPIRED` | Líder com `hasValidLease == false` |
