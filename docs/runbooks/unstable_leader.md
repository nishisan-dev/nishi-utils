# Runbook: Líder Instável

## Visão Geral
Procedimento para diagnosticar e estabilizar situações onde o líder do cluster NGrid
está mudando com frequência anormal (flapping) ou apresenta lease expirada recorrente.

---

## Sintomas

| Indicador | Valor Esperado | Valor em Alerta |
|:---|:---|:---|
| Alerta `LEADER_LEASE_EXPIRED` | Ausente | Recorrente |
| `leaderEpoch` | Estável | Incrementando rapidamente |
| `hasValidLease` | `true` | Alternando `true/false` |
| Leader identity | Fixo | Mudando entre nós |
| `averageConvergenceTimeMs` | < 100ms | Elevado (> 1000ms) |

---

## Diagnóstico

### 1. Verificar padrão de eleições via dashboard

```bash
cat <data-dir>/dashboard.yaml
```

Observar se `leaderEpoch` está crescendo entre consultas consecutivas:
```yaml
cluster:
  leaderId: "node-2"
  leaderEpoch: 47    # ← muito alto para uptime curto
  hasValidLease: false
```

### 2. Analisar logs de eleição

```bash
grep -i "leader\|election\|epoch\|lease" ngrid.log | tail -50
```

Procurar padrões como:
- Eleições consecutivas em intervalos curtos
- Heartbeats não alcançando followers a tempo
- Epoch mismatch entre nós

### 3. Verificar carga no líder

```yaml
io:
  writesByNode:
    node-1: 50000    # ← alta carga pode impedir heartbeats
```

Se o throughput de writes for muito alto, o threadpool do líder pode estar saturado
e não consegue enviar heartbeats no prazo.

### 4. Verificar RTT entre nós

```yaml
io:
  rttMsByNode:
    node-2: 250.0    # ← RTT alto pode causar timeout de lease
```

Se RTT > `leaseTimeout / 3`, heartbeats podem chegar tarde demais.

---

## Ação

### Cenário A: Lease expirando por carga alta

1. **Reduzir carga de writes** temporariamente (se possível).
2. **Aumentar `leaseTimeout`**: ajustar para tolerância maior.
   - Valor padrão: 5 segundos
   - Recomendação sob carga: 10-15 segundos
3. **Verificar `heartbeatInterval`**: deve ser < `leaseTimeout / 3`.
4. Após estabilização, restaurar carga gradualmente.

### Cenário B: Leader reelection excessiva

O `LeaderReelectionService` pode estar sugerindo trocas de líder com base em write rates.

1. **Verificar `leaderReelectionCooldown`**: deve ser suficiente para evitar flapping.
2. **Desabilitar reelection temporariamente**: `leaderReelectionEnabled: false`.
3. Analisar se delta de write rate justifica reelection.

### Cenário C: RTT alto entre nós

1. Verificar latência de rede entre os nós.
2. Se possível, reduzir distância de rede (co-locate nós).
3. Se RTT é inerente (WAN), ajustar `leaseTimeout` proporcionalmente.

### Cenário D: Nó líder com problemas de hardware

1. Verificar CPU, memória e I/O do nó líder.
2. Se o nó está degradado, forçar failover matando o processo.
3. O cluster elegerá um novo líder automaticamente.

---

## Validação Pós-Estabilização

- [ ] `leaderEpoch` estável por pelo menos 5 minutos
- [ ] `hasValidLease == true` consistentemente
- [ ] `averageConvergenceTimeMs < 100ms`
- [ ] Alerta `LEADER_LEASE_EXPIRED` não dispara
- [ ] Writes completam normalmente sem timeout
- [ ] `leaderId` não mudou entre observações

---

## Métricas Relacionadas

| Métrica | Localização |
|:---|:---|
| `leaderEpoch` | `NGridOperationalSnapshot.leaderEpoch()` |
| `hasValidLease` | `NGridOperationalSnapshot.hasValidLease()` |
| `leaderId` | `NGridOperationalSnapshot.leaderId()` |
| `averageConvergenceTimeMs` | `NGridOperationalSnapshot.averageConvergenceTimeMs()` |
| RTT per-node | `NGridStatsSnapshot.rttMsByNode()` |

---

## Parâmetros de Configuração

| Parâmetro | Descrição | Default |
|:---|:---|:---|
| `heartbeatInterval` | Intervalo entre heartbeats | 1s |
| `leaseTimeout` | Tempo até lease expirar | 5s |
| `leaderReelectionEnabled` | Habilitar reelection por write rate | true |
| `leaderReelectionCooldown` | Cooldown entre sugestões | config dependent |
