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

### Cenário E: Dual-leader (dois nós se afirmando líderes)

Assinatura nos logs: `Dual-leader detected with <node>` e/ou `Leader epoch converged to N (leader
re-stamp above observed N-1)` recorrente nos dois nós (issue tems#9, D10).

**A resolução é automática**: o heartbeat carrega a flag de líder e, após 3 heartbeats consecutivos
de observação mútua, o nó de **menor afinidade** cede de forma determinística (prioridade, depois
NodeId), ressinca do vencedor via snapshot e **descarta a cauda produzida na janela dual** (estado
não-confiável por definição). Logs esperados do desfecho: `Dual-leader resolved: yielding leadership
to higher-affinity <node>` no perdedor e `retaining (higher affinity)` no vencedor.

1. **Não intervir** durante a resolução (segundos); confirmar líder único na sequência.
2. Se o dual ocorreu durante um handoff por afinidade sob produção contínua, habilitar/ajustar o
   **quiesce-assisted reclaim** (`leaderPauseOnReclaim` + `reclaimQuiesceThreshold`) — ele elimina a
   causa-raiz (delta in-flight eterno) e evita o descarte da janela dual.
3. Verificar a saúde do **join-quiesce** no rejoin (`Join-quiesce released` deve acontecer por
   catch-up real ou timeout, nunca em ~1s sob lag grande).
4. A contenção manual antiga (SIGTERM no nó de menor afinidade + wipe + cold bootstrap) permanece
   válida apenas para versões sem o fix do D10.

### Cenário F: Volta do líder por afinidade com `affinityHandbackMode` (D11)

Com o handback orquestrado ligado (default no Cardinal), a volta do líder preferido NÃO usa os gates
de watermark — segue um handover stop-the-world. Assinatura saudável nos logs:
`Affinity handback: requesting orchestrated handover` (candidato) → `PREP for candidate` /
`granting handover to ... at W=` (incumbente) → `GRANT received ... installing full snapshot` →
`cutover complete ... asserting leadership` → `completed cutover ... demoting` (incumbente). O offset
de linhagem (`counter-scale desync symptom`) **desaparece** após o handback (cutover `SET` zera).

1. **Não intervir** durante o handover (segundos a poucos minutos para snapshot multi-GB; bounded por
   `handoverMaxDuration`). O consumo do Kafka pausa no incumbente e retoma no novo líder após o
   drain-gate; o backlog drena depois (lag, não perda — o Kafka bufferiza).
2. Se aparecer `handover ... aborted`/`exceeded max duration`: o incumbente **retém** a liderança e
   retoma o consumo (seguro). Investigar por que o candidato não completou (transfer travado, snapshot
   muito grande vs `handoverMaxDuration`, candidato instável). O backstop de dual-leader (D10c)
   continua valendo para qualquer aborto sujo.
3. Para desligar o handback e voltar ao caminho legado (D10b): `affinityHandbackMode=false` (a config
   reativa `leaderPauseOnReclaim`).

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
