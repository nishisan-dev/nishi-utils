# Runbook: Recuperação Pós-Crash

## Visão Geral
Procedimento para recuperar um nó NGrid após um crash inesperado (kill -9, OOM,
falha de hardware, etc.) garantindo integridade de dados e reintegração ao cluster.

---

## Sintomas

| Indicador | Valor Esperado | Valor Observado |
|:---|:---|:---|
| Processo NGrid | Rodando | Down |
| Alerta `PERSISTENCE_FAILURE` | Ausente | Possível (antes do crash) |
| Nó na lista de `activeMembers` | Presente | Faltando |
| `reachableNodesCount` dos peers | N | N-1 |

---

## Procedimento de Recuperação

### Fase 1 — Avaliação

1. **Verificar causa do crash**:
   ```bash
   # Últimas linhas do log
   tail -100 ngrid.log

   # Verificar OOM killer
   dmesg | grep -i "oom\|killed"

   # Verificar espaço em disco
   df -h <data-dir>
   ```

2. **Verificar integridade dos dados persistidos**:
   ```bash
   # Verificar WAL
   ls -la <data-dir>/replication/wal/

   # Verificar epoch
   cat <data-dir>/replication/epoch.txt

   # Verificar sequence state
   cat <data-dir>/replication/sequence_state.txt
   ```

3. **Avaliar se o cluster manteve quorum** durante o crash:
   - Se quorum foi mantido: writes continuaram, nó será sincronizado.
   - Se quorum foi perdido: writes foram bloqueados, sem divergência.

### Fase 2 — Restart

1. **Subir o nó normalmente**:
   ```bash
   # O NGrid recupera estado do WAL automaticamente no startup
   java -jar ngrid-app.jar --config=config.yaml
   ```

2. **Observar logs de startup** para confirmar:
   - Carga do epoch e sequence state do disco
   - Conexão ao cluster via transport
   - Registro como membro

3. **O nó se reintegrará como follower**:
   - Receberá entradas faltantes via replication resend.
   - Se o gap for muito grande, fará snapshot fallback (full sync).

### Fase 3 — Monitoramento Pós-Restart

```bash
# Verificar dashboard a cada 10 segundos
watch -n 10 cat <data-dir>/dashboard.yaml
```

Confirmar evolução:
```yaml
replication:
  replicationLag: 5000    # ← diminuindo progressivamente
  gapsDetected: 12        # ← estável (sem novos gaps)
  resendSuccessCount: 10  # ← resends bem-sucedidos
```

---

## Ação por Cenário

### Crash durante write (WAL corruption possível)

1. O WAL possui checksums e truncamento seguro.
2. Se detectada corrupção no startup, o nó rejeitará entradas corrompidas.
3. Entradas perdidas serão resincronizadas via replication.

### Crash após partição de rede

1. Seguir primeiro o [Runbook de Partição de Rede](network_partition.md).
2. Resolver a partição antes de reiniciar o nó.

### Múltiplos nós crasharam simultaneamente

1. Se quorum foi perdido, aguardar que maioria dos nós volte.
2. O líder de maior epoch prevalece.
3. Se não há líder, nova eleição ocorre automaticamente quando quorum é restaurado.

---

## Validação Pós-Recuperação

- [ ] Nó aparece em `activeMembers` do cluster
- [ ] `replicationLag == 0`
- [ ] `gapsDetected` estável (delta == 0)
- [ ] `snapshotFallbackCount` não está crescendo
- [ ] Writes e reads completam normalmente
- [ ] `reachableNodesCount == totalNodesCount` em todos os peers
- [ ] Sem alertas `PERSISTENCE_FAILURE` no log

---

## Métricas Relacionadas

| Métrica | Localização |
|:---|:---|
| `replicationLag` | `NGridOperationalSnapshot.replicationLag()` |
| `gapsDetected` | `NGridOperationalSnapshot.gapsDetected()` |
| `resendSuccessCount` | `NGridOperationalSnapshot.resendSuccessCount()` |
| `snapshotFallbackCount` | `NGridOperationalSnapshot.snapshotFallbackCount()` |
| `averageConvergenceTimeMs` | `NGridOperationalSnapshot.averageConvergenceTimeMs()` |
| `pendingOperationsCount` | `NGridOperationalSnapshot.pendingOperationsCount()` |

---

## Alertas Automáticos Relacionados

| Alerta | Condição |
|:---|:---|
| `PERSISTENCE_FAILURE` | Falha de escrita WAL/epoch/sequence |
| `HIGH_REPLICATION_LAG` | Lag acima do threshold configurado |
| `HIGH_GAP_RATE` | Gaps crescendo rapidamente |
| `SNAPSHOT_FALLBACK_SPIKE` | Full syncs crescendo |
