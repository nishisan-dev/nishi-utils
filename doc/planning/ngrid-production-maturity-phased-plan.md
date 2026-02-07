# Plano Faseado de Maturidade para Produção (NGrid, Queue e Map)

## 1. Objetivo
Este plano define um caminho faseado para elevar a maturidade de produção do NGrid com foco em:
- Durabilidade de dados (queue/map/offsets).
- Resiliência a falhas (crash, partição, failover e recovery).
- Consistência operacional (semântica previsível sob degradação).
- Operabilidade (observabilidade, runbooks, SLOs e automação de validação).

## 2. Escopo e Premissas
- Escopo principal: `ngrid`, `queue`, `map`, camada de replicação e persistência.
- Contexto atual (baseline em 07/02/2026):
1. Cobertura de integração relevante já existe (failover, quorum, catch-up, durability tests).
2. Há funcionalidades de sync/catch-up por snapshot.
3. Persistência de map/offsets depende de configuração e parte do comportamento ainda é best-effort.
4. Em modo não estrito, quorum pode ser reduzido em partição.

## 3. Nível-Alvo de Maturidade
- Alvo final: **Maturidade de Produção Nível 4 (Confiável em cenário crítico)**.
- Definição resumida:
1. Perda de dados evitada para operações confirmadas (dentro da política escolhida).
2. Comportamento consistente e documentado em partições/failover.
3. Recuperação testada sistematicamente por automação.
4. SLOs operacionais definidos e monitorados.

## 4. Princípios de Arquitetura
1. Consistência primeiro para dados críticos.
2. Durabilidade explícita (modo e trade-off devem ser declarados, nunca implícitos).
3. Falha detectável: erro importante não pode ficar apenas em log.
4. Segurança operacional: cada fase libera somente com critério objetivo (gate).

## 5. Plano Faseado

## Fase 0 - Baseline e Gate Inicial (Semana 1)
Objetivo: congelar diagnóstico, riscos e critérios de sucesso.

Entregáveis:
1. Documento de baseline com comportamento atual por cenário:
- Crash do líder.
- Crash de follower.
- Partição curta e longa.
- Restart total do cluster.
2. Matriz de modo de operação por ambiente (`dev`, `staging`, `prod`):
- `strictConsistency`.
- `mapPersistenceMode`.
- `NQueue.Options.withFsync`.
3. Definição de SLO inicial:
- Disponibilidade de escrita.
- Latência P95/P99 de `offer`, `poll`, `put`, `remove`.
- Taxa de erro de replicação e de sync.

Critério de saída:
1. Baseline aprovado pelo time.
2. SLOs e limites de aceite documentados.

Riscos:
1. Subestimar gaps por falta de medição real.

---

## Fase 1 - Consistência e Política de Quorum (Semanas 2-3)
Objetivo: tornar o comportamento em partição previsível e seguro para produção crítica.

Entregáveis:
1. Política padrão de produção:
- `strictConsistency=true` em ambientes críticos.
- fallback para modo não estrito apenas com aceite explícito de risco.
2. Guardrails de configuração:
- falhar startup em `prod` se combinação de config violar política.
3. Documento de decisão arquitetural (ADR):
- quando usar consistência estrita vs. disponibilidade priorizada.

Critério de saída:
1. Testes de partição validando que escrita confirmada respeita política escolhida.
2. Configuração inválida bloqueada automaticamente.

Riscos:
1. Redução de disponibilidade percebida em cenários de rede ruim.

---

## Fase 2 - Durabilidade Forte de Offsets e Maps Críticos (Semanas 3-5)
Objetivo: reduzir replay/regressão após crash e restart.

Entregáveis:
1. Tornar persistência de mapas críticos obrigatória:
- `_ngrid-queue-offsets` com `ASYNC_WITH_FSYNC` (ou modo síncrono dedicado, se criado).
2. Endurecer persistência local de offsets:
- escrita atômica (`tmp` + `atomic move` + `fsync` quando aplicável).
- validação de integridade no boot.
3. Sinalização de falha de durabilidade:
- erro crítico de persistência deve aparecer em métrica/healthcheck e não somente em log.

Critério de saída:
1. Testes de crash abrupto repetidos (N execuções) sem regressão de offset confirmada.
2. Recovery consistente após restart simultâneo de nós.

Riscos:
1. Custo de latência por fsync em carga alta.

---

## Fase 3 - Recovery Inteligente de Lacunas de Sequência (Semanas 5-6)
Objetivo: diminuir custo de recuperação e melhorar convergência.

Entregáveis:
1. Implementação de `SEQUENCE_RESEND_REQUEST` e `SEQUENCE_RESEND_RESPONSE`.
2. Estratégia híbrida de recuperação:
- tentar resend pontual primeiro;
- fallback para snapshot sync completo quando necessário.
3. Métricas dedicadas:
- quantidade de gaps detectados.
- tempo médio de convergência.
- frequência de fallback para snapshot completo.

Critério de saída:
1. Testes de perda/atraso de mensagens com convergência dentro da janela alvo.
2. Redução mensurável de syncs completos em cenários de falha parcial.

Riscos:
1. Complexidade adicional de protocolo e compatibilidade entre versões.

---

## Fase 4 - Observabilidade, Operação e Runbooks (Semanas 6-7)
Objetivo: tornar produção operável com previsibilidade.

Entregáveis:
1. Dashboard operacional:
- quorum efetivo, lag por tópico, taxa de timeout, tempo de failover, backlog por queue.
2. Alertas de produção:
- falha de persistência.
- aumento anormal de sync completo.
- degradação de latência de operação crítica.
3. Runbooks:
- partição de rede.
- líder instável.
- recuperação pós-crash.
- validação de consistência pós-incidente.

Critério de saída:
1. Simulação de incidente com execução de runbook ponta a ponta.
2. Alertas acionando com baixo falso positivo.

Riscos:
1. Excesso de ruído sem thresholds calibrados.

---

## Fase 5 - Hardening Final e Go-Live Gate (Semanas 8-9)
Objetivo: liberar produção crítica com confiança.

Entregáveis:
1. Pipeline de resiliência obrigatório no CI:
- failover, partição, crash abrupto, restart total, soak test.
2. Teste de carga de longa duração:
- 12h a 24h com churn de liderança controlado.
3. Checklist de go-live com evidências:
- critérios funcionais, não funcionais e operacionais.

Critério de saída:
1. Todos os gates aprovados por 2 ciclos consecutivos.
2. Sem bugs críticos abertos em consistência/durabilidade.

Riscos:
1. Defeitos intermitentes só aparecerem em longa duração.

## 6. Backlog Técnico Priorizado
Prioridade P0:
1. Política de consistência para produção (`strictConsistency` + guardrails).
2. Persistência robusta de `_ngrid-queue-offsets`.
3. Health/alert para falha de persistência.

Prioridade P1:
1. Reenvio de sequência (resend protocol).
2. Métricas de convergência/sync.
3. Runbooks operacionais.

Prioridade P2:
1. Otimizações de performance sob fsync.
2. Melhorias de ergonomia de configuração por perfil de ambiente.

## 7. Métricas de Aceite por Pilar
Consistência:
1. Zero confirmação indevida fora da política de quorum definida.
2. Divergência entre nós detectada e corrigida na janela de convergência alvo.

Durabilidade:
1. Zero perda de operação confirmada em cenários cobertos pela política de durabilidade.
2. Zero regressão de offset confirmada nos testes de crash.

Resiliência:
1. Failover dentro do tempo alvo.
2. Recuperação de partição sem intervenção manual no cenário padrão.

Operação:
1. 100% dos incidentes simulados cobertos por runbook.
2. Alertas críticos com acionamento confiável.

## 8. Responsabilidades Sugeridas
1. Arquitetura/Backend: consistência, protocolo e persistência.
2. SRE/Plataforma: observabilidade, alerta, CI de resiliência.
3. QA/Qualidade: cenários de caos, regressão e soak.
4. Product/Tech Lead: aprovação de trade-offs de consistência vs disponibilidade.

## 9. Cronograma Resumido
1. Semanas 1-3: Fases 0 e 1 (baseline e política de consistência).
2. Semanas 3-6: Fases 2 e 3 (durabilidade forte e resend).
3. Semanas 6-9: Fases 4 e 5 (operação, hardening e gate final).

## 10. Critério de "Pronto para Produção Crítica"
Considerar pronto quando:
1. Fases 0-5 concluídas com evidência.
2. SLOs estabilizados por pelo menos 2 janelas de validação consecutivas.
3. Sem pendência P0 aberta.
4. Operação com runbooks e alertas efetivos em staging espelhado de produção.

