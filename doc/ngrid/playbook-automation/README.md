# Playbook Automation (CI)

Este pacote traz scripts e configuracao de logging para rodar o playbook em CI.

## Scripts

- `run-ci-resilience.sh`: executa o conjunto minimo de testes de resiliencia do NGrid.

## Logging

- `logging.properties` configura java.util.logging para console e arquivo.
- Logs sao gravados em `doc/ngrid/playbook-automation/logs/`.

## Uso

```bash
bash doc/ngrid/playbook-automation/run-ci-resilience.sh
```

## Testes incluidos

- `QueueCatchUpIntegrationTest`
- `CatchUpIntegrationTest`
- `QueueNodeFailoverIntegrationTest`
- `LeaderReelectionIntegrationTest`

Se quiser adicionar mais casos, edite o script e inclua novos `-Dtest`.
