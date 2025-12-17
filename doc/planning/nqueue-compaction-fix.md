# Plano de Refatoração: NQueue Compaction & Stability Fix

**Data:** 17/12/2025
**Objetivo:** Corrigir falhas silenciosas na compactação (aborto por mudança de estado) e bugs críticos de I/O que podem causar perda de dados ou inconsistência na `NQueue`.

## 1. Diagnóstico dos Problemas Atuais

1.  **Compactação Ineficaz (Bug do "State Changed"):** A lógica atual aborta a compactação se `producerOffset` ou `consumerOffset` mudarem durante a cópia assíncrona. Em sistemas ativos, isso acontece 100% do tempo, tornando a compactação inútil e o arquivo cresce indefinidamente.
2.  **Risco de Corrupção (ClosedChannel):** O método `finalizeCompaction` fecha o canal do arquivo original *antes* de mover o novo arquivo. Se o `Files.move` falhar, a fila entra em estado irrecuperável.
3.  **Leitura Insegura (Partial Read):** O método `readAtInternal` assume que `channel.read(buffer)` preenche o buffer inteiro em uma chamada. Se o SO devolver menos bytes (leitura parcial), uma `EOFException` incorreta é lançada.
4.  **Falhas Silenciosas:** Exceções durante a compactação (ex: disco cheio) são capturadas e ignoradas, sem alertas ou retries.

## 2. Estratégia de Solução

### 2.1. Novo Algoritmo de Compactação ("Catch-up Strategy")
Para permitir que a fila continue operando enquanto compactamos, mudaremos a abordagem:

1.  **Fase 1 (Async - Sem Lock):**
    *   Copiar dados do `snapshot.consumerOffset` até `snapshot.producerOffset` para o arquivo temporário (`data.log.compacting`).
    *   Isso remove o "lixo" antigo (dados antes do consumer).
2.  **Fase 2 (Sync - Com Lock):**
    *   Adquirir o `lock` principal da fila.
    *   Verificar o estado atual (`current`).
    *   **Append Delta:** Copiar os dados novos que chegaram no arquivo original (`snapshot.producerOffset` até `current.producerOffset`) para o final do arquivo temporário.
    *   **Cálculo de Offset:** Calcular o novo `consumerOffset` baseado no progresso que o consumidor fez no arquivo original durante a Fase 1.
    *   **Swap Atômico:** Trocar os arquivos e referências de canal.

### 2.2. Correção de Robustez de I/O
*   **Safe Swap:** Implementar um mecanismo de troca que só fecha o canal original se o arquivo temporário estiver pronto e íntegro. Adicionar bloco `try-catch` para reabrir o original em caso de falha no `move`.
*   **Loop de Leitura:** Reescrever a leitura de buffers para usar um loop `while(buffer.hasRemaining())` garantindo leitura completa.

## 3. Plano de Execução

### Passo 1: Preparação e Teste de Reprodução
*   Criar um teste `NQueueCompactionStressTest.java`.
*   Cenário: Produtor e Consumidor rodando em paralelo em alta frequência.
*   Gatilho: Forçar compactação frequente.
*   Assert: Verificar se o tamanho do arquivo físico diminui (atualmente deve falhar).

### Passo 2: Correções de Baixo Nível (Safe I/O)
*   Modificar `readAtInternal` para garantir leitura completa de buffers.
*   Adicionar validações de integridade no `NQueueRecord`.

### Passo 3: Refatoração da Compactação (Core)
*   Alterar `runCompaction` para não usar a verificação restritiva de `stateChanged`.
*   Implementar a lógica de cópia do "Delta" (dados produzidos durante a compactação).
*   Reescrever `finalizeCompaction` para ser resiliente a falhas de FS.

### Passo 4: Verificação
*   Rodar o teste de estresse criado no Passo 1.
*   Rodar a suíte de testes existente (`mvn test`) para garantir que nada quebrou (regressão).

## 4. API e Compatibilidade
*   **Nenhuma mudança na API pública** (`offer`, `poll`, `peek`, construtores).
*   A classe `NQueue` manterá compatibilidade total com o código existente.
*   A serialização interna dos dados não será alterada neste momento (para evitar problemas de compatibilidade com arquivos `.log` existentes).

## 5. Critérios de Aceite
1.  O arquivo `data.log` deve diminuir de tamanho após ciclos de produção/consumo, mesmo sob carga constante.
2.  Nenhuma `ClosedChannelException` deve ocorrer se o sistema de arquivos falhar temporariamente durante o swap.
3.  Todos os testes unitários existentes devem passar.
