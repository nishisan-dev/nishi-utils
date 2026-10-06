/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.api;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Fronteira capturada por {@link NgrrdClusterClient#mark()}: todas as escritas admitidas no cliente antes da
 * chamada. A marca conclui quando cada uma dessas escritas recebeu resposta final do cluster, segundo a política
 * de confirmação vigente (hoje: ACK do dono, sem fsync), com sucesso ou falha. Escritas em retentativa
 * ({@code MIGRATING}, troca de dono, reabertura, storage node fora) apenas atrasam a conclusão — não há prazo na
 * biblioteca.
 *
 * <p>Marcas concluem em ordem: o resultado da marca N só fica disponível depois do da N−1. Cada falha de escrita é
 * atribuída a exatamente uma marca (ver {@link WriteMarkResult}).</p>
 *
 * <p>Thread-safe; consultas não bloqueiam.</p>
 *
 * @since 8.12.0
 */
public interface WriteMark {

    /** Identificador crescente, a partir de 1, por cliente. */
    long id();

    /** {@code true} quando o resultado está disponível — ou quando a marca falhou ({@code close()} do cliente). */
    boolean isDone();

    /** O resultado, se a marca já concluiu com sucesso; vazio enquanto pendente ou se falhou. */
    Optional<WriteMarkResult> result();

    /**
     * Uma nova {@link CompletableFuture} dependente da conclusão da marca, a cada chamada: completá-la ou
     * cancelá-la não afeta a marca. Falha com {@link NgrrdClusterException} ({@link ErrorCode#CLOSED}) se o
     * cliente fechar antes da conclusão.
     *
     * <p>Callbacks encadeados sem {@code *Async} rodam numa thread interna da biblioteca — ou, para as marcas
     * resolvidas pelo fechamento, na thread que chamou {@code close()}: não bloqueie neles.</p>
     */
    CompletableFuture<WriteMarkResult> completion();
}
