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

/**
 * Desfecho do {@link NgrrdClusterClient#release release} de uma série ({@link SeriesRelease#status()}). Só
 * {@link #RELEASED} e {@link #NOT_HELD} autorizam o chamador a confirmar o progresso (por exemplo, o offset da
 * partição) que cobre a série.
 *
 * @since 8.15.0
 */
public enum ReleaseStatus {

    /**
     * Toda escrita admitida antes do release teve resposta final do dono, sem falha ainda não reportada (e o
     * {@code CHECKPOINT} confirmou, se pedido). Em {@code MARKS}, uma falha que uma marca aberta ANTES do release já
     * colheu foi reportada por essa marca e não aparece aqui; marcas criadas durante o release não colhem as falhas
     * da série liberada. Durabilidade: sem checkpoint, ACK do dono sem fsync, a mesma garantia de uma marca de escrita;
     * com checkpoint, fsync confirmado no dono.
     */
    RELEASED,

    /** Este cliente não tinha handle nem rota de escrita da série: nada a fazer. */
    NOT_HELD,

    /**
     * Drenou, mas há escritas admitidas antes do release com falha final ainda não reportadas a uma marca. Elas são
     * entregues aqui ({@link SeriesRelease#failedSamplesByStatus()}) e não vão mais para nenhuma marca. Em
     * {@code MARKS}, só as falhas que uma marca aberta antes do release já colheu ficam de fora (foram reportadas
     * por ela); em {@code BARRIER}, nenhuma barreira consome a falha, que sempre chega aqui. Com releases sobrepostos
     * da mesma série, a mesma falha aparece em todo release ainda ativo cuja fronteira a cubra; um release que começa
     * depois de outro já ter colhido a série não a vê de novo, e o release que admitiu a escrita sempre a recebe.
     */
    WRITE_FAILURES,

    /**
     * As escritas tiveram {@code OK}, mas o {@code CHECKPOINT} pedido falhou, estourou o prazo ou não foi executado: as
     * escritas da série vieram de um handle que já estava fechando quando o release começou, e não há handle gravável
     * deste release para mandar o {@code CHECKPOINT} ({@link SeriesRelease#errorCode()} {@link ErrorCode#CLOSED}).
     */
    CHECKPOINT_FAILED,

    /**
     * O dreno não terminou no prazo. O handle já foi fechado e a rota de escrita ficou sem dono; as escritas
     * pendentes continuam sendo entregues em segundo plano, e as falhas que surgirem vão para a próxima marca
     * ({@code MARKS}) ou ficam na rota ({@code BARRIER}). O chamador não deve confirmar o progresso dessa série.
     */
    TIMED_OUT,

    /** A geração da série foi removida ou posta em quarentena durante o release. */
    SERIES_DELETED
}
