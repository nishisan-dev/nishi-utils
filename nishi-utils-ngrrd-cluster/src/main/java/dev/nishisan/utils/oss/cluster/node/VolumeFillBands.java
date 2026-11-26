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

package dev.nishisan.utils.oss.cluster.node;

/**
 * Máquina de faixas do fill sobre a capacidade declarada do volume, usada para emitir o marcador
 * {@code NGRRD_VOLUME_FILL} só na <em>mudança</em> de faixa.
 *
 * <p>Limiares fixos (não configuráveis): {@code 0,80} aviso, {@code 0,90} crítico, {@code 0,95} recusa
 * ({@code CapacityBudget.FILL_RATIO}). A subida é imediata; a descida de uma faixa só ocorre quando o fill fica
 * abaixo do limiar dela menos {@link #HYSTERESIS} (1 ponto percentual), para o marcador não oscilar quando o fill
 * ronda o limiar. Uma queda grande pode descer várias faixas de uma vez.</p>
 *
 * <p>Não é thread-safe: é usada só pela thread do tick de status. O chamador é responsável por não alimentar a
 * máquina com "desconhecido" (capacidade declarada {@code 0}), que não é uma descida para a faixa normal.</p>
 */
final class VolumeFillBands {

    /** Histerese da descida, em fração (1 ponto percentual). */
    static final double HYSTERESIS = 0.01;

    /** Faixas de fill, em ordem crescente de severidade. */
    enum Band {
        NORMAL(0.0), WARN(0.80), CRITICAL(0.90), REFUSING(0.95);

        private final double threshold;

        Band(double threshold) {
            this.threshold = threshold;
        }
    }

    private Band current = Band.NORMAL;

    /** Faixa atual. */
    Band current() {
        return current;
    }

    /**
     * Alimenta a máquina com o fill observado e devolve a faixa resultante.
     *
     * @param fill fill sobre a capacidade declarada (pode passar de {@code 1.0})
     * @return a nova faixa atual (igual à anterior quando não há mudança)
     */
    Band update(double fill) {
        Band[] bands = Band.values();
        for (int i = bands.length - 1; i > current.ordinal(); i--) {
            if (fill >= bands[i].threshold) {
                current = bands[i];
                return current;
            }
        }
        while (current != Band.NORMAL && fill < current.threshold - HYSTERESIS) {
            current = bands[current.ordinal() - 1];
        }
        return current;
    }
}
