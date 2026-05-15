package dev.nishisan.utils.oss.api;

/**
 * Tipos de Data Source suportados, em paridade conceitual com RRDtool.
 *
 * <ul>
 *     <li>{@link #COUNTER} — valor monotônico crescente (ex.: ifInOctets); a engine
 *     deriva taxa por meio do delta entre amostras consecutivas, com detecção de
 *     reset e wrap (32/64 bits).</li>
 *     <li>{@link #GAUGE} — valor pontual sem derivação (ex.: temperatura, uso de CPU).</li>
 *     <li>{@link #DERIVE} — comporta-se como COUNTER mas permite deltas negativos
 *     (sem detecção automática de reset).</li>
 *     <li>{@link #ABSOLUTE} — contador que é zerado a cada leitura (delta = valor atual).</li>
 * </ul>
 */
public enum DataSourceType {
    COUNTER,
    GAUGE,
    DERIVE,
    ABSOLUTE
}
