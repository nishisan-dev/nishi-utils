package dev.nishisan.utils.oss.format;

import java.util.Objects;

/**
 * Resultado da decodificação de um bloco {@code .ngrrd}: cabeçalho validado
 * (CRC verificado) e payload já materializado em {@code double[]}.
 *
 * @param header   cabeçalho do bloco
 * @param payload  array de doubles ({@link Double#NaN} = missing); comprimento
 *                 sempre igual a {@code header.rows()}
 * @param crc32    CRC32 lido do bloco (após validação)
 */
public record EncodedBlock(BlockHeader header, double[] payload, int crc32) {

    public EncodedBlock {
        Objects.requireNonNull(header, "header é obrigatório");
        Objects.requireNonNull(payload, "payload é obrigatório");
        if (payload.length != header.rows()) {
            throw new IllegalArgumentException(
                    "payload.length=" + payload.length + " incompatível com header.rows=" + header.rows());
        }
    }
}
