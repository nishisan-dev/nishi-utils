package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;

import java.util.Objects;

/**
 * Cabeçalho fixo de 30 bytes que prefixa cada bloco binário {@code .ngrrd}.
 *
 * <p>O layout é estável entre versões pelo campo {@link #version()}; alterações
 * incompatíveis exigem incremento. O CRC32 cobre os 26 bytes anteriores do
 * header (excluindo o próprio CRC) seguidos do payload {@code rows * 8 bytes}.</p>
 *
 * <p>Bits de {@link #flags()}:</p>
 * <ul>
 *     <li>{@code bit 0} — payload comprimido (reservado; sempre {@code false} no MVP)</li>
 *     <li>{@code bit 1} — bloco parcial (ainda não fechado, gravado em snapshot)</li>
 *     <li>{@code bit 2} — bloco agregado (RRA) vs raw</li>
 * </ul>
 *
 * @param version           versão do formato (atualmente {@link #CURRENT_VERSION})
 * @param flags             bits descritos acima
 * @param stepSec           segundos por linha do payload
 * @param cf                função de consolidação (raw = LAST por convenção)
 * @param ds                tipo do data source originário do bloco
 * @param blockStartEpoch   epoch em segundos UTC alinhado a múltiplos de stepSec
 * @param rows              quantidade de doubles no payload
 */
public record BlockHeader(
        int version,
        int flags,
        int stepSec,
        ConsolidationFunction cf,
        DataSourceType ds,
        long blockStartEpoch,
        int rows) {

    /** Magic number ASCII {@code "NGRD"} = {@code 0x4E475244}. */
    public static final int MAGIC = 0x4E475244;

    /** Versão atual do formato binário. */
    public static final int CURRENT_VERSION = 1;

    /** Tamanho fixo do header em bytes (incluindo CRC32). */
    public static final int HEADER_BYTES = 30;

    /** Tamanho do header sem o campo CRC32 (utilizado no cálculo de checksum). */
    public static final int HEADER_BYTES_NO_CRC = HEADER_BYTES - 4;

    public static final int FLAG_COMPRESSED = 0x0001;
    public static final int FLAG_PARTIAL = 0x0002;
    public static final int FLAG_AGG = 0x0004;

    public BlockHeader {
        if (version <= 0 || version > 0xFFFF) {
            throw new IllegalArgumentException("version fora do intervalo uint16: " + version);
        }
        if ((flags & ~0xFFFF) != 0) {
            throw new IllegalArgumentException("flags fora do intervalo uint16: " + flags);
        }
        if (stepSec <= 0) {
            throw new IllegalArgumentException("stepSec deve ser > 0: " + stepSec);
        }
        if (rows < 0) {
            throw new IllegalArgumentException("rows deve ser >= 0: " + rows);
        }
        Objects.requireNonNull(cf, "cf é obrigatório");
        Objects.requireNonNull(ds, "ds é obrigatório");
        if (blockStartEpoch % stepSec != 0) {
            throw new IllegalArgumentException(
                    "blockStartEpoch=" + blockStartEpoch + " deve ser múltiplo de stepSec=" + stepSec);
        }
    }

    public boolean compressed() {
        return (flags & FLAG_COMPRESSED) != 0;
    }

    public boolean partial() {
        return (flags & FLAG_PARTIAL) != 0;
    }

    public boolean agg() {
        return (flags & FLAG_AGG) != 0;
    }

    /** Builder ergonômico de {@code flags} a partir dos três bits expostos. */
    public static int flags(boolean compressed, boolean partial, boolean agg) {
        int f = 0;
        if (compressed) {
            f |= FLAG_COMPRESSED;
        }
        if (partial) {
            f |= FLAG_PARTIAL;
        }
        if (agg) {
            f |= FLAG_AGG;
        }
        return f;
    }
}
