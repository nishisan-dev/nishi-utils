package dev.nishisan.utils.oss.format;

/**
 * Visão decodificada do cabeçalho fixo (96 bytes) do arquivo de série NGRR.
 *
 * <p>Os offsets são redundantes com a {@link SeriesGeometry} recomputada a
 * partir da definição — servem para validação cruzada e para ferramentas que
 * leem o arquivo sem a definição em mãos. A continuidade do dado é garantida
 * pela comparação de {@code definitionHash}.</p>
 *
 * @param formatVersion      versão do formato binário NGRR
 * @param flags              bits reservados
 * @param baseStepSec        step base em segundos
 * @param definitionHash     SHA-256 (32 bytes) do YAML da definição
 * @param columnCount        número de colunas (DS derivados)
 * @param archiveCount       número de archives ({@code rra × cf})
 * @param staticSectionBytes fim da seção estática (header + dicionários)
 * @param liveStateOffset    offset absoluto da live-state
 * @param liveStateBytes     tamanho da live-state
 * @param ringDataOffset     offset absoluto da região de rings
 * @param fileTotalBytes     tamanho total pré-alocado do arquivo
 */
public record SeriesHeader(
        int formatVersion,
        int flags,
        int baseStepSec,
        byte[] definitionHash,
        int columnCount,
        int archiveCount,
        long staticSectionBytes,
        long liveStateOffset,
        long liveStateBytes,
        long ringDataOffset,
        long fileTotalBytes) {
}
