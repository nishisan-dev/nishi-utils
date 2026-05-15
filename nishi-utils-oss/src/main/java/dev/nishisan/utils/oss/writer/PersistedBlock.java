package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;

/**
 * Registro em memória de um bloco já persistido pelo {@code NgrrdWriter}. É a
 * fonte usada pelo {@code ManifestUpdater} para gerar snapshots versionados.
 *
 * @param rraName         nome do RRA (ou {@code "raw"} para PDPs do step base)
 * @param dsName          DS derivado a que o bloco pertence
 * @param cf              função de consolidação aplicada
 * @param stepSec         resolução do bloco em segundos
 * @param blockStartEpoch epoch (segundos UTC) do início do bloco
 * @param rows            número de CDPs/PDPs no bloco
 * @param crc32           CRC32 unsigned do conteúdo persistido
 * @param storageKey      chave completa no Storage
 */
public record PersistedBlock(
        String rraName,
        String dsName,
        ConsolidationFunction cf,
        int stepSec,
        long blockStartEpoch,
        int rows,
        long crc32,
        String storageKey) {
}
