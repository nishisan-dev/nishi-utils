package dev.nishisan.utils.oss.api;

/**
 * Função de consolidação aplicada por um RRA ao agregar os PDPs (Primary Data
 * Points) do step base em CDPs (Consolidated Data Points) no step do arquivo.
 */
public enum ConsolidationFunction {
    AVERAGE,
    MAX,
    MIN,
    LAST
}
