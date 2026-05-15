package dev.nishisan.utils.oss.storage;

/**
 * Resultado de {@link NgrrdStorage#verifyOrReplaceIfIdentical(String, byte[])}.
 *
 * <ul>
 *     <li>{@link #WRITTEN} — não existia; gravado pela primeira vez.</li>
 *     <li>{@link #IDENTICAL_SKIPPED} — existente já idêntico; escrita evitada.</li>
 *     <li>{@link #REPLACED} — existente divergente; substituído.</li>
 * </ul>
 */
public enum VerifyResult {
    WRITTEN,
    IDENTICAL_SKIPPED,
    REPLACED
}
