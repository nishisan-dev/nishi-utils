/**
 * Backends de armazenamento plugáveis para o ngrrd.
 *
 * <p>Define a interface {@link dev.nishisan.utils.oss.storage.NgrrdStorage} e
 * as implementações iniciais:</p>
 *
 * <ul>
 *     <li>{@link dev.nishisan.utils.oss.storage.LocalDiskStorage} — disco local
 *     com escrita atômica via tmp + {@code Files.move(ATOMIC_MOVE, REPLACE_EXISTING)};</li>
 *     <li>{@link dev.nishisan.utils.oss.storage.S3Storage} — AWS S3 e compatíveis
 *     (MinIO/Ceph) via SDK v2 com suporte a {@code endpointOverride} e path-style.</li>
 * </ul>
 *
 * <p>O builder {@link dev.nishisan.utils.oss.storage.StorageKey} centraliza a
 * convenção determinística {@code {prefix}/{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}}
 * usada por escrita, leitura e idempotência.</p>
 */
package dev.nishisan.utils.oss.storage;
