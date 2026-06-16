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

package dev.nishisan.utils.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Internal helpers for mapping offloaded entries to disk paths.
 * <p>
 * The directory fan-out is configurable: a layout produces sharded paths with
 * {@code shardDepth} directory levels, each consuming {@code shardWidth}
 * hexadecimal characters of the key hash. The historical layout
 * ({@code hash[0:2]/hash[2:4]/hash.dat}) corresponds to {@link #defaults()}
 * ({@code shardDepth=2}, {@code shardWidth=2}).
 * <p>
 * Hashing ({@link #keyHash(Object)}) and flat (legacy) paths are independent of
 * the fan-out and remain static. Sharded-path construction depends on the
 * configured fan-out and is therefore instance scoped.
 */
final class OffloadLayout {

    /** Historical shard depth (number of directory levels). */
    static final int DEFAULT_SHARD_DEPTH = 2;
    /** Historical shard width (hex characters per directory level). */
    static final int DEFAULT_SHARD_WIDTH = 2;
    /** Length, in hex characters, of the SHA-1 key hash. */
    private static final int HASH_HEX_LENGTH = 40;

    private final int shardDepth;
    private final int shardWidth;

    private OffloadLayout(int shardDepth, int shardWidth) {
        if (shardDepth < 0) {
            throw new IllegalArgumentException("shardDepth must be >= 0");
        }
        if (shardWidth < 1) {
            throw new IllegalArgumentException("shardWidth must be >= 1");
        }
        if (shardDepth * shardWidth > HASH_HEX_LENGTH) {
            throw new IllegalArgumentException(
                    "shardDepth * shardWidth must be <= " + HASH_HEX_LENGTH
                            + " (got " + (shardDepth * shardWidth) + ")");
        }
        this.shardDepth = shardDepth;
        this.shardWidth = shardWidth;
    }

    /**
     * Creates a layout with the given fan-out.
     *
     * @param shardDepth number of directory levels (0 = flat layout)
     * @param shardWidth hex characters consumed per directory level
     * @return the layout
     */
    static OffloadLayout of(int shardDepth, int shardWidth) {
        return new OffloadLayout(shardDepth, shardWidth);
    }

    /**
     * Returns the historical layout ({@code shardDepth=2}, {@code shardWidth=2}),
     * preserving compatibility with data written before the fan-out became
     * configurable.
     *
     * @return the default layout
     */
    static OffloadLayout defaults() {
        return new OffloadLayout(DEFAULT_SHARD_DEPTH, DEFAULT_SHARD_WIDTH);
    }

    /** Returns the configured shard depth. */
    int shardDepth() {
        return shardDepth;
    }

    /** Returns the configured shard width. */
    int shardWidth() {
        return shardWidth;
    }

    static String keyHash(Object key) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(key);
            }
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(baos.toByteArray());
            return HexFormat.of().formatHex(digest);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new UncheckedIOException(new IOException("Failed to compute key hash", e));
        }
    }

    static Path legacyPath(Path rootDir, String keyHash, String entrySuffix) {
        Objects.requireNonNull(rootDir, "rootDir");
        Objects.requireNonNull(keyHash, "keyHash");
        Objects.requireNonNull(entrySuffix, "entrySuffix");
        return rootDir.resolve(keyHash + entrySuffix);
    }

    Path shardedPath(Path rootDir, String keyHash, String entrySuffix) {
        Objects.requireNonNull(rootDir, "rootDir");
        Objects.requireNonNull(keyHash, "keyHash");
        Objects.requireNonNull(entrySuffix, "entrySuffix");
        Path dir = rootDir;
        for (int level = 0; level < shardDepth; level++) {
            int start = level * shardWidth;
            dir = dir.resolve(keyHash.substring(start, start + shardWidth));
        }
        return dir.resolve(keyHash + entrySuffix);
    }

    static boolean isSharded(Path rootDir, Path candidate) {
        if (candidate == null) {
            return false;
        }
        Path relative = rootDir.normalize().relativize(candidate.normalize());
        return relative.getNameCount() > 1;
    }

    static boolean shouldPrefer(Path rootDir, Path current, Path candidate) {
        if (current == null) {
            return true;
        }
        boolean currentSharded = isSharded(rootDir, current);
        boolean candidateSharded = isSharded(rootDir, candidate);
        return candidateSharded && !currentSharded;
    }

    Path preferredExistingPath(Path rootDir, String keyHash, String entrySuffix) {
        Path sharded = shardedPath(rootDir, keyHash, entrySuffix);
        if (Files.exists(sharded)) {
            return sharded;
        }
        Path legacy = legacyPath(rootDir, keyHash, entrySuffix);
        return Files.exists(legacy) ? legacy : null;
    }

    static void deleteFileQuietly(Path rootDir, Path path, Logger logger) {
        if (path == null) {
            return;
        }
        try {
            if (Files.deleteIfExists(path)) {
                deleteEmptyShardParents(rootDir, path, logger);
            }
        } catch (IOException e) {
            if (logger != null) {
                logger.log(Level.WARNING, "Failed to delete offloaded file: " + path, e);
            }
        }
    }

    static void clearDirectoryContentsRecursively(Path rootDir, Logger logger) {
        if (!Files.exists(rootDir)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(rootDir)) {
            stream.filter(path -> !path.equals(rootDir))
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            if (logger != null) {
                                logger.log(Level.WARNING, "Failed to delete offload path: " + path, e);
                            }
                        }
                    });
        } catch (IOException e) {
            if (logger != null) {
                logger.log(Level.WARNING, "Failed to clear offload directory: " + rootDir, e);
            }
        }
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            if (logger != null) {
                logger.log(Level.WARNING, "Failed to recreate offload directory: " + rootDir, e);
            }
        }
    }

    private static void deleteEmptyShardParents(Path rootDir, Path path, Logger logger) {
        Path parent = path.getParent();
        while (parent != null && !parent.equals(rootDir)) {
            try (Stream<Path> children = Files.list(parent)) {
                if (children.findAny().isPresent()) {
                    return;
                }
            } catch (IOException e) {
                if (logger != null) {
                    logger.log(Level.WARNING, "Failed to inspect shard directory: " + parent, e);
                }
                return;
            }
            try {
                Files.deleteIfExists(parent);
            } catch (IOException e) {
                if (logger != null) {
                    logger.log(Level.WARNING, "Failed to delete shard directory: " + parent, e);
                }
                return;
            }
            parent = parent.getParent();
        }
    }
}
