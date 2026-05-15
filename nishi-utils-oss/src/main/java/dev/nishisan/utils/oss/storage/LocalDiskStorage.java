package dev.nishisan.utils.oss.storage;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Backend de storage que materializa chaves como caminhos relativos sob um
 * diretório raiz local. Escritas atômicas usam arquivo temporário no mesmo
 * diretório do destino seguido de {@code Files.move(ATOMIC_MOVE, REPLACE_EXISTING)};
 * quando o sistema de arquivos não suporta atomic move, faz fallback transparente
 * para {@code REPLACE_EXISTING}.
 */
public final class LocalDiskStorage implements NgrrdStorage {

    private final Path rootDir;

    public LocalDiskStorage(Path rootDir) {
        this.rootDir = Objects.requireNonNull(rootDir, "rootDir é obrigatório");
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao garantir diretório raiz: " + rootDir, e);
        }
    }

    public Path rootDir() {
        return rootDir;
    }

    @Override
    public void put(String key, byte[] data) {
        Path target = resolve(key);
        try {
            Files.createDirectories(target.getParent());
            Files.write(target, data);
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao gravar " + key, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Path target = resolve(key);
        try {
            return Optional.of(Files.readAllBytes(target));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao ler " + key, e);
        }
    }

    @Override
    public boolean exists(String key) {
        return Files.exists(resolve(key));
    }

    @Override
    public void delete(String key) {
        try {
            Files.deleteIfExists(resolve(key));
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao deletar " + key, e);
        }
    }

    @Override
    public List<String> list(String prefix) {
        Path base = resolve(prefix);
        if (!Files.exists(base)) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        try {
            Files.walkFileTree(base, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    Path relative = rootDir.relativize(file);
                    result.add(relative.toString().replace('\\', '/'));
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao listar " + prefix, e);
        }
        return result;
    }

    @Override
    public void atomicReplace(String key, byte[] data) {
        Path target = resolve(key);
        Path parent = target.getParent();
        try {
            Files.createDirectories(parent);
            Path tmp = Files.createTempFile(parent, ".ngrrd-tmp-", ".part");
            try {
                Files.write(tmp, data);
                try {
                    Files.move(tmp, target,
                            StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha em atomicReplace " + key, e);
        }
    }

    private Path resolve(String key) {
        Objects.requireNonNull(key, "key é obrigatório");
        if (key.startsWith("/") || key.contains("..")) {
            throw new IllegalArgumentException("key inválida: " + key);
        }
        return rootDir.resolve(key);
    }
}
