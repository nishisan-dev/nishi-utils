package dev.nishisan.utils.oss.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
 * diretório raiz local. Escritas atômicas (YAML) usam arquivo temporário no
 * mesmo diretório do destino seguido de {@code Files.move(ATOMIC_MOVE,
 * REPLACE_EXISTING)}; quando o sistema de arquivos não suporta atomic move, faz
 * fallback transparente para {@code REPLACE_EXISTING}.
 *
 * <p>O objeto único da série é acessado via {@link SeriesChannelProvider} com
 * escrita <strong>in-place</strong> ({@link RandomAccessFile} + {@link FileChannel}),
 * regravando apenas as regiões alteradas (paridade com o RRDtool).</p>
 */
public final class LocalDiskStorage implements NgrrdStorage, SeriesChannelProvider {

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

    @Override
    public boolean seriesExists(String key) {
        return Files.exists(resolve(key));
    }

    @Override
    public SeriesChannel openSeries(String key) {
        Path target = resolve(key);
        try {
            Files.createDirectories(target.getParent());
            RandomAccessFile raf = new RandomAccessFile(target.toFile(), "rw");
            return new LocalSeriesChannel(raf, key);
        } catch (IOException e) {
            throw new NgrrdStorageException("Falha ao abrir série " + key, e);
        }
    }

    private Path resolve(String key) {
        Objects.requireNonNull(key, "key é obrigatório");
        if (key.startsWith("/") || key.contains("..")) {
            throw new IllegalArgumentException("key inválida: " + key);
        }
        return rootDir.resolve(key);
    }

    /** Canal in-place sobre {@link FileChannel}: grava só as regiões alteradas. */
    private static final class LocalSeriesChannel implements SeriesChannel {

        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final String key;
        private boolean dirty;

        LocalSeriesChannel(RandomAccessFile raf, String key) {
            this.raf = raf;
            this.channel = raf.getChannel();
            this.key = key;
        }

        @Override
        public long size() {
            try {
                return channel.size();
            } catch (IOException e) {
                throw new NgrrdStorageException("Falha ao obter tamanho da série " + key, e);
            }
        }

        @Override
        public void allocate(long totalBytes) {
            try {
                if (raf.length() != totalBytes) {
                    raf.setLength(totalBytes);
                    dirty = true;
                }
            } catch (IOException e) {
                throw new NgrrdStorageException("Falha ao pré-alocar série " + key, e);
            }
        }

        @Override
        public byte[] readRegion(long offset, int len) {
            ByteBuffer buf = ByteBuffer.allocate(len);
            try {
                long pos = offset;
                while (buf.hasRemaining()) {
                    int n = channel.read(buf, pos);
                    if (n < 0) {
                        throw new NgrrdStorageException("Leitura além do fim da série " + key
                                + " (offset=" + offset + ", len=" + len + ")");
                    }
                    pos += n;
                }
            } catch (IOException e) {
                throw new NgrrdStorageException("Falha ao ler região da série " + key, e);
            }
            return buf.array();
        }

        @Override
        public void writeRegion(long offset, byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            try {
                long pos = offset;
                while (buf.hasRemaining()) {
                    pos += channel.write(buf, pos);
                }
                dirty = true;
            } catch (IOException e) {
                throw new NgrrdStorageException("Falha ao escrever região da série " + key, e);
            }
        }

        @Override
        public void force() {
            if (!dirty) {
                return;
            }
            try {
                channel.force(true);
                dirty = false;
            } catch (IOException e) {
                throw new NgrrdStorageException("Falha ao sincronizar série " + key, e);
            }
        }

        @Override
        public void close() {
            try {
                force();
            } finally {
                try {
                    raf.close();
                } catch (IOException e) {
                    throw new NgrrdStorageException("Falha ao fechar série " + key, e);
                }
            }
        }
    }
}
