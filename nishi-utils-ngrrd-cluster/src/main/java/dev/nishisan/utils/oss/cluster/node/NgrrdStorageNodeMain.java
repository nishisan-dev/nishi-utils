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

package dev.nishisan.utils.oss.cluster.node;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processo standalone de um storage node do cluster ngrrd — sem Spring, sem dependências além do
 * próprio módulo: {@code java -cp ... dev.nishisan.utils.oss.cluster.node.NgrrdStorageNodeMain --config
 * <yaml>}. Sobe o nó a partir de {@link StorageNodeConfig#fromYaml(Path, java.util.function.Function)},
 * registra um shutdown hook que fecha o nó na ordem correta ({@link NgrrdStorageNode#close()}) e
 * bloqueia a thread principal até o processo ser encerrado (SIGTERM/Ctrl-C).
 *
 * <p>Loga {@code NGRRD_STORAGE_NODE_STARTED nodeId=... port=...} assim que o nó está pronto — marker de
 * log usado por futuros testes de integração via Docker (ver convenção de markers do módulo
 * {@code ngrid-test}, documentada em {@code CLAUDE.md}); não renomear sem atualizar quem depende dele.</p>
 *
 * <p>Argumentos inválidos ou um YAML de configuração malformado (ambos manifestados como
 * {@link IllegalArgumentException}) imprimem só a mensagem de erro e o uso em {@code stderr} — sem
 * stack trace — e saem com código 1; nunca deixam a exceção subir crua para o runtime da JVM.</p>
 */
public final class NgrrdStorageNodeMain {

    private static final Logger LOGGER = Logger.getLogger(NgrrdStorageNodeMain.class.getName());
    private static final String CONFIG_FLAG = "--config";
    private static final String USAGE = "uso: java -cp ... " + NgrrdStorageNodeMain.class.getName()
            + " --config <caminho-do-yaml>";

    private NgrrdStorageNodeMain() {
    }

    public static void main(String[] args) throws IOException {
        StorageNodeConfig config;
        try {
            Path configPath = parseConfigPath(args);
            config = StorageNodeConfig.fromYaml(configPath, System::getenv);
        } catch (IllegalArgumentException e) {
            System.err.println("erro: " + e.getMessage());
            System.err.println(USAGE);
            System.exit(1);
            return;
        }
        NgrrdStorageNode node = NgrrdStorageNode.start(config);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("NGRRD_STORAGE_NODE_STOPPING nodeId=" + node.nodeId());
            try {
                node.close();
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Falha ao fechar o storage node " + node.nodeId() + " no shutdown", e);
            } finally {
                shutdownLatch.countDown();
            }
        }, "ngrrd-storage-node-shutdown"));

        LOGGER.info("NGRRD_STORAGE_NODE_STARTED nodeId=" + node.nodeId() + " port=" + node.config().port());
        awaitUninterruptibly(shutdownLatch);
    }

    /** Bloqueia a thread principal até o shutdown hook liberar o latch, ignorando interrupções espúrias. */
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Visível para teste (mesmo pacote) — {@link #main} nunca deixa isto escapar sem tratar. */
    static Path parseConfigPath(String[] args) {
        if (args != null) {
            for (int i = 0; i < args.length - 1; i++) {
                if (CONFIG_FLAG.equals(args[i])) {
                    return Path.of(args[i + 1]);
                }
            }
        }
        throw new IllegalArgumentException("uso: --config <caminho-do-yaml>");
    }
}
