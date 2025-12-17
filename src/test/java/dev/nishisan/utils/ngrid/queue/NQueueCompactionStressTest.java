package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.queue.NQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste de estresse focado em reproduzir a falha de compactação
 * quando a fila está sob carga constante de escrita/leitura.
 */
@Disabled("Teste de estresse para validação manual de concorrência e compactação")
public class NQueueCompactionStressTest {

    @TempDir
    Path tempDir;

    private NQueue<String> queue;
    private ExecutorService executor;

    @BeforeEach
    public void setUp() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (queue != null) {
            queue.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCompactionUnderLoad() throws Exception {
        // Configuração agressiva para forçar compactação frequente
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionInterval(Duration.ofMillis(100)) // Tenta compactar a cada 100ms
                .withCompactionWasteThreshold(0.2) // 20% de desperdício já gatilha
                .withFsync(false); // Desabilitar fsync para velocidade do teste

        String queueName = "stress-queue";
        queue = NQueue.open(tempDir, queueName, options);

        int itemCount = 50_000;
        AtomicLong consumedCount = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch producerDoneLatch = new CountDownLatch(1);

        // Producer
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < itemCount; i++) {
                    queue.offer("Payload-Data-" + i + "-Extra-Padding-To-Make-File-Grow-" + i);
                    // Removido sleep para aumentar pressão e velocidade
                }
                producerDoneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Consumer
        executor.submit(() -> {
            try {
                startLatch.await();
                while (running.get() || queue.size() > 0) {
                    Optional<String> item = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (item.isPresent()) {
                        consumedCount.incrementAndGet();
                    } else if (producerDoneLatch.getCount() == 0 && queue.size() == 0) {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Monitor de Tamanho do Arquivo
        AtomicLong maxFileSize = new AtomicLong(0);
        AtomicLong finalFileSize = new AtomicLong(0);
        
        startLatch.countDown();
        
        // Aguarda producer terminar
        boolean producerFinished = producerDoneLatch.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(producerFinished, "Producer timed out");

        // Aguarda consumer terminar (com timeout)
        long deadline = System.currentTimeMillis() + 30000;
        while (consumedCount.get() < itemCount && System.currentTimeMillis() < deadline) {
            File dataFile = tempDir.resolve(queueName).resolve("data.log").toFile();
            long currentSize = dataFile.length();
            maxFileSize.updateAndGet(max -> Math.max(max, currentSize));
            Thread.sleep(100);
        }

        running.set(false);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Força uma última compactação e aguarda um pouco
        Thread.sleep(500);
        
        File dataFile = tempDir.resolve(queueName).resolve("data.log").toFile();
        finalFileSize.set(dataFile.length());

        System.out.println("Max File Size: " + maxFileSize.get() + " bytes");
        System.out.println("Final File Size: " + finalFileSize.get() + " bytes");
        System.out.println("Consumed: " + consumedCount.get() + " / " + itemCount);

        boolean compactionWorked = finalFileSize.get() < (maxFileSize.get() * 0.5);
        
        Assertions.assertEquals(itemCount, consumedCount.get(), "Deveria ter consumido todos os itens");
        
        // Só valida compactação se o arquivo cresceu o suficiente para valer a pena
        if (maxFileSize.get() > 1024 * 100) { 
             assertTrue(compactionWorked, 
                String.format("Compactação falhou. Final: %d, Max: %d. O arquivo não diminuiu significativamente.", 
                finalFileSize.get(), maxFileSize.get()));
        }
    }
}
