package dev.nishisan.utils.queue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class NQueueExample {

    private NQueueExample() {
    }

    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("nqueue-example");

        try (NQueue<String> queue = NQueue.open(baseDir, "demo")) {
            ExecutorService executor = Executors.newFixedThreadPool(2);

            executor.submit(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        String message = "message-" + i;
                        queue.offer(message);
                        System.out.println("Produced: " + message);
                        Thread.sleep(150);
                    }
                } catch (IOException e) {
                    System.err.println("Failed to offer message: " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            executor.submit(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        queue.poll().ifPresent(record -> {
                            String message = deserialize(record.payload());
                            System.out.println("Consumed: " + message);
                        });
                    }
                } catch (IOException e) {
                    System.err.println("Failed to poll message: " + e.getMessage());
                }
            });

            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate cleanly.");
                }
            }
        }
    }

    private static String deserialize(byte[] payload) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (String) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to deserialize payload", e);
        }
    }
}
