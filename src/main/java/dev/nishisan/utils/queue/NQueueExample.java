/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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

package dev.nishisan.utils.queue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class NQueueExample {

    private NQueueExample() {
    }

    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp");

        try (NQueue<String> queue = NQueue.open(baseDir, "demo")) {
            ExecutorService executor = Executors.newFixedThreadPool(2);

            executor.submit(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        String message = "message-" + i;
                        queue.offer(message);
                        System.out.println("Produced: " + message + " Size:" + queue.size());
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
                        queue.poll().ifPresent(message -> System.out.println("Consumed: " + message));
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

}
