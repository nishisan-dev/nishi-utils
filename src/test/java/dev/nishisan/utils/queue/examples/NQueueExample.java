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

package dev.nishisan.utils.queue.examples;

import dev.nishisan.utils.queue.NQueue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class NQueueExample {

    private NQueueExample() {
    }

    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp");
        NQueue.Options options = NQueue
                .Options
                .defaults()
                .withFsync(false);

        try (NQueue<String> queue = NQueue.open(baseDir, "demo", options)) {
            System.out.println("Initial Size:" + queue.size());
            ExecutorService executor = Executors.newFixedThreadPool(2);
            AtomicLong totalOffered = new AtomicLong(0);
            AtomicLong totalConsumed = new AtomicLong(0);
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 5000; i++) {
                        String message = "message-" + i;
                        queue.offer(message);
                        totalOffered.incrementAndGet();
//                        System.out.println("Produced: " + message + " Size:" + queue.size());
//                        Thread.sleep(150);
                    }
                    System.out.println("Offered Size:" + queue.size());
                } catch (IOException e) {
                    System.err.println("Failed to offer message: " + e.getMessage());
                }
            });

            executor.submit(() -> {
                try {
                    Thread.sleep(1000);
                    while (!queue.isEmpty()) {
                        queue.poll();
                        totalConsumed.incrementAndGet();
                    }
                    System.out.println("Total Offered: " + totalOffered.get());
                    System.out.println("Total Consumed: " + totalConsumed.get());
                } catch (IOException e) {
                    System.err.println("Failed to poll message: " + e.getMessage());
                } catch (InterruptedException e) {

                }
            });

            executor.shutdown();
//            System.out.println("Final Size:" + queue.size());


            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                System.out.println("Final Size:" + queue.size());
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate cleanly.");
                }
            }
        }
    }

}
