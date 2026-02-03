package dev.nishisan.utils.test;

import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

public class Main {
    public Main(String[] args) {
        if (args == null || args.length == 0) {
            System.err.println("Uso: java -jar ngrid-test.jar [server|client|client-auto|scenario-1]");
            return;
        }
        if (args[0].equals("server")) startServer();
        if (args[0].equals("client")) startClient();
        if (args[0].equals("client-auto")) startClientAuto();
        if (args[0].equals("scenario-1")) startScenario1();
    }

    private void startServer() {
        Path yamlFile = Paths.get("config/server-config.yml");
        try {

            try (NGridNode node = new NGridNode(yamlFile)) {
                node.start();
                System.out.println("NGrid Node iniciado com sucesso!");
                System.out.println("ID Local: " + node.transport().local().nodeId());
                System.out.println("Is Leader:" + node.coordinator().isLeader());
                node.getQueueNames().forEach(System.out::println);
                node.getMapNames().forEach(System.out::println);
                DistributedQueue<String> queue = node.getQueue("global-events", String.class);
                int index = 0;
                String epochPrefix = System.getenv("NG_MESSAGE_EPOCH");
                String prefix = epochPrefix != null && !epochPrefix.isBlank()
                        ? "INDEX-" + epochPrefix + "-"
                        : "INDEX-";

                node.coordinator().awaitLocalStability();

                while (true) {
                    String msg = prefix + index;
                    //produz aqui
                    System.out.println("Enviando:");
                    queue.offer(msg);
                    System.out.println("Enviado: " + msg);
                    index++;
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }


//                Thread.currentThread().join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startClient(){
        Path yamlFile = Paths.get("config/client-config.yml");
        try {

            try (NGridNode node = new NGridNode(yamlFile)) {
                node.start();
                System.out.println("NGrid Node iniciado com sucesso!");
                System.out.println("ID Local: " + node.transport().local().nodeId());
                System.out.println("Is Leader:" + node.coordinator().isLeader());
                node.getQueueNames().forEach(System.out::println);
                node.getMapNames().forEach(System.out::println);
                System.out.println("Seeds configurados: " + node.config().peers());
                DistributedQueue<String> queue = node.getQueue("global-events", String.class);
                node.coordinator().awaitLocalStability();
                while (true) {

                    try {
                        boolean got = queue.pollWhenAvailable(Duration.ofSeconds(5)).map(value -> {
                            System.out.println(value);
                            return true;
                        }).orElse(false);
                    } catch (IllegalStateException e) {
                        System.err.println("Poll falhou (provavel troca de lider): " + e.getMessage());
                    }

                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startClientAuto(){
        Path yamlFile = Paths.get("config/client-autodiscover.yml");
        try {

            try (NGridNode node = new NGridNode(yamlFile)) {
                node.start();
                System.out.println("NGrid Node iniciado com sucesso!");
                System.out.println("ID Local: " + node.transport().local().nodeId());
                System.out.println("Is Leader:" + node.coordinator().isLeader());
                node.getQueueNames().forEach(System.out::println);
                node.getMapNames().forEach(System.out::println);
                System.out.println("Seeds configurados: " + node.config().peers());
                DistributedQueue<String> queue = node.getQueue("global-events", String.class);
                node.coordinator().awaitLocalStability();
                while (true) {

                    try {
                        boolean got = queue.pollWhenAvailable(Duration.ofSeconds(5)).map(value -> {
                            System.out.println(value);
                            return true;
                        }).orElse(false);
                    } catch (IllegalStateException e) {
                        System.err.println("Poll falhou (provavel troca de lider): " + e.getMessage());
                    }


                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main(args);
    }

    private void startScenario1() {
        Path seedConfig = Paths.get("config/scenario-seed.yml");
        Path client1Config = Paths.get("config/scenario-client-1.yml");
        Path client2Config = Paths.get("config/scenario-client-2.yml");

        cleanupScenarioDirs();

        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicReference<NGridNode> seedRef = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Thread> seedThreadRef = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger epochRef = new java.util.concurrent.atomic.AtomicInteger(1);

        Thread seedThread = new Thread(() -> runSeed(seedConfig, running, seedRef, epochRef.get()), "ngrid-seed");
        Thread client1Thread = new Thread(() -> runClient("client-1", client1Config, running), "ngrid-client-1");
        Thread client2Thread = new Thread(() -> runClient("client-2", client2Config, running), "ngrid-client-2");
        seedThreadRef.set(seedThread);

        seedThread.start();
        client1Thread.start();
        client2Thread.start();

        java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "ngrid-scenario-control"));
        scheduler.schedule(() -> {
            NGridNode seed = seedRef.getAndSet(null);
            if (seed != null) {
                System.out.println("Parando seed para simular falha...");
                try {
                    seed.close();
                } catch (Exception e) {
                    System.err.println("Falha ao fechar seed: " + e.getMessage());
                }
            }
            Thread currentSeed = seedThreadRef.getAndSet(null);
            if (currentSeed != null) {
                currentSeed.interrupt();
            }
        }, 10, java.util.concurrent.TimeUnit.SECONDS);
        scheduler.schedule(() -> {
            System.out.println("Reiniciando seed...");
            int nextEpoch = epochRef.incrementAndGet();
            Thread restart = new Thread(() -> runSeed(seedConfig, running, seedRef, nextEpoch),
                    "ngrid-seed-restart");
            seedThreadRef.set(restart);
            restart.start();
        }, 15, java.util.concurrent.TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            scheduler.shutdownNow();
            NGridNode seed = seedRef.getAndSet(null);
            if (seed != null) {
                try {
                    seed.close();
                } catch (Exception ignored) {
                }
            }
        }, "ngrid-scenario-shutdown"));

        try {
            long timeoutAt = System.currentTimeMillis() + 120_000;
            while (running.get() && System.currentTimeMillis() < timeoutAt) {
                Thread.sleep(200);
            }
            running.set(false);
            scheduler.shutdownNow();
            NGridNode seed = seedRef.getAndSet(null);
            if (seed != null) {
                try {
                    seed.close();
                } catch (Exception ignored) {
                }
            }
            Thread currentSeed = seedThreadRef.getAndSet(null);
            if (currentSeed != null) {
                currentSeed.interrupt();
            }
            client1Thread.interrupt();
            client2Thread.interrupt();
            seedThread.join(10_000);
            client1Thread.join(10_000);
            client2Thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runSeed(Path yamlFile,
            java.util.concurrent.atomic.AtomicBoolean running,
            java.util.concurrent.atomic.AtomicReference<NGridNode> seedRef,
            int epoch) {
        try (NGridNode node = new NGridNode(yamlFile)) {
            node.start();
            seedRef.set(node);
            System.out.println("Seed iniciado: " + node.transport().local().nodeId() + " (epoch=" + epoch + ")");
            DistributedQueue<String> queue = node.getQueue("global-events", String.class);
            node.coordinator().awaitLocalStability();
            int index = 0;
            while (running.get()) {
                String msg = "INDEX-" + epoch + "-" + index;
                queue.offer(msg);
                index++;
                if (index >= 60) {
                    running.set(false);
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Exception e) {
            running.set(false);
            System.err.println("Seed falhou: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void runClient(String name, Path yamlFile, java.util.concurrent.atomic.AtomicBoolean running) {
        try (NGridNode node = new NGridNode(yamlFile)) {
            node.start();
            System.out.println(name + " iniciado: " + node.transport().local().nodeId());
            DistributedQueue<String> queue = node.getQueue("global-events", String.class);
            node.coordinator().awaitLocalStability();
            final java.util.Set<String> seen = new java.util.HashSet<>();
            final java.util.concurrent.atomic.AtomicInteger currentEpoch =
                    new java.util.concurrent.atomic.AtomicInteger(-1);
            while (running.get()) {
                try {
                    queue.pollWhenAvailable(Duration.ofSeconds(2))
                            .ifPresent(value -> {
                                int epoch = parseEpoch(value);
                                if (epoch >= 0) {
                                    int previousEpoch = currentEpoch.get();
                                    if (epoch > previousEpoch) {
                                        currentEpoch.set(epoch);
                                        seen.clear();
                                    } else if (epoch < previousEpoch) {
                                        System.out.println(name + " recebeu-epoch-antigo: " + value);
                                        return;
                                    }
                                    if (!seen.add(value)) {
                                        System.out.println(name + " recebeu-duplicado: " + value);
                                        return;
                                    }
                                }
                                System.out.println(name + " recebeu: " + value);
                            });
                } catch (IllegalStateException e) {
                    System.err.println(name + " poll falhou: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            running.set(false);
            System.err.println(name + " falhou: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private int parseEpoch(String value) {
        if (value == null || !value.startsWith("INDEX-")) {
            return -1;
        }
        int firstDash = value.indexOf('-', 6);
        if (firstDash <= 6) {
            return -1;
        }
        try {
            return Integer.parseInt(value.substring(6, firstDash));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void cleanupScenarioDirs() {
        deleteRecursively(Paths.get("/tmp/ngrid-scenario/seed"));
        deleteRecursively(Paths.get("/tmp/ngrid-scenario/client-1"));
        deleteRecursively(Paths.get("/tmp/ngrid-scenario/client-2"));
    }

    private void deleteRecursively(Path path) {
        try {
            if (!java.nio.file.Files.exists(path)) {
                return;
            }
            java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.deleteIfExists(p);
                        } catch (Exception e) {
                            System.err.println("Falha ao remover " + p + ": " + e.getMessage());
                        }
                    });
        } catch (Exception e) {
            System.err.println("Falha ao limpar " + path + ": " + e.getMessage());
        }
    }
}
