package dev.nishisan.utils.test;

import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    public Main(String[] args) {
        if (args == null || args.length == 0) {
            System.err.println("Uso: java -jar ngrid-test.jar [server|client|client-auto]");
            return;
        }
        if (args[0].equals("server")) startServer();
        if (args[0].equals("client")) startClient();
        if (args[0].equals("client-auto")) startClientAuto();
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

                node.coordinator().awaitLocalStability();

                while (true) {
                    String msg= "INDEX-" + index;
                    //produz aqui
                    System.out.println("Enviando:");
                    queue.offer(msg);
                    System.out.println("Enviado: " + msg);
                    index++;
                    try {
                        Thread.sleep(1000);
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
                DistributedQueue<String> queue = node.getQueue("global-events", String.class);
                while (true) {

                    queue.poll().ifPresent(System.out::println);
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
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
                DistributedQueue<String> queue = node.getQueue("global-events", String.class);
                while (true) {

                    queue.poll().ifPresent(System.out::println);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
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
}
