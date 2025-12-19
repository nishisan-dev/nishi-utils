# NQueue - Exemplos de uso

Este guia complementa `doc/nqueue-readme.md` com cenarios praticos e exemplos prontos para copiar.

## 1) Produtor/consumidor simples

```java
import dev.nishisan.utils.queue.NQueue;
import java.nio.file.Path;
import java.util.Optional;

public class BasicExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        try (NQueue<String> queue = NQueue.open(baseDir, "basic")) {
            queue.offer("a");
            queue.offer("b");

            Optional<String> first = queue.poll();
            Optional<String> second = queue.poll();

            System.out.println(first.orElse("(vazio)"));
            System.out.println(second.orElse("(vazio)"));
        }
    }
}
```

## 2) Poll bloqueante com timeout

```java
import dev.nishisan.utils.queue.NQueue;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TimeoutExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        try (NQueue<String> queue = NQueue.open(baseDir, "timeout")) {
            Optional<String> item = queue.poll(3, TimeUnit.SECONDS);
            System.out.println(item.orElse("nenhum item"));
        }
    }
}
```

## 3) Durabilidade estrita (sem short-circuit)

```java
import dev.nishisan.utils.queue.NQueue;
import java.nio.file.Path;

public class DurableExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        NQueue.Options options = NQueue.Options.defaults()
            .withShortCircuit(false)
            .withFsync(true);

        try (NQueue<String> queue = NQueue.open(baseDir, "durable", options)) {
            queue.offer("persistente");
            System.out.println(queue.poll().orElseThrow());
        }
    }
}
```

## 4) Buffer de memoria para picos

```java
import dev.nishisan.utils.queue.NQueue;
import java.nio.file.Path;

public class MemoryBufferExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        NQueue.Options options = NQueue.Options.defaults()
            .withMemoryBuffer(true)
            .withMemoryBufferSize(2000)
            .withFsync(false);

        try (NQueue<Integer> queue = NQueue.open(baseDir, "burst", options)) {
            for (int i = 0; i < 1000; i++) {
                queue.offer(i);
            }
            while (!queue.isEmpty()) {
                queue.poll();
            }
        }
    }
}
```

## 5) Inspecao de registros e offsets

```java
import dev.nishisan.utils.queue.NQueue;
import dev.nishisan.utils.queue.NQueueReadResult;
import dev.nishisan.utils.queue.NQueueRecord;
import java.nio.file.Path;

public class InspectExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        try (NQueue<String> queue = NQueue.open(baseDir, "inspect")) {
            long off1 = queue.offer("a");
            queue.offer("b");

            NQueueRecord head = queue.peekRecord().orElseThrow();
            System.out.println("index=" + head.meta().getIndex());
            System.out.println("class=" + head.meta().getClassName());

            if (off1 >= 0) {
                NQueueReadResult read = queue.readRecordAt(off1).orElseThrow();
                System.out.println("nextOffset=" + read.getNextOffset());
            }
        }
    }
}
```

## 6) Monitorando metricas

```java
import dev.nishisan.utils.queue.NQueue;
import dev.nishisan.utils.queue.NQueueMetrics;
import java.nio.file.Path;

public class MetricsExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");

        try (NQueue<String> queue = NQueue.open(baseDir, "metrics")) {
            queue.offer("x");
            queue.poll();

            Long offered = queue.getStats().getCounterValueOrNull(NQueueMetrics.OFFERED_EVENT);
            Long polled = queue.getStats().getCounterValueOrNull(NQueueMetrics.POLL_EVENT);
            Long violations = queue.getStats().getCounterValueOrNull("nqueue.out_of_order");

            System.out.println("offered=" + offered);
            System.out.println("polled=" + polled);
            System.out.println("outOfOrder=" + violations);
        }
    }
}
```

## 7) Uso com multiplos produtores e consumidores

```java
import dev.nishisan.utils.queue.NQueue;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyExample {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of("/tmp/queues");
        ExecutorService exec = Executors.newFixedThreadPool(4);
        AtomicInteger consumed = new AtomicInteger(0);

        try (NQueue<Integer> queue = NQueue.open(baseDir, "concurrent")) {
            exec.submit(() -> {
                try {
                    for (int i = 0; i < 100; i++) queue.offer(i);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            exec.submit(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        queue.poll(5, TimeUnit.SECONDS).ifPresent(v -> consumed.incrementAndGet());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            exec.shutdown();
            exec.awaitTermination(30, TimeUnit.SECONDS);
        }

        System.out.println("consumed=" + consumed.get());
    }
}
```

