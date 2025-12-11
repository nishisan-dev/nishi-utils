package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransportConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High level component that ties together transport, coordination, replication and data
 * structures for a single node instance.
 */
public final class NGridNode implements Closeable {
    private final NGridConfig config;

    private Transport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager replicationManager;
    private QueueClusterService<Serializable> queueService;
    private MapClusterService<Serializable, Serializable> mapService;
    private DistributedQueue<Serializable> queue;
    private DistributedMap<Serializable, Serializable> map;
    private ScheduledExecutorService coordinatorScheduler;
    private final AtomicBoolean started = new AtomicBoolean();

    public NGridNode(NGridConfig config) {
        this.config = config;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        TcpTransportConfig.Builder transportBuilder = TcpTransportConfig.builder(config.local())
                .connectTimeout(Duration.ofSeconds(5))
                .reconnectInterval(Duration.ofSeconds(2));
        config.peers().forEach(transportBuilder::addPeer);
        transport = new TcpTransport(transportBuilder.build());
        transport.start();

        coordinatorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ngrid-coordinator");
            t.setDaemon(true);
            return t;
        });
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), coordinatorScheduler);
        coordinator.start();

        replicationManager = new ReplicationManager(transport, coordinator, ReplicationConfig.of(config.replicationQuorum()));
        replicationManager.start();

        queueService = new QueueClusterService<>(config.queueDirectory(), config.queueName(), replicationManager);
        mapService = new MapClusterService<>(replicationManager);
        queue = new DistributedQueue<>(transport, coordinator, queueService);
        map = new DistributedMap<>(transport, coordinator, mapService);
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> queue(Class<T> type) {
        return (DistributedQueue<T>) queue;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> map(Class<K> keyType, Class<V> valueType) {
        return (DistributedMap<K, V>) map;
    }

    public Transport transport() {
        return transport;
    }

    public ClusterCoordinator coordinator() {
        return coordinator;
    }

    public ReplicationManager replicationManager() {
        return replicationManager;
    }

    @Override
    public void close() throws IOException {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (queue != null) {
            queue.close();
        }
        if (map != null) {
            map.close();
        }
        if (replicationManager != null) {
            replicationManager.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
        if (coordinatorScheduler != null) {
            coordinatorScheduler.shutdownNow();
        }
        if (transport != null) {
            transport.close();
        }
    }
}
