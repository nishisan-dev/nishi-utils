package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Transport abstraction providing basic messaging primitives between nodes.
 */
public interface Transport extends Closeable {
    void start();

    NodeInfo local();

    Collection<NodeInfo> peers();

    void addListener(TransportListener listener);

    void removeListener(TransportListener listener);

    void broadcast(ClusterMessage message);

    void send(ClusterMessage message);

    CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message);

    boolean isConnected(NodeId nodeId);
}
