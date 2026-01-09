# NGrid Evolution: Mesh-Based Routed Replication

## Context
Currently, NGrid utilizes a "Star" topology for replication. The Leader node maintains direct connections to all Followers and broadcasts replication messages (`REPLICATION_REQUEST`) directly to each one via `ReplicationManager`.

While simple and low-latency for local networks, this approach has limitations:
1.  **Single Point of Failure (Link):** If the direct TCP connection between Leader and Follower A breaks, Follower A stops receiving updates, even if it is still reachable via Follower B.
2.  **Bandwidth Bottleneck:** The Leader's outbound bandwidth scales linearly with the cluster size ($O(N)$), as it must send a copy of the payload to every node.

## Proposal: Full Mesh / Routed Replication
The goal is to leverage the existing `NetworkRouter` capabilities to allow the cluster to act as a mesh. If the Leader cannot reach Follower A directly, it should be able to route the replication message through an intermediary node (Proxy).

### Architecture Phases

#### Phase 1: Resilience (Smart Fallback) - **IMPLEMENTED**
**Goal:** Use the mesh topology primarily for recovery when direct links fail.

*   **Mechanism:**
    *   The `ReplicationManager` (or `Transport` layer) checks connectivity before sending.
    *   If `isConnected(Target)` is false, query `NetworkRouter.nextHop(Target)`.
    *   If a proxy path exists (e.g., Leader -> Proxy -> Target), encapsulate the replication message and send it to the Proxy.
    *   The Proxy unwraps the message and forwards it to the Target.
*   **Benefits:** Drastically increases cluster stability in flaky network environments.
*   **Risks:**
    *   **Message Ordering:** Critical. If message $T_1$ is sent directly (stalls) and $T_2$ is sent via proxy (fast), $T_2$ might arrive before $T_1$.
    *   *Mitigation:* The `ReplicationManager` already handles ordering via WAL/Log Index, but the transport layer must ensure causal delivery or the application must handle out-of-order retries gracefully.

#### Phase 2: Optimization (Cost-Based Routing)
**Goal:** Optimize data flow based on network metrics (RTT, Throughput).

*   **Mechanism:**
    *   Integrate `RttMonitor` metrics into `NetworkRouter`.
    *   Use a pathfinding algorithm (e.g., simplified Dijkstra) to find the "lowest cost" path.
    *   **Scenario:** In a multi-region setup (Region A, Region B), the Leader in Region A sends *one* message to a "Bridge Node" in Region B. The Bridge Node then redistributes it to other nodes in Region B (Gossip/Multicast tree style).
*   **Benefits:** Reduces Leader bandwidth usage; optimizes cross-region latency.

## Implementation Plan (Draft)

### 1. Message Encapsulation
We need a new message type or a wrapper to support multi-hop routing without deserializing the payload at every hop.

```java
// Concept
class RoutedMessagePayload implements Serializable {
    NodeId finalDestination;
    NodeId originalSource;
    ClusterMessage innerMessage; // Or byte[] rawPayload
}
```

### 2. Router Integration in ReplicationManager
Refactor the broadcast loop in `ReplicationManager`:

```java
// Current
for (NodeInfo member : members) {
    transport.send(messageTo(member));
}

// Proposed
for (NodeInfo member : members) {
    NodeId target = member.nodeId();
    Optional<NodeId> nextHop = networkRouter.nextHop(target);
    
    if (nextHop.isPresent()) {
        if (nextHop.get().equals(target)) {
            // Direct
            transport.send(messageTo(target));
        } else {
            // Via Proxy
            transport.send(wrapForProxy(messageTo(target), nextHop.get()));
        }
    }
}
```

### 3. Loop Prevention
The `NetworkRouter` must ensure no routing loops exist (A -> B -> A). The existing "Sticky Fallback" or a standard TTL (Time To Live) field in the `RoutedMessage` can prevent infinite bouncing.
