# NGrid MVP Overview

## Configuration
- **Node identity**: Each node is described by a `NodeInfo` (node ID, host, port). IDs drive the deterministic leader election (highest ID wins).
- **Peers**: Provide an initial peer list when bootstrapping `NGridNode`. The TCP transport shares newly discovered peers to converge toward a full mesh.
- **Queue storage**: Configure the base directory and queue name used by the local `NQueue` backend in `NGridConfig`.
- **Quorum**: `ReplicationConfig` is derived from `NGridConfig.replicationQuorum`. The leader must collect acknowledgements from the configured number of replicas (including itself) before committing operations.

## Message Flow
- **Handshake** (`HANDSHAKE`): exchanged upon establishing a TCP connection. Carries the sender metadata and its known peer list.
- **Peer updates** (`PEER_UPDATE`): periodic broadcasts with the known peers to help late joiners connect to the cluster.
- **Heartbeats** (`HEARTBEAT`): emitted on the coordination layer to keep membership fresh and trigger leader re-election when a node becomes unhealthy.
- **Replication** (`REPLICATION_REQUEST` / `REPLICATION_ACK`): leader initiated operations distribute queue and map mutations, with deduplication via operation IDs.
- **Client RPC** (`CLIENT_REQUEST` / `CLIENT_RESPONSE`): followers proxy queue/map operations to the leader transparently for API consumers.

## Known Limitations
- **Leader-only reads**: `DistributedMap` and `DistributedQueue` route all mutating operations to the leader; followers rely on the leader for consistency.
- **State catch-up**: A simple in-memory log of committed operations is maintained, but full snapshotting is not yet implemented. Restarted nodes rely on replayed replication traffic and their persisted `NQueue` contents.
- **Networking**: Connections are best-effort with exponential-style retries. Long partitions are mitigated by peer gossip but not fully healed automatically.

## Future Risks & Mitigations
- **State synchronization after failure**: Promote the existing log to a durable changelog and add periodic snapshots (especially for the in-memory map) to accelerate recovery.
- **Deduplication durability**: Share replicated operation metadata with a newly elected leader to avoid reprocessing already committed operations.
- **Partial discovery**: Keep broadcasting peer lists after startup so nodes that miss the initial handshake eventually learn the full topology.
- **TCP performance**: Introduce connection pooling and backpressure; monitor timeouts and reconnect with exponential backoff.
