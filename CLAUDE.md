# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build without tests
mvn clean install -DskipTests

# Unit tests (baseline)
mvn test

# Single test class / method
mvn test -Dtest=NQueueTest
mvn test -Dtest=NQueueTest#testMethodName

# Module-specific build
mvn -pl nishi-utils-core clean install

# Resilience tests (in-process cluster simulation)
mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1

# Docker resilience tests (requires Testcontainers)
mvn verify -Pdocker-resilience

# Soak test (long-running)
mvn test -Psoak -Dngrid.soak.durationMinutes=720

# Javadoc validation
mvn verify -Pvalidate-javadoc

# Coverage report (JaCoCo) — excludes ngrid packages
mvn verify -pl nishi-utils-core

# Security scan (OWASP Dependency Check)
mvn verify -Psecurity-scan -DskipTests
```

**Toolchain note:** POMs and Dockerfile compile with Java 25, though README says Java 21+. If you see compilation errors, check Java version first.

## Repository Structure

Multi-module Maven monorepo:
- **nishi-utils-core** — the library (production code + tests). Only this module is published.
- **ngrid-test** — support module for Docker-based integration tests and manual scenarios. Not a release artifact.

## Architecture Overview

Four main components in `dev.nishisan.utils`:

1. **NMap** (`map/`) — Thread-safe persistent map with WAL+snapshot and pluggable offload strategies (in-memory, disk, hybrid LRU). Standalone, no cluster dependency.

2. **NQueue** (`queue/`) — File-backed persistent FIFO queue with automatic compaction and in-memory staging (`MemoryStager`) for burst absorption. Standalone.

3. **NGrid** (`ngrid/`) — Distributed TCP cluster providing `DistributedQueue` and `DistributedMap`. Leader-based writes with quorum replication. Uses NQueue/NMap as local backends.

4. **Stats** (`stats/`) — Simple metrics utilities (counters, averages, gauges) via `StatsUtils`. Used by other components for observability.

### NGrid Internal Architecture

`NGridNode` is the central integration point wiring together:
- **Transport** (`cluster/transport/`) — TCP with handshake, gossip, RTT-based proxy routing
- **ClusterCoordinator** (`cluster/coordination/`) — Membership, leader election, heartbeats
- **ReplicationManager** (`replication/`) — Quorum-based replication with sequence tracking
- **QueueClusterService** / **MapClusterService** (`queue/`, `map/`) — Per-structure distributed backends
- **Codec** (`cluster/transport/codec/`) — `CompositeMessageCodec` dispatches between binary frames (HEARTBEAT/PING) and Jackson JSON. First byte determines path (`0x01`/`0x02` = binary, `0x00` = JSON with marker, `0x7B` = legacy JSON).

**Write flow:** Client → any node → forwarded to leader via `CLIENT_REQUEST` → leader applies locally + replicates via `REPLICATION_REQUEST` → awaits quorum ACKs → returns result.

**Facade:** `NGrid.local(n)` for in-memory test clusters, `NGrid.node(host, port)` for production. Prefer facade over raw `NGridConfig.Builder` in new code/tests.

### Key Design Patterns

- Multi-tenant by name: commands include qualifier (`queue.offer:{queue}`, `map.put:{map}`)
- Consumer-first: prefer `DistributedQueue.openConsumer(groupId, consumerId)` over legacy `poll()`
- `TypedQueue<T>` descriptors for compile-time type-safe queue access
- YAML config with `${VAR}` / `${VAR:default}` interpolation and autodiscovery
- `DeploymentProfile` (DEV/STAGING/PRODUCTION) — PRODUCTION enforces guardrails

## Testing Conventions

- Standard unit tests run with `mvn test`. Resilience profile (`-Presilience`) includes in-process cluster tests.
- In-process cluster tests in core use `*Test.java` naming (not `*IT.java`). Docker tests in `ngrid-test` use `*IT.java` with Failsafe.
- Use `ClusterTestUtils.awaitClusterConsensus(...)` to stabilize cluster state in tests.
- Docker tests depend on log markers (`CURRENT_LEADER_STATUS`, `ACTIVE_MEMBERS_COUNT`, `REACHABLE_NODES_COUNT`) emitted by `ngrid-test/.../Main.java` — don't rename these without updating `NGridNodeContainer`/`NGridMapNodeContainer`.
- For new tests with the facade, prefer `NGrid.local(n)` over manual `NGridConfig.Builder` setup.
- Register distributed maps on participating nodes before traffic to avoid handler-missing errors (see `UnknownMapRequestHandler`).
- Surefire runs with `forkCount=1`, `reuseForks=false`.

## Project Language

Documentation (`doc/`, README, AGENTS.md) is written in Brazilian Portuguese (pt-BR). Code, APIs, and variable names are in English.

## License

GNU GPL v3 (or later).
