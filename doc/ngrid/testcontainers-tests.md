# Testcontainers NGrid Tests

This document explains the Testcontainers-based integration tests for NGrid.

## Purpose

These tests spin up multiple NGrid nodes as Docker containers to validate:

- Multi-node communication.
- Message delivery in distributed queues.
- No duplicate messages per client.
- No message loss across a seed restart.

They are designed to be realistic and repeatable without manual cluster setup.

## Tests

### `shouldDeliverMessagesAcrossNodes`

Creates three containers:

- `seed` running the `server` command.
- `client-1` and `client-2` running the `client` command.

It waits for at least one client to observe messages with the `INDEX-` prefix.
If no messages are received, it prints a short tail of logs for diagnosis.
It also checks each client log for duplicate `INDEX-<epoch>-<n>` entries.

### `shouldRecoverAfterSeedRestartWithoutDuplicatesOrLoss`

Steps:

1. Start seed + two clients.
2. Wait for messages from epoch `1` (`INDEX-1-<n>`).
3. Stop the seed container.
4. Restart the seed using the same data directory, but with epoch `2`.
5. Wait for messages from epoch `2`.
6. Validate:
   - No duplicate messages per client.
   - No missing indices within each observed epoch.

This validates resilience across leader restart while preserving ordering
and delivery guarantees per client.

## Message Format

The seed emits messages like:

```
INDEX-<epoch>-<n>
```

The epoch is injected via the `NG_MESSAGE_EPOCH` environment variable when the
seed container starts. This prevents collisions across restarts.

## Data Persistence

Each container binds a unique host temp directory to `/data/...` inside the
container. This ensures:

- Seed state persists across restart in the failover test.
- Client offsets persist within a test run.

## Running

Use Maven to run the tests:

```bash
mvn test -Dtest=NGridTestcontainersSmokeTest
```

Docker must be running locally.

---

## Durability Tests

The `NGridDurabilityTest` class focuses on validating **crash recovery** scenarios using `docker kill` (SIGKILL) to simulate abrupt failures.

### Tests

| Test | Validates |
|------|-----------|
| `shouldRecoverOffsetAfterCrashAndNotReceiveDuplicates` | Client offset durability - no duplicates after client crash |
| `shouldRecoverMessagesAfterSeedCrash` | Queue durability - messages available after seed crash |
| `shouldMaintainIndependentOffsetsAfterMultiClientCrash` | Multi-client independence - each client recovers its own offset |

### Crash Simulation

Uses Docker's kill command to send SIGKILL:

```java
container.getDockerClient()
    .killContainerCmd(container.getContainerId())
    .exec();
```

This simulates a hard crash (power failure, OOM kill, etc.) without graceful shutdown.

### What is Validated

- **Queue (`global-events`)**: Messages survive seed crash
- **Map (`_ngrid-queue-offsets`)**: Consumer offsets survive client crash (prevents duplicates)

### Running

```bash
mvn test -Dtest=NGridDurabilityTest
```

