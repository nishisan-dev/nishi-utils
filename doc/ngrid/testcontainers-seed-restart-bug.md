# Bug Note: Seed Stops Between Restart Cycles (Testcontainers)

## Summary

While running the multi-restart Testcontainers scenario, the test failed with:

```
Some containers are not running (before kill (cycle=2)).
- seed not running
```

This looked like the seed was crashing. It turned out to be a **test bug**.

## Root Cause

The test used a `try-with-resources` block for the restarted seed inside the loop:

```
try (GenericContainer<?> seedRestarted = createSeedContainer(...)) {
  seedRestarted.start();
  ...
}
```

At the end of each cycle, `seedRestarted` was closed automatically.  
The next cycle tried to kill the old seed reference, which was already stopped.

## Fix

Keep a `currentSeed` reference across cycles and avoid `try-with-resources` inside the loop:

- Kill `currentSeed`
- Create and start a new seed
- Assign it back to `currentSeed`
- Close `currentSeed` once at the end

## History / What Was Done And Why

- Added a **Testcontainers-based test harness** to run multi-node NGrid locally.
  - Goal: reproduce crash/failover issues in a realistic environment.
- Expanded the test to **3 nodes**, then to **6 nodes** (seed + 5 clients).
  - Goal: stress quorum/replication with more fan-out.
- Implemented **crash-hard restart** (SIGKILL) to simulate real node failure.
  - Goal: validate durability guarantees under abrupt termination.
- Added validation for:
  - **No duplicates per client**
  - **No message loss within an observed epoch**
  - **Clients continue receiving after restart**
- Introduced **epoch prefix** (`NG_MESSAGE_EPOCH`) for messages.
  - Reason: avoid false positives when comparing before/after restart.
- Found duplicate delivery after crash hard; traced to offsets not being durably persisted.
  - Attempted config-only persistence; YAML `maps.persistence` was ignored.
- Fixed **NGridConfigLoader** to read `maps[*].persistence` into `mapPersistenceMode`.
  - Reason: ensure `_ngrid-queue-offsets` can actually persist.
- Still saw duplicates; implemented **sync WAL writes** for offsets:
  - Added `appendSync(...)` to `MapPersistence`.
  - Used it for `_ngrid-queue-offsets` in `MapClusterService`.
  - Reason: crash-hard requires fsync before acknowledging offset.
- Duplicates stopped after that change.
- Introduced container liveness checks and logs on failure.
  - Goal: diagnose "seed not running" quickly.
- Discovered the seed-stop issue was **test logic**, not system stability.
  - Root cause: `try-with-resources` closed the restarted seed after each cycle.
  - Fixed by keeping a `currentSeed` reference across cycles.
