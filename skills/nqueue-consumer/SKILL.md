---
name: nqueue-consumer
description: Use this skill when integrating NQueue into another project without copying source code, including dependency setup, option tuning, usage patterns, and validation checklist.
---

# NQueue Consumer

Use this skill to guide integration of `dev.nishisan.utils.queue.NQueue` as a library dependency in external projects.

## What this skill does

- Applies the recommended integration path (dependency + API usage).
- Selects an options profile based on reliability/latency goals.
- Prevents common misuses (durability and retention assumptions).
- Provides a minimal validation checklist.

## Required workflow

1. Read `../../doc/nqueue-agent-guide.md` first.
2. If you need implementation details, read only one of:
- `../../doc/nqueue-readme.md` for architecture/options behavior.
- `../../doc/nqueue-examples.md` for copyable recipes.
3. Generate integration code using `NQueue.open(...)`, `offer`, `poll`, `peek`, and `try-with-resources`.
4. Add a restart test for critical flows.

## Decision rules for options

- If user prioritizes durability: prefer `withFsync(true)`.
- If user prioritizes throughput: consider `withFsync(false)` and explain crash-loss risk.
- If replay window is needed: use `RetentionPolicy.TIME_BASED` + `withRetentionTime(...)`.
- If strict persist-before-deliver is required: set `withShortCircuit(false)`.
- If investigating ordering anomalies: enable `withOrderDetection(true)` temporarily.

## Output contract

When answering integration requests with this skill, include:

- dependency snippet;
- queue initialization snippet;
- recommended options for the stated goal;
- one short test checklist (including restart scenario).

