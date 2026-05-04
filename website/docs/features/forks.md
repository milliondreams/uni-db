---
title: Forks
status: phase-1
---

# Forks

Forks are **named, durable, isolated branches** of the graph. A fork lets a session reason about an alternate version of the database — for "what if" analyses, regulatory scenarios, write-audit-publish workflows, or simply for an inspectable counterfactual that survives across restarts.

Forks are a sibling of [snapshots](snapshots-time-travel.md). Where a snapshot is a read-only point-in-time view, a fork is a *named*, *durable*, and (in Phase 2) *writable* parallel timeline.

## Status

Phase 1: **read-only forks**. Reads on a forked session see the database as of the fork-point. Writes through `forked.tx()` return `UniError::ForkWritesNotYetSupported` until Phase 2 lifts the gate.

Later phases land:

- **Phase 2** — fork-local writes via `forked.tx()`. Same Cypher / Locy as primary; commits land on the fork's Lance branches.
- **Phase 3** — nested forks (`forked.fork(name)`).
- **Phase 4** — TTL, tags, watch filtering, hooks, params, version pinning on a forked session.
- **Phase 5** — fork-local index fusion.
- **Phase 6** — diff and promotion.

## Quick start

```rust
use uni_db::Uni;

let db = Uni::in_memory().build().await?;
db.schema()
    .label("Person")
    .property("name", uni_db::DataType::String)
    .apply()
    .await?;

let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Alice'})").await?;
tx.commit().await?;

// Take a fork at the current state.
let scenario = session.fork("scenario_1").await?;

// Mutate primary further.
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob'})").await?;
tx.commit().await?;

// Primary sees both; fork still sees only Alice.
let primary_rows = session
    .query("MATCH (p:Person) RETURN p.name")
    .await?
    .rows()
    .len();
assert_eq!(primary_rows, 2);

let fork_rows = scenario
    .query("MATCH (p:Person) RETURN p.name")
    .await?
    .rows()
    .len();
assert_eq!(fork_rows, 1);
```

## API

### Session-level

| Method | Description |
|---|---|
| `Session::fork(name)` | Open or create a fork. Returns a `ForkBuilder`; `.await` for open-or-create. |
| `ForkBuilder::new_()` | Require a fresh fork. Errors with `ForkAlreadyExists` if the name is taken. |
| `Session::is_forked()` | `true` when this session was returned by `fork`. |

### Database-level admin

| Method | Description |
|---|---|
| `Uni::list_forks()` | All Active forks. |
| `Uni::fork_info(name)` | Metadata for a single fork. |
| `Uni::drop_fork(name)` | Full 2PC drop. |

### Errors

All fork-related errors are `UniError::Fork*` variants — `ForkNotFound`, `ForkAlreadyExists`, `ForkWritesNotYetSupported`, `ForkInUse { name, holder_count }`, `ForkCorruptRegistry`, `ForkLifecycle { name, stage, source }`.

## Snapshot vs Fork

| | Snapshot | Fork |
|---|---|---|
| Identity | Snapshot id (content) | Name (user-chosen) |
| Mutable | No | Yes (Phase 2) |
| Survives restart | Yes | Yes |
| Used for | Time-travel reads | What-if scenarios, audit, sandbox |
| API | `session.pin_to_version` | `session.fork(name)` |

## Storage layout

A fork is one Lance branch per dataset (vertex, edge-delta, adjacency). Reads chain to the parent via Lance's `base_paths` resolution. Primary writes after the fork-point are invisible to the fork; fork writes never appear on primary.

On disk:

- `catalog/fork_registry.json` — the registry of all forks.
- `catalog/fork_schemas/{fork_id}.json` — per-fork schema overlay (empty in Phase 1).
- `catalog/fork_tombstones/{fork_id}.json` — durable drop intent, removed on completion.

## Concurrency and isolation

- **Fork creation does not block primary** (spec §10). Reads and writes on primary continue at full throughput while a fork is being created.
- **Different forks proceed in parallel.** Same-name open-or-create serializes via a per-name async mutex.
- **Multiple sessions can hold the same fork.** A holder count is tracked and `drop_fork` refuses with `ForkInUse` while sessions are alive.
- **Lance compaction honors branch references.** Primary GC will not reclaim fragments that a live fork still references.

## Crash recovery

Recovery runs in `Uni::open` before any session is exposed.

- **Pending fork** (create crashed before completion) → rolled back. Branches force-deleted via the missing-branch-tolerant `lance_branch::delete_branch` wrapper.
- **Tombstoned fork** (drop crashed mid-2PC) → completed. Branches deleted, registry entry removed, tombstone + overlay files cleaned.

Recovery is idempotent — running it twice is a no-op the second time.
