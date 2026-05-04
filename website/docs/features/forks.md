---
title: Forks
status: phase-2
---

# Forks

Forks are **named, durable, isolated branches** of the graph. A fork lets a session reason about an alternate version of the database — for "what if" analyses, regulatory scenarios, write-audit-publish workflows, or simply for an inspectable counterfactual that survives across restarts.

Forks are a sibling of [snapshots](snapshots-time-travel.md). Where a snapshot is a read-only point-in-time view, a fork is a *named*, *durable*, *writable* parallel timeline.

## Status

Phase 2: **writable forks**. `forked.tx().execute(...).commit()` lands mutations on the fork's Lance branches without touching primary. New labels and edge types created on a fork stay fork-local; primary's schema is unchanged.

Later phases land:

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

// Write through the fork — lands on the fork's branch only.
let tx = scenario.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob-on-fork'})").await?;
tx.commit().await?;

// Primary sees only Alice; fork sees Alice + Bob-on-fork.
assert_eq!(
    session.query("MATCH (p:Person) RETURN p.name").await?.rows().len(),
    1
);
assert_eq!(
    scenario.query("MATCH (p:Person) RETURN p.name").await?.rows().len(),
    2
);
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

### Fork-local schema additions

`Session::fork_schema()` mirrors `db.schema()` but lands new labels and edge types in the fork's persisted overlay (`catalog/fork_schemas/{fork_id}.json`) and the fork's in-memory `SchemaManager`. Primary's `catalog/schema.json` is **never** touched.

```rust
let forked = session.fork("scenario_1").await?;
forked
    .fork_schema()
    .label("OnlyOnFork")
    .edge_type("ONLY_ON_FORK", &["Item"], &["Item"])
    .apply()
    .await?;

// Strict-schema mode now lets the fork CREATE new fork-only entities:
let tx = forked.tx().await?;
tx.execute("CREATE (:OnlyOnFork)").await?;
tx.commit().await?;
```

Required only under `UniConfig { strict_schema: true, .. }`. In schemaless mode (the default) `BranchedBackend` materializes the dataset+branch on the fly without a schema entry; calling `fork_schema()` is harmless but unnecessary.

`apply()` errors with `UniError::InvalidArgument` on a non-forked session.

### Errors

All fork-related errors are `UniError::Fork*` variants — `ForkNotFound`, `ForkAlreadyExists`, `ForkInUse { name, holder_count }`, `ForkInflightTx { name }`, `ForkCorruptRegistry`, `ForkLifecycle { name, stage, source }`.

`ForkInflightTx` fires when `drop_fork` is called while at least one `Transaction` is alive on the fork. Commit or roll back the transaction first, then retry the drop.

`ForkWritesNotYetSupported` is retired in Phase 2 — `forked.tx()` is now writable.

## Snapshot vs Fork

| | Snapshot | Fork |
|---|---|---|
| Identity | Snapshot id (content) | Name (user-chosen) |
| Mutable | No | Yes |
| Survives restart | Yes | Yes |
| Used for | Time-travel reads | What-if scenarios, audit, sandbox |
| API | `session.pin_to_version` | `session.fork(name)` |

## Storage layout

A fork is one Lance branch per dataset (vertex, edge-delta, adjacency). Reads chain to the parent via Lance's `base_paths` resolution. Primary writes after the fork-point are invisible to the fork; fork writes never appear on primary.

At fork creation, every dataset that exists on disk gets branched: the main label-agnostic `vertices` and `edges` tables, every `vertices_{label}`, and every `deltas_{type}_{fwd,bwd}` and `adjacency_{type}_{fwd,bwd}`. Datasets that don't exist yet (e.g. a label with no rows at fork-point, or a brand-new fork-only label) get materialized on-the-fly the first time the fork's writer touches them, with the parent commit on `main` left empty so primary's view stays untouched. The dynamic dataset → branch mapping is persisted into the fork's registry entry, so a restart recovers the same view.

On disk:

- `catalog/fork_registry.json` — the registry of all forks.
- `catalog/fork_schemas/{fork_id}.json` — per-fork schema overlay (currently always empty under the default `strict_schema: false` mode; reserved for Phase 6 promotion semantics).
- `catalog/fork_tombstones/{fork_id}.json` — durable drop intent, removed on completion.
- `catalog/forks/{fork_id}/id_allocator.json` — per-fork VID/EID allocator, bootstrapped from primary's HWM at fork creation.
- `wal_forks/{fork_id}/` — per-fork WAL stream. Replayed in `at_fork`; primary's recovery never reads it.

## Concurrency and isolation

- **Fork creation does not block primary** (spec §10). Reads and writes on primary continue at full throughput while a fork is being created.
- **Different forks proceed in parallel.** Same-name open-or-create serializes via a per-name async mutex.
- **Same-name fork sessions share a writer.** Two `session.fork("x")` calls on the same name resolve to the same `UniInner` (cached as `Weak<UniInner>` so the cache never extends a session's lifetime). A commit on session A is immediately visible to session B's reads — no flush required.
- **Multiple sessions can hold the same fork.** A holder count is tracked and `drop_fork` refuses with `ForkInUse` while sessions are alive, or with `ForkInflightTx` when an open transaction has yet to commit or roll back.
- **Lance compaction honors branch references.** Primary GC will not reclaim fragments that a live fork still references.

## Operational signals

- `uni_fork_l1_flushes{fork=...}` — gauge incremented on every successful fork flush. A proxy for fragment growth on the fork's branches.
- `tracing::warn!` fires once per writer when the per-fork flush count crosses `UniConfig::fork_fragment_warn_threshold` (default 256). Fork compaction is deferred to Phase 5; until then, long-lived heavy-write forks should be `drop_fork`-and-recreate to bound fragment accumulation.

## Crash recovery

Recovery runs in `Uni::open` before any session is exposed.

- **Pending fork** (create crashed before completion) → rolled back. Branches force-deleted via the missing-branch-tolerant `lance_branch::delete_branch` wrapper.
- **Tombstoned fork** (drop crashed mid-2PC) → completed. Branches deleted, registry entry removed, tombstone + overlay files cleaned.

Recovery is idempotent — running it twice is a no-op the second time.
