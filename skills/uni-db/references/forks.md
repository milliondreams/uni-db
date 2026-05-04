# Forks

Named, durable, isolated branches of the graph. Phase 2: writable.

## Quick start

```rust
let session = db.session();
let fork = session.fork("scenario_1").await?;
// or .new_().await for must-create

let tx = fork.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob-on-fork'})").await?;
tx.commit().await?;
// Lands on the fork's branches; primary is unchanged.
```

## API

| Method | What it does |
|---|---|
| `Session::fork(name)` | Open or create a fork. `.await` opens-or-creates; `.new_().await` errors with `ForkAlreadyExists` if the name is taken. |
| `Session::is_forked()` | `true` when this session was returned by `fork`. |
| `Uni::list_forks()` | All Active forks. |
| `Uni::fork_info(name)` | Metadata for a single fork. |
| `Uni::drop_fork(name)` | Full 2PC drop. Errors with `ForkInUse` while sessions are alive, `ForkInflightTx` while a transaction is open on the fork. |

## Errors

`UniError::Fork*`: `ForkNotFound`, `ForkAlreadyExists`, `ForkInUse { name, holder_count }`, `ForkInflightTx { name }`, `ForkCorruptRegistry`, `ForkLifecycle { name, stage, source }`.

## What sibling sessions on the same fork see

Two `session.fork("x")` calls on the same name resolve to the same `UniInner`. A commit on session A is immediately visible to session B's reads — no flush required. The cache is `Weak<UniInner>` so it never extends a session's lifetime.

## What writes through a fork actually do

- The fork's writer flushes through `BranchedBackend` to the fork's Lance branches.
- Datasets that exist on primary at fork-point (main `vertices`/`edges` plus per-label / per-edge-type tables) are branched eagerly.
- Datasets that don't exist yet are materialized on-the-fly: an empty parent commit on `main` (so primary stays untouched) plus a branch with the actual data. The dataset → branch mapping is persisted into the fork's registry entry, so a restart recovers the same view.
- Per-fork `IdAllocator` (`catalog/forks/{fork_id}/id_allocator.json`) is bootstrapped from primary's HWM — so VID/EID streams don't collide with primary.
- Per-fork WAL lives at `wal_forks/{fork_id}/` (NOT `wal/forks/...`).

## Operational signals

- `uni_fork_l1_flushes{fork=...}` gauge — per-fork flush count.
- `tracing::warn!` once per writer when the count crosses `UniConfig::fork_fragment_warn_threshold` (default 256). Fork compaction is Phase 5; until then, drop-and-recreate to bound fragment growth.

## What's not in Phase 2

- Nested forks (`forked.fork(name)`) — Phase 3.
- TTL, tags, watch filtering, hooks/params propagation — Phase 4.
- Fork-local index fusion — Phase 5.
- Diff and promotion — Phase 6.
- Strict-schema deployments creating new fork-only labels (the `SchemaDelta` overlay growth path is reserved for that).
