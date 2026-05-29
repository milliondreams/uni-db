# Code Simplifier Review: `uni-plugin-wasm-rt`

Scope: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-wasm-rt/`
Focus: duplication, dead code, complex functions, unnecessary abstractions, IPC boundary.
Recently modified: `src/error.rs`, `src/ipc.rs`.

## Summary

The crate is small, well-documented, and the M6.shared lift is justified (the two
sibling loaders genuinely re-use `ipc` and `pool`). Most observations are minor.
The only meaningful duplication is around `Send + Sync + 'static` trait bounds
on the pool generics, and a small amount of redundant work inside the IPC layer
(e.g., `encode_batches` walks every batch even though all batches share a schema).
The `idle_ttl_secs` field is plumbed through `PoolConfig` but never consulted —
this is dead surface area that should either be wired up or documented as
"reserved for the reaper task".

---

## Findings

### F1. `PoolConfig::idle_ttl_secs` is unused (dead field)

- **Where:** `src/pool.rs:37`, `src/pool.rs:45`
- **Description:** `idle_ttl_secs` is in `PoolConfig`, set by `Default`, and
  set explicitly in every test (`src/pool.rs:315, 331, 355, 376, 399`) but
  never read by `InstancePool` — there is no reaper or TTL-driven `release`.
  The pool only honours `max_instances` and `warm_count`.
- **Suggestion:** Either (a) remove the field until a reaper exists, or (b) add a
  one-line `#[doc = "reserved — currently unused; will drive the idle reaper in M7"]`
  and emit a `tracing::warn!` once in `new()` if the user sets a non-default
  value, so callers don't assume reaping is active. Option (a) is preferred —
  it can be re-introduced when the reaper lands.
- **Effort:** XS (≈10 min for removal, ≈5 min for doc-and-warn).

### F2. `encode_batches` walks every batch for secret-handle, but Arrow IPC enforces a single schema

- **Where:** `src/ipc.rs:93-101`
- **Description:** `encode_batches` calls `reject_secret_handles(b)` for every
  batch in the slice. Because Arrow IPC stream-writer requires all batches to
  share the schema of the first one (`write_stream` uses `first.schema()`),
  walking each batch's schema is redundant work — the schema is identical by
  contract. The fields walked are also identical Arc-shared `Field` instances.
- **Suggestion:** Walk only `batches[0]` once. If a defensive check is wanted
  for the (unreachable) mismatched-schema case, let the `StreamWriter` raise it.
- **Effort:** XS (3 min). Trivially preserves behaviour because of the schema
  invariant.

### F3. Repetitive `Send + Sync + 'static` bounds on `InstancePool` / `PooledInstance`

- **Where:** `src/pool.rs:85-94, 96-100, 114-118, 213-220, 222-226, 234-238, 273-277`
- **Description:** The same `where T: Send + 'static, E: PoolResourceLimit + Send + Sync + 'static`
  clause appears seven times. Each new method/impl block restates it.
- **Suggestion:** Add a sealed helper trait to collapse the bounds, e.g.
  `trait PoolError: PoolResourceLimit + Send + Sync + 'static {}`
  with a blanket impl, then write `where T: Send + 'static, E: PoolError`. Cuts
  ~20 lines and centralises the bound. (Optional — this is purely cosmetic and
  the bounds are already trivial to grok.)
- **Effort:** S (≈15 min, no behaviour change).

### F4. `acquire` race: `live` count can exceed `max_instances` under contention

- **Where:** `src/pool.rs:161-178`
- **Description:** The check-then-increment pattern (`load` at line 166,
  `fetch_add` at line 176) is racy: N concurrent acquires that all see
  `live == max - 1` will each construct a fresh instance, pushing `live` over
  `max_instances`. Not a simplification issue per se, but flagged because the
  fix often *simplifies* the code: `fetch_add` then compare, and on overflow
  `fetch_sub` and return `resource_limit`.
- **Suggestion:** Optimistic `fetch_add` + rollback; or use a semaphore. The
  current weaker invariant ("approximately max_instances") should at minimum
  be documented on `PoolConfig::max_instances`.
- **Effort:** S (≈20 min including a stress test).

### F5. `PooledInstance::take` and `Drop` both decrement `live` — asymmetric with `release`

- **Where:** `src/pool.rs:266-270` (take), `src/pool.rs:184-188` (release),
  `src/pool.rs:278-282` (drop)
- **Description:** `release` decrements `live` only when the idle queue is
  full (instance dropped on the floor). On the normal release path, `live`
  is *not* decremented because the instance is still considered alive in the
  pool. `take` always decrements — correct, since the caller keeps the
  instance outside the pool's accounting. This is subtle and worth a comment.
- **Suggestion:** Add a 2-line doc comment on `release` explaining the
  "live counts warm + checked-out, not checked-out alone" accounting model.
  No code change needed.
- **Effort:** XS (5 min).

### F6. `decode_batch` collects all batches then discards all but one

- **Where:** `src/ipc.rs:128-131`, `src/ipc.rs:152-158`
- **Description:** `decode_batch` calls `read_stream` (which `collect`s the
  entire stream into a `Vec<RecordBatch>`) and then `.pop()`s the last one.
  If a malicious/large plugin sent multi-batch output to a single-batch
  caller, the host allocates the full vector for nothing.
- **Suggestion:** Have `decode_batch` directly drive the iterator and take
  `next()` (or `last()`), avoiding the intermediate `Vec`. Trivially
  re-uses `StreamReader::try_new`; share via a tiny `open_reader` helper.
- **Effort:** S (≈15 min). Also slightly clarifies semantics: today
  "first" batch is actually returned as `pop()` (the *last* batch). The
  docstring says "the first" — this is a real correctness bug to flag,
  not just a simplification.

### F7. `reject_secret_handles` nested `fn walk` could be a free function

- **Where:** `src/ipc.rs:45-65`
- **Description:** `walk` is a closed-over inner `fn` (no captures), so it
  is effectively a free function in disguise. Hoisting it to module scope
  makes it directly testable and slightly clearer.
- **Suggestion:** Promote `walk` to `fn walk_field(field: &Field) -> Result<(), IpcError>`
  at module level; `reject_secret_handles` becomes a 2-line wrapper.
- **Effort:** XS (5 min).

### F8. `estimate_size` heuristic is fine but unexplained per-cell constant

- **Where:** `src/ipc.rs:160-165`
- **Description:** `rows * cols * 16 + 4096` — the `16` is a guess at
  "average bytes per cell", but a `LargeBinary` row or a list-of-int64
  row easily blows that. Not wrong (the writer grows on demand), just
  worth a 1-line justification.
- **Suggestion:** Add a comment naming an Arrow type the heuristic is
  tuned for, e.g. "tuned for the typical scalar-call case (one int64 +
  one string column)".
- **Effort:** XS (2 min).

### F9. `IpcError` is `#[non_exhaustive]` but only has 3 variants — confirm contract

- **Where:** `src/error.rs:16`
- **Description:** `#[non_exhaustive]` is appropriate for a public error
  enum, but the docstring promises "both loaders wrap via `#[from]`" —
  worth verifying that callers (`uni-plugin-wasm/src/error.rs:32`,
  `uni-plugin-extism/src/error.rs`) do handle the wildcard correctly.
  From a quick grep, both crates use `#[from]` to convert and never
  `match` exhaustively — good.
- **Suggestion:** None — leave as is. Listed only because the reviewer
  asked to confirm dead code; this is not dead.
- **Effort:** N/A.

### F10. `idle_len` is `#[doc(hidden)]` — test-only API leaking from prod

- **Where:** `src/pool.rs:202-205`
- **Description:** `idle_len` exists only to support the two tests at
  `src/pool.rs:382-388, 405-409`. `#[doc(hidden)]` makes it invisible in
  docs but it's still a stable surface other crates can call.
- **Suggestion:** Gate it on `#[cfg(any(test, feature = "test-util"))]`
  or move the tests inside the `pool.rs` module (already there) and use
  a `pub(crate)` accessor. Since the tests *are* in-module, just change
  `pub fn idle_len` to `pub(crate) fn idle_len`. One-line change.
- **Effort:** XS (2 min).

### F11. Two repeated `Field`-with-secret-extension builders in tests

- **Where:** `src/ipc.rs:365-372` (`secret_tagged_field`) and inline at
  `src/ipc.rs:456-461` (inside `encode_batch_rejects_secret_handle_inside_struct`).
- **Description:** The test for struct-nested secrets re-builds the
  tagged field with the same `HashMap` instead of calling the existing
  helper. Trivial duplication.
- **Suggestion:** Reuse `secret_tagged_field("handle")` in the struct
  test.
- **Effort:** XS (2 min).

---

## What is NOT a problem

- `InstancePool` generics over `T, E` are justified by two real callers.
- The factory `Mutex<Box<dyn Fn ...>>` is fine — `Fn` not `FnMut`, so the
  mutex is strictly belt-and-braces and could become `Arc<dyn Fn>` (no
  lock at all). Optional micro-improvement, ~XS effort, but not flagged
  as a finding since the current code is correct.
- `lib.rs` re-exports are minimal and well-curated.
- IPC tests are thorough and round-trip enough Arrow types to be a real
  regression net.

---

## Effort summary

- XS (≤10 min): F1, F2, F5, F7, F8, F10, F11
- S  (≤30 min): F3, F4, F6
- Total if all applied: roughly 1.5–2 hours.

Highest-value items: **F6** (potential correctness bug — `decode_batch`
docstring vs `pop()` behaviour), then **F2** (clear redundant work),
then **F1** (dead field). F4 is the only concurrency concern worth
escalating beyond simplification.
