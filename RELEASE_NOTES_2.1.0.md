# Uni 2.1.0 Release Notes

**Release scope:** `v2.0.6` → `2.1.0` · 57 commits · ~22.7K insertions / 4.1K deletions across 215 files
**Dates:** 2026-06-05 → 2026-06-13
**Version path:** `2.0.6` → `2.0.7` (internal) → `2.1.0`

Uni 2.1 is a **consolidation and hardening** release on top of the headline 2.0 line. It lands a large Cypher-ingest and `MERGE` performance pass, a sweeping correctness/durability hardening sweep (the 2026-06-10 architecture & correctness review), a **durable Locy rule registry**, tighter plugin sandboxing, and Python concurrency ergonomics — with no change to the 2.0 data format and a refreshed dependency baseline.

---

## ⚠️ Breaking & Behavioral Changes (read first)

Four changes can affect existing code. Most are narrow; read the one that touches your usage.

### 1. Database-level Locy rule mutators are now `async` (and rules persist)

`db.rules()` registry mutators changed signature and gained durability (`2b81bee98`).

- **API break:** `register` / `remove` / `clear` on the **database-level** rule registry are now `async` — add `.await` (Rust) / `await` (Python async API). Session/transaction/fork rule scopes stay ephemeral and are unchanged.
- **New behavior:** database-level rules now **persist** their source to `catalog/locy_rules.json` (the same ObjectStore catalog as `schema.json`) and **recompile on open** — rules survive a restart. A missing file opens clean.
- Registry state is now a pure function of the source list (every mutation rebuilds from sources), structurally fixing the prior orphan-strata / duplicate-source accumulation bug. `register` is idempotent; redefining a name **supersedes** the prior source (one owner per name); removing a rule that shares its source with siblings is a **hard error** (no silent drops).
- **Escape hatch:** `UniBuilder::skip_invalid_locy_rules(true)` skips (with a warning) rules that fail to recompile on open instead of failing the open.

### 2. `tx.apply(derived)` is now fresh-by-default

DERIVE-then-apply now rejects stale derivations by default (`1ef384668`).

- **Before:** `tx.apply(derived)` accepted a derivation even if a commit had landed between DERIVE evaluation and apply.
- **Now:** it rejects with `StaleDerivedFacts` when the version gap is non-zero. Session-level DERIVE reads happen outside any transaction and can never be OCC-validated, so this version-gap check is the only staleness guard.
- **Opt out:** `.allow_stale()` restores the old behavior, or `.max_version_gap(n)` bounds the gap; `.require_fresh()` remains as the (now default) explicit form. Mirrored in `UniSync` and both Python APIs — async `apply()` defaults to `require_fresh=True`; pass `require_fresh=False` for the old stale-allowed behavior.
- Related: Locy reads are now recorded in the SSI read-set, so a read-modify-write performed through `tx.locy()` now conflicts correctly (previously it could commit cleanly past a conflict the equivalent Cypher path would catch).

### 3. The CLI now autocommits write statements

The REPL and `uni query` previously ran every statement through the read-only `session.query()`, so write statements either errored or silently dropped their effect. They now route through the new autocommitting `Session::run` (`48dfb3ccb`): writes execute in an autocommitting transaction (preserving `RETURN` rows); read-only `query()` remains write-rejecting.

### 4. New distinct Python exceptions for retriable conflicts

`SerializationConflict`, `ConstraintConflict`, and `LockTimeout` previously fell through to the generic `UniError` catch-all (`ef6195ded`). They now map to dedicated classes — `UniTransactionConflictError`, `UniConstraintConflictError`, `UniLockTimeoutError` — distinct from the non-retriable `UniConstraintError` / `UniTimeoutError`. **If you were catching the generic error to detect SSI contention, switch to the specific class.**

---

## 🚀 Performance — Cypher ingest & MERGE

The largest theme of the release: the write path and `MERGE` got materially faster.

- **SET→CREATE fusion** (`9f5601aa0`) — a trailing `SET` on a freshly-`CREATE`d node is folded into the create, dropping the entire `MutationSetExec` pass.
- **Transaction write-path plan cache** (`d6adcf327`) — logical plans are cached on the write path. Combined with fusion, ingest improved substantially (node ≈ −41%, edge −11%→−38% on the ingest harness).
- **MERGE fast-path prefetch** — the persisted-row lookup is batched to **one scan per statement** (`74b67919a`) and properties are prefetched at statement level (`f614f317c`), collapsing the per-row planning that made batched `MERGE` no faster than a loop. On the SET-residual MERGE benchmark this is a ~**63×** improvement (1.10 s → 17.5 ms).
- **Schemaless traversal** pushes source vids into the scan and `Arc`-shares edge properties (`d898f4fe9`).
- **Storage hot paths** — WAL exactly-once commits, reduced property cloning, and an **O(1) `ext_id` index** (`5819194d3`); a read-lock fast path for `get_or_assign_edge_type_id` (`f0f48e6d8`); plus four small hot-path wins from the review (`f4c5f51d4`).
- **Group commit** was prototyped, benched, and **declined (NO-GO)** with the data recorded for posterity (`298214cf6`).

New benchmarks back these up: ext-id ingest, batched-MERGE, and schemaless-traversal harnesses (`c7222b7d0`).

---

## ✅ Correctness — MERGE, MVCC, temporals, and the 2026-06-10 review

### MERGE & MVCC
- **Schemaless `MERGE` now matches flushed main-table rows** (`d5b77ea71`) — previously a persisted match could be missed in three per-label blind spots, causing duplicate creates.
- **Superseded MVCC rows are dropped** in the `MERGE` lookup and pushed-filter scans (`b4bda0793`) — stale indexed-property matches after a rewrite + reflush no longer match.
- **`MERGE … ON CREATE SET` no longer false-rejects NOT-NULL props** (`3155a3710`) — the create path now seeds `ON CREATE SET` values before constraint validation, so a NOT-NULL property supplied only by `ON CREATE SET` is accepted (self-referential assignments are left to the post-create SET to avoid double-application).
- **`id(r)` on a relationship is edge-aware in `WHERE`** (`f99c2dfd7`) — it now lowers to the edge id column (`_eid`) instead of the nonexistent `_vid`, so `WHERE id(r) = …` matches the right rows.

### Temporals
- **Typed temporals are preserved through property-map paths** (`4583ee870`) — `RETURN n`, `properties(n)`, single-property access, and edge maps all return `Value::Temporal` (not stringified) across L0 and L1 tiers.

### The 2026-06-10 architecture & correctness review
A multi-tier review landed a cluster of verified fixes:
- 9 verified correctness-cluster bugs (`eca24239a`) and Tier 1–4 correctness/durability/security fixes (`03944c135`, `6f2e3c8e7`, `3790bb822`).
- Integer-overflow errors in DataFusion column arithmetic now error instead of wrapping (`293a0868a`).
- Returned-path edges keep their **stored** direction rather than traversal order (`8fd40049f`).
- Parenthesized-path quantifier bounds corrected; malformed integers rejected (`dea6d46b2`).
- `MERGE`/plan-cache hits **re-verify the query text** before reuse (`854c495c5`).
- BTIC rejects multi-byte UTF-8 near the BCE suffix instead of panicking (`daf070c4a`).
- Async-flush window: commit-time overlay checks now cover it (`057ade69f`).

### Locy
- Clauses with **multiple positive IS-refs** now derive correctly (`dbaeabf3a`).
- Empty-typed-`List<Float32>` columns keep their element type across flush + reopen (regression guard, `b25f7fc69`).

---

## 🔒 Storage durability & isolation

- **WAL integrity** — per-record checksums, torn-tail recovery policy, and local `fsync` (`88c730cb9`).
- **C2 read-tier pinning** — read-write transactions pin the L1 scan tier (`5fa8d804a`); the pin is row-existence-only so property/edge reads stay live (`af81cfa07`).

---

## 🛡️ Plugin sandboxing

- **Security trio** (`cf2d8c248`) — Rhai operation limits, plus per-invoke isolation for WASM and Extism plugins.
- **WASM resource limits** (`68eaaacb8`) — enforced wall-clock timeouts, memory caps, and per-call fuel.

---

## 🐍 Python bindings

- **GIL released across blocking calls** (`ef6195ded`) — 89/90 `block_on` sites now run inside `py.detach`, so a long query/commit no longer stalls every Python thread (and a Rust→Python callback on a non-Locy path can't deadlock).
- **Conflict-retry helpers** — pure-Python `transact_with_retry` / `execute_with_retry` (and async variants) mirroring Rust's `RetryOptions` defaults (5 attempts, 200 µs base, 50 ms cap, ±50% jitter), plus the new distinct conflict exceptions (see Breaking Change #4).

---

## 🔧 CI, tooling & tests

- **CI lanes expanded** (`b32304eb2`) — PR-time openCypher TCK, fuzz targets, per-test nextest timeouts, and a nightly lane (soak / fuzz / bench).
- TCK oracles aligned with openCypher comparison semantics (`c45cd7030`); the sidecar schema generator falls back to `CypherValue` for mixed-type columns (`f9804de43`) and covers `NonLinearRecursion` (`d50a3c9d2`).
- Flaky `corrupt_wal_tail_does_not_block_reopen` stabilized under load (`936838382`).

---

## 📦 Dependencies

- `uni-xervo` 0.13.1, `pyo3` 0.29 (capsule `CStr` fix), `chrono` 0.4.45, plus a transitive refresh and `uv.lock` regeneration (`306d6257f`).
- Wheel variants declare empty `wasm-plugins` / `extism-plugins` features on plugin-less builds (`af31eb9f6`).

---

## 📚 Documentation

- Added the 2026-06-10 architecture review and correctness/performance review docs, with fix-status and benchmark-outcome records (`8a22b2a57`, `075f9cba1`, `f75c337a1`, `e549de03b`, `d056ddd2b`, `9c37daa3b`).
- Pre-release doc-honesty pass (`d850ac447`) and a broken intra-doc-link fix surfaced by the docs gate (`93275cd6f`).

---

## Upgrade checklist

1. **Locy rule registry users:** add `.await` to `db.rules()` `register`/`remove`/`clear`; expect db-level rules to now persist and recompile on open.
2. **DERIVE/apply users:** if you relied on applying possibly-stale derivations, add `.allow_stale()` (or `.max_version_gap(n)`); otherwise enjoy the safer default.
3. **Python SSI-retry users:** catch `UniTransactionConflictError` (and friends) instead of the generic error, or adopt the new `transact_with_retry` helper.
4. **CLI users:** write statements now take effect (autocommit) — intended, but be aware if you scripted around the old drop-on-write behavior.
5. **Everyone else:** drop-in upgrade — no data-format change; rebuild against the refreshed dependency baseline.
