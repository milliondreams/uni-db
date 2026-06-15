# Uni 2.2.0 Release Notes

**Release scope:** `2.1.0` → `2.2.0` · the 2026-06-13 architecture & correctness review (5 critical + 16 high-tier fixes) + MERGE concurrency/performance
**Dates:** 2026-06-13 → 2026-06-14
**Version path:** `2.1.0` → `2.2.0`

Uni 2.2 is a focused **correctness, durability, and security hardening** release. It lands the full 2026-06-13 architecture review — five **critical** (C1–C5) and sixteen **high-tier** (H1–H16) fixes spanning storage durability, MVCC/SSI, query semantics, plugin sandboxing, and graph algorithms — plus two MERGE wins: concurrent MERGE is now phantom-safe, and relationship `MERGE` gets a bulk fast-path. The on-disk format is unchanged except for two backward-compatible migrations (the ORSet CRDT format and the UID index), both noted below. Dependency baseline refreshed (`uni-xervo` 0.14.0).

---

## ⚠️ Breaking & Behavioral Changes (read first)

Four changes can affect existing code or stored data. Read the ones that touch your usage.

### 1. Concurrent `MERGE` without a UNIQUE constraint now conflicts instead of duplicating (RC2)

Previously, two transactions that concurrently `MERGE`d the same key with **no** declared `UNIQUE` constraint could both see an empty match and both insert — silently producing duplicate nodes (SSI tracks item-level reads only, so an empty match registered no conflict).

- **Now:** a `MERGE` that *creates* a node registers an implicit per-transaction MERGE-key guard; at commit, a concurrent transaction that already committed the same key is aborted with the **retriable** `ConstraintConflict` (it never silently duplicates). Re-running through `transact_with_retry` converges to exactly one node (the retry observes the committed row). A plain `CREATE` of the same properties registers no key and is unaffected.
- **Action:** if you `MERGE` the same key from concurrent writers, either wrap the write in `transact_with_retry` / `execute_with_retry`, or declare a `UNIQUE` constraint. This lets you drop any application-side striped-lock RMW layer.

### 2. ORSet CRDT is now tombstone-free (ORSWOT) (H10)

The observed-remove set CRDT migrated to an **ORSWOT** (Observed-Remove Set Without Tombstones) encoding.

- **Reads** of the legacy v1 (tombstoned) format are backward-compatible (transparent decode).
- **Writes** use the new tombstone-free format. Data written by 2.2.0 to an ORSet column is **not** readable by a pre-2.2.0 binary — the upgrade is one-way for ORSet-typed properties.

### 3. Random walks are now deterministic & seedable (H16b)

`node2vec` / random-walk sampling moved to a deterministic, seedable RNG with proper **p/q biased** second-order sampling.

- Embeddings and walk outputs are now **reproducible** run-to-run from a seed, but **differ** from 2.1.0's output. Re-baseline any stored embeddings or golden outputs if you compare across versions.

### 4. UID index gains a versioned column with on-open migration (C3)

The UID index now carries a deterministic `_version` column. Older databases **migrate transparently on first open** (no tombstone is written; downstream liveness was re-verified). No action required.

---

## ✅ Correctness & Durability — the 2026-06-13 review

### Critical (C1–C5)
- **C1 — Lance scans fail closed.** A scan error no longer degrades to a silent partial/empty result; it surfaces as an error.
- **C2 — Tombstone/version-aware key reads.** Four key-read paths now honor tombstones and version visibility, so a deleted or superseded row can't resurface.
- **C3 — Deterministic, versioned UID index** with legacy migration (see Breaking #4).
- **C4 — Durable pointer before WAL truncation.** The manifest and `latest` pointer are fsync'd *before* the WAL is truncated, so a crash can't strand a committed tail.
- **C5 — Schema bumps on new edge-type id.** Assigning a new dynamic edge-type id now advances the schema version.

### High-tier (H1–H16)
- **Storage durability & correctness (H1–H3, H11–H13):** complete SSI read-set capture (H1); flush panic isolation (H2); WAL ghost-commit deletion on recovery (H3); compaction high-watermark predicate delete (H11); adjacency-compaction mutex + drain (H12); integer-truncation guarded via `try_from` (H13).
- **Query semantics (H5–H7):** `range()` overflow / float-argument handling (H5); `size()` returns character count (H6); subplan cache keyed by structural equality (H7).
- **Variable-length-path edge filtering (H4):** flushed edges are filtered by inline edge-property conditions during VLP warming, so a `()-[r:T {k: v}]->()` predicate matches correctly across the flush boundary.
- **Bulk loading (H8, H9):** the `UNIQUE` constraint check now spans buffer flushes (H8); the multi-dataset bulk dual-write is crash-atomic via a durable intent marker + reopen reconciliation (H9).
- **CRDT (H10):** tombstone-free ORSet (see Breaking #2).

---

## 🚀 MERGE — concurrency & performance

- **Phantom-safe `MERGE` (RC2)** — see Breaking #1. Implemented as an implicit commit-time MERGE-key guard that reuses the existing `UNIQUE`-constraint machinery (`L0Buffer::merge_guard_index`), so it adds no new locking and stays consistent with the optimistic SSI model.
- **Relationship-`MERGE` bulk fast-path (RC3)** — a batched `(a)-[:R]->(b)` `MERGE` rebuilt and ran a per-row traversal plan just to check edge existence (~**19×** the bulk `CREATE` of the same edges). It now detects the bound-endpoints, anonymous-edge shape and resolves existence with one MVCC-correct adjacency probe (`get_neighbors`, which merges CSR + all L0 buffers including the transaction's own writes — so intra-batch dedup is correct), reusing the general create / `ON CREATE` path. **~19× → ~1×** of bulk `CREATE`. Shapes it does not cover fall through to the general path.
- **Atomic SET without a client retry loop** was evaluated and ruled a **wishlist**, not a bug: SSI already prevents lost updates *with* retry, so no engine change was made.

---

## 🛡️ Plugin sandboxing

- **Redirect-based SSRF blocked (H14)** — the plugin host-net path no longer follows redirects that could reach internal addresses.
- **Extism sandbox limit floors (H15)** — minimum memory / fuel / timeout floors are enforced so a plugin can't request an effectively-unbounded sandbox.

---

## 📊 Graph algorithms

- **Weighted all-pairs shortest paths (H16a).**
- **Deterministic, seedable `node2vec` with p/q biased walks (H16b)** — see Breaking #3.
- **Louvain local-move baseline (H16c).**

---

## 📦 Dependencies

- `uni-xervo` **0.13.1 → 0.14.0** (compiles clean across the workspace; no API changes required).

---

## 🔧 CI, tooling & tests

- **CI lanes split** (`52cf98d30`) — fast PR checks are separated from the thorough push-to-main run, with no duplicated jobs.
- Each review fix ships with a fail-before / pass-after regression test; the openCypher TCK (3925 ×2 schema modes) and Locy TCK (501 ×2) remain green.
- **Doc gate** — four broken intra-doc links repaired so `cargo doc -D warnings` is green again (one new, three pre-existing).
- In-tree repro/guard tests added for the five "open" uni-db issues raised by the downstream workarounds catalog (RC2 and RC3 reproduced and were fixed; RC6, RC13, and the slow-pattern-in-WHERE residual were verified **not** to reproduce against HEAD and are pinned as guards).

---

## Upgrade checklist

1. **Concurrent-`MERGE` users:** same-key `MERGE` without a `UNIQUE` constraint now returns a retriable `ConstraintConflict` instead of duplicating — wrap writes in `transact_with_retry`, or declare a `UNIQUE` constraint. Application-side striped locks for merge keys can be removed.
2. **ORSet CRDT users:** 2.2.0 writes the tombstone-free format; reads of older data are transparent, but do **not** downgrade a binary after writing ORSet data.
3. **`node2vec` / embedding users:** walk and embedding outputs change (now deterministic/seedable); re-baseline stored embeddings if you depend on cross-version reproducibility.
4. **Everyone else:** drop-in upgrade — the UID index migrates on open; no other on-disk format change. Rebuild against the refreshed `uni-xervo` 0.14.0 baseline.
