# M0 Prework — Baseline Snapshot

**Date:** 2026-05-22
**Base SHA:** `aa6446c30c0926d692c2c45f106dd0f550b655ee`
**Branch:** `main` (in worktree `plugin-fw`)

This document captures the pre-refactor state of the codebase. All milestones after M0 are diffed against this baseline.

## Workspace structure

12 crates in the workspace:

```
crates/uni-common   crates/uni-btic     crates/uni-store    crates/uni-algo
crates/uni-query    crates/uni-cli      crates/uni          crates/uni-crdt
crates/uni-cypher   crates/uni-locy     crates/uni-tck      crates/uni-locy-tck
```

Plus Python bindings under `bindings/uni-db` (wheel matrix variants).

Workspace package version: **1.3.0**, Rust edition **2024**.

Key dependency versions to pin in `uni-plugin`:
- `arrow` / `arrow-array` / `arrow-schema` / `arrow-row`: **57.2.0**
- `datafusion`: **52.3.0**
- `arc-swap`: **1**
- `serde`, `serde_json`, `thiserror`, `tracing`

## Inventories captured

- `fold-agg-kind-refs.txt` — 120 references to `FoldAggKind` (enum + variants) across `crates/`. To be deleted in M3.
- `hardcoded-procedure-refs.txt` — 40 matched lines of hardcoded procedure / function dispatch in `crates/uni-query/src/`. To be migrated in M2/M4.

## Test baseline

Recorded at start of milestone work. Pre-refactor TCK and unit test pass-list will be re-validated at every milestone boundary; any regression vs. this baseline blocks the milestone.

Baseline benchmarks deferred (not run in this session); pre-refactor perf snapshot to be captured when M2's `NativeArrowUdf` is ready for comparison.

## Mechanical acceptance criteria for downstream milestones

Anchored at these baselines:

- After **M3**: `grep -rn 'enum FoldAggKind' crates/` MUST return zero hits.
- After **M3**: `grep -rn 'FoldAggKind::' crates/` MUST return zero hits.
- After **M4**: `grep -rn '"uni.admin.compact"' crates/uni-query/src/` MUST return zero hits in match-shaped dispatch (i.e., no `match` arm referencing the string literal — the string only appears as a `QName` parse target or registration site).

## What's next

M1 starts immediately: scaffold `crates/uni-plugin/` with the core trait, registry, manifest, and per-surface capability traits. No host integration; no behavior change; tests-green at completion.
