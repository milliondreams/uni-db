# Landing plan — `fix/unlabelled-source-narrowing`, 2026-09-14

Nine commits, three of them engineering. **Not ready to move to `main` yet.**
The gates that have run are green; the two that bear most directly on the change
have not run at all. This says what is verified, what is not, and in what order
to close the gap.

Engineering work and remaining LDBC findings live in
[`ldbc_findings_remediation_2026-09-13.md`](ldbc_findings_remediation_2026-09-13.md).
This document is only about landing the branch.

## What is on the branch

| commit | what |
|---|---|
| `5cf2a0fa6` | `fix(query)` narrow an unlabelled source to its edge type's labels |
| `aabbad593` | `fix(bench)` label the LDBC WORK_AT derivation source |
| `83946b162` | `fix(query)` chunk a full-graph scan instead of building it whole |
| `f70205e50` | `perf(query)` stop projecting an entity's properties to count it |
| `84b3c0f05` | `test(bench)` report storage round trips from the ad-hoc probe |
| 4 × `docs:` | the remediation plan and three corrections to it |

Touches `uni-query` (planner, df_planner, scan), `uni-store` (manager), and two
bench/example files. 1344 insertions, 60 deletions.

## Verified

- `uni-query` 849/849, `uni-store` 748/748, `uni-db` 2972/2972 — **4569 tests**
- `cargo fmt` and `cargo clippy` clean **on `uni-query` and `uni-store` only**
- Seven plan-shape tests covering the narrowing's direction handling, the
  undirected case, the multi-label decline, untyped relationships and an
  already-labelled source
- **SF1 end to end, by result content**: 11 of 14, the same three failures, all
  fourteen row counts identical, nine of eleven byte-identical. IC1 and IC12
  differ only in `collect()` element order — rows, scalars and list *sets* match
  (IC1 on all 260 fields). This is the check that can see a dropped property;
  a row count cannot.

## Not verified — and two of these matter

`pr.yml` has twelve lanes. Three have been run, two of those only partially.

| lane | run? | relevance to this branch |
|---|---|---|
| **tck** | **no** | **High.** `perf(query)` changes what `count(n)` materialises. The openCypher TCK has many `count()` scenarios and this branch has not faced one. |
| **rust-tests** (workspace) | partial — 3 crates of ~27 | **High.** `uni-locy` builds plans through the same planner (`plan_pattern_scoped` → `plan_path`). Untested here. |
| **metamorphic-smoke** | no | **High.** Oracle-based; it exists to catch exactly the silent wrong answer this branch's failure mode would be. |
| **python-tests** | no | Medium. Bindings drive these query paths. |
| **perf-gate** | no | Medium. Scan behaviour changed; instruction counts may move. |
| **static-guards** | no | Medium. The runbook says run before every push. |
| **lint** (workspace) | partial — 2 crates | Medium. `--workspace` clippy not run. |
| failpoints | no | Low. No recovery or write-path change. |
| loom-smoke | no | Low. No concurrency change. |
| miri | no | Low. No `unsafe` added. |
| fuzz-smoke | no | Low. No parser or codec change. |
| supply-chain | no | Low. No dependency change. |

## Order to close it

Commands from `docs/local_ci_runbook.md`. `RUSTC_WRAPPER=""` throughout; the
seven `--exclude`s are load-bearing on Linux (metal/cuda bindings and `uni-tck`
cannot build in a bare `--workspace`).

1. **TCK** — highest risk, run first so a failure lands before more time is spent:
   ```bash
   cargo nextest run -p uni-tck --test tck
   ```
2. **Workspace rust-tests** — the other high-relevance lane:
   ```bash
   cargo nextest run --workspace \
     --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
     --exclude uni-python-cuda --exclude uni-python-metal \
     --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
   ```
3. **Metamorphic smoke** and **static guards** — see the runbook's sections.
4. **Workspace lint** — `cargo fmt --all -- --check` plus `clippy --workspace`
   with the same excludes.
5. **python-tests** and **perf-gate** — the runbook notes the perf gate needs
   `--allow-foreign-machine` locally, and that a local baseline sits ~10x below
   CI's, so read it for regressions against itself rather than against CI.

Do **not** run `cargo nextest run -p uni-db` without `--tests`: it builds all 40
examples, each statically linking datafusion/lance/candle. Measured this session
at 30+ minutes without finishing, against under 15 minutes with `--tests` for
identical coverage. Worth adding to the runbook — the repo's 3-binaries-per-crate
cap covers `tests/` and nothing covers examples.

## Known-unexplained, and whether it should block

Three things on this branch are not understood. None is a known defect; all are
recorded at their code sites.

- **`count(DISTINCT n)` projects as narrowly as `count(n)`** though the pass
  excludes it, through a path not identified. *Should not block* — the answer is
  correct and the guard is the conservative direction. Two attempts to attribute
  it were inconclusive; the second control was invalid because `ORDER BY … LIMIT 1`
  over a scalar aggregate is elided.
- **Multi-label source narrowing is declined on measurement, not principle**
  (1.4x–2.2x slower on every multi-source type at SF1). *Should not block* —
  declining is the safe side. See remediation 0.3.
- **The pass fails closed on unmodelled operators**, so a `LogicalPlan` variant
  added later silently disables it rather than under-projecting. *Should not
  block* — but a reviewer should confirm they want that trade, because it means
  the optimization can quietly stop applying.

One thing *would* block if it appeared: any TCK or metamorphic failure touching
`count`, projection, or property visibility. That is the failure mode the
projection rule risks, and it is the reason those two lanes are first.

## Rebase note

The branch is 9 ahead of `main`, 0 behind, so it is a fast-forward today. Local
`main` tracks `md/main`, not `origin` — confirm the intended target before
pushing. Nothing here has been pushed to any remote.
