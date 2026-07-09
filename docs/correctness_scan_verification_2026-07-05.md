# Correctness Scan — Independent Verification & Repro Coverage (2026-07-05)

Companion to `docs/correctness_scan_2026-07-05.md`. Every finding in that scan was
**independently re-verified from source** (original verdicts/classifications ignored),
and a runnable repro test was created for every survivor.

## Verification outcome

| Metric | Count |
|---|---|
| Findings audited | 171 |
| **Confirmed** (re-verified against current source) | **167** |
| Uncertain | 1 |
| **Refuted** (disagree with original scan) | **3** |
| Already fixed | 0 |

### The 3 refuted findings (no repro — bug not present)

1. **`uni` [10] — `impl_query.rs:246` aggregate column order.** Claim: aggregate plans
   aren't wrapped in a `Project`, so interleaved `RETURN a, count(*), b` order is lost.
   Refuted: the RETURN planner builds `Aggregate` then *unconditionally* wraps it in a
   final `Project` (planner.rs ~3295) whenever projections are non-empty, iterating in
   original RETURN order — order is preserved.
2. **`uni-query` [3] — `locy_fixpoint.rs:5169` TopKProofs IS-ref over-inflation.** Claim:
   the `body_support_map` reads a rule's *converged* fact set, inflating IS-ref support.
   Refuted: a recursive rule owns two registry handles; the self-ref handle carries the
   (usually empty) semi-naive delta, and the code reads the correct one. (One verifier
   returned *uncertain* on this; net disposition: refuted.)
3. **`uni-db-bindings` — `builders.rs` `block_on` under `std::Mutex`.** Claim: holding a
   `std::Mutex` guard across `block_on` deadlocks/nests runtimes. Refuted: `block_on` is
   invoked from synchronous `#[pymethods]` via `py.detach(...)`, never from inside the
   tokio runtime — standard pyo3 ownership pattern, no nesting.

## Repro coverage

All 167 confirmed findings have a repro. Would-fail (correct-behavior) assertions are
`#[ignore]`d so CI stays green; most repros instead assert the *observed buggy behavior*
directly and pass. Every crate below compiles clean (`cargo test -p <crate> --no-run`,
independently re-verified via forced rebuilds).

| Crate | Findings | Repro location | Notes |
|---|---|---|---|
| `uni-query` | 40 | `tests/correctness_repros.rs` (+`vector_agg_return_type_repro.rs`) | [3] refuted; [22]/[36]/[39] recalibrated (see below); 8 ignored |
| `uni-store` | 18 | `tests/common/bugs/repro_*.rs` | `fault_store`/`fault_backend` harness; 3 ignored (order/TOCTOU) |
| `uni-query-functions` | 16 | `tests/repro_df_*.rs`, `repro_value_functions.rs`, `repro_function_rename.rs` | all pass |
| `uni-db-bindings` (`uni-python`) | 11 | `bindings/uni-db/tests/test_repro_*.py` | 1 refuted (builders Mutex); needs built extension to run |
| `uni` (`uni-db`) | 11 | `tests/common/bugs/repro_*.rs` | [10] refuted; 3 ignored (timing/race) |
| `uni-algo` | 8 | `tests/algo_correctness_repros.rs` | |
| `uni-plugin-custom` | 8 | `tests/correctness_repros.rs` | |
| `uni-plugin-host` | 6 | `tests/bug_*.rs` | all pass |
| `uni-bulk` | 5 | `crates/uni/tests/common/bugs/bug_bulk_*.rs` | repro'd via `uni-db` test tree |
| `uni-cypher` | 5 | `tests/repro_*.rs` | |
| `uni-locy` | 5 | `tests/repro_*.rs` | |
| `uni-plugin-rhai` | 5 | `tests/it/bug_repros.rs` | |
| `uni-fork` | 4 | `tests/promote_diff_bugs.rs` | |
| `uni-plugin` | 4 | `tests/bug_repros.rs` | |
| `uni-tck` | 4 | `tests/repro_*.rs` | |
| `uni-cli` | 3 | `tests/repro_*.rs` | |
| `uni-common` | 3 | `tests/repro_*.rs` | fixed `Arc<dyn ObjectStore>` coercion |
| `uni-plugin-apoc-core` | 3 | `tests/bug_repros.rs` | |
| `uni-plugin-extism` | 3 | `tests/manifest_callout_repro.rs`, `aggregate_return_type_repro.rs` | [2] uncoverable at integration level (see below) |
| `uni-btic` | 2 | `tests/bug_repros.rs` | |
| `uni-crdt` | 2 | `tests/bugs_repro.rs` | |
| `uni-plugin-builtin` | 2 | `tests/repro_*.rs` | |
| `uni-plugin-pyo3` | 2 | `tests/repro_*.rs` | |
| `uni-locy-tck` | 1 | `tests/repro_having_executed_no_docstring.rs` (+`.feature`) | |

## Recalibrated repros — a caught false-positive

Three `uni-query` repros initially *passed while asserting the bug* for the wrong reason:
their queries returned correct results because they never forced the plan shape that
triggers the defect. Source re-check confirmed the buggy arms still exist, so the repros
(not the bugs) were fixed:

- **[22] `apply.rs:350`** — unsupported-operator conjuncts in an `Apply` `input_filter`
  evaluate to `false`. Trigger needs an in-query `CALL proc() YIELD` (builds `Apply`) with
  the predicate injected as `input_filter`; `a.name STARTS WITH 'Al'` is mis-evaluated to
  false and drops a matching row.
- **[36] `planner.rs:6211/7739`** — `WHERE` on a scan-bound var is dropped when a
  `Sort/Limit/Aggregate/Apply` sits between the filter and the `ScanAll`. Trigger:
  `MATCH (n) WITH n ORDER BY n.name MATCH (n) WHERE n.age > 30 RETURN n.name` returns the
  `age=30` row it should exclude.
- **[39] `planner.rs:6168/6665`** — label disjunction (`WHERE n:A OR n:B`) dropped past an
  intervening `Sort`; `replace_scan_all_with_label_union` falls through `other => other`.

Root pattern (recurs across [22]/[36]/[39]): a helper (`find_scan_label_id` /
`is_scan_all_for`) *descends* `Sort/Limit/Aggregate/Apply/Union` to mark a predicate
consumed, but the sibling rewriter has no arm for those nodes and drops it via
`other => other`.

## Coverage caveats (honestly unreached)

- **`uni-plugin-extism` [2] `host_svc/mod.rs:86` `from_hex` multibyte panic** — `pub(crate)`
  with no public seam; reachable only through a compiled wasm guest fixture that doesn't
  exist (building one is out of test-only scope). Already pinned by the in-crate unit test
  `from_hex_panics_on_even_byte_multibyte_input`. Not re-implemented (would test a copy).
- **`uni-query` Locy-runtime findings ([10],[13],[24],[25])** — these are fold/fixpoint/
  query-execution bugs whose end-to-end output surfaces only in crate `uni` (`uni-db`),
  which `uni-query` cannot depend on (circular). In addition to the documenting tests on
  the reachable `uni-query` path, **true end-to-end repros now live in the `uni-db` test
  tree** (`crates/uni/tests/common/locy/repro_locy_runtime_*.rs`, wired in `mod.rs`):
    - **[10] ABDUCE `target_var`** (`repro_locy_runtime_abduce_target_var.rs`) —
      **reproduces cleanly.** A 2-hop `ABDUCE` over `(a)-[:R1]->(b)-[:R2]->(c)` emits
      AddEdge pairs `[("a",""),("b","b")]` (empty target on the first, a `(b,b)` self-loop
      on the second) instead of `(a,b),(b,c)` — the `candidates.last_mut()` mis-attribution.
    - **[25] `RETURN DISTINCT`** (`repro_locy_runtime_distinct_debug_dedup.rs`) —
      **reproduces cleanly.** 48 rows sharing `(color,shape)` under `RETURN DISTINCT color,
      shape` yield **2** rows (should be 1); the `format!("{row:?}")` HashMap-Debug key
      splits identical content across iteration orders. Reliable (>1), not flaky.
    - **[24] TopKProofs MNOR empty-clause → 1.0** (`repro_locy_runtime_topk_mnor_mixed_support.rs`)
      — **defect real, latent e2e.** The 1.0 collapse needs a KEY group mixing a supported
      and an unsupported row, but support resolution keys on a group-invariant column, so a
      group is uniformly supported or unsupported — the precondition is unreachable via the
      public FOLD surface. A passing test pins the DNF branch executing (shared-base group
      → 0.7 ≠ 0.88 independence); the correctness assertion is `#[ignore]`d with the reason.
    - **[13] exact-WMC positional mis-group** (`repro_locy_runtime_wmc_shared_lineage.rs`)
      — **defect real, latent e2e.** The raw-yield-position read only diverges when the
      pre-fold batch column order differs from the yield schema, which reachable shapes
      don't force; the diamond yields 0.79, which the Locy TCK codifies as the correct
      exact-mode answer. A passing test pins `apply_exact_wmc`/`detect_shared_lineage`
      executing (0.79 + `SharedProbabilisticDependency` warning); the divergence assertion
      is `#[ignore]`d.

  Each file ships a passing observation test (proves the buggy path runs) plus an
  `#[ignore]`d correctness test (pins the defect / correct behavior). Normal `cargo test`
  is green (4 passed, 4 ignored); the ignored ones fail under `--include-ignored` as
  documented. No passing assertion was fabricated for the two latent defects.
- **`uni-query` [5],[16],[35],[14],[17],[32]** — the schemaless planner takes a non-buggy
  path for the query shapes reachable here; tests document the trigger and stand as
  regression guards.

## Verification method

- One independent verifier per finding re-derived the verdict from current source, ignoring
  the original scan's classification.
- Repros were compile-verified per crate; the heavy crates (`uni-query`, `uni-store`,
  `uni-db`) were additionally re-verified by forced rebuilds (defeating Cargo's fingerprint
  cache) after several rust-analyzer `E0308` diagnostics proved to be false positives on
  `Arc<T>`→`Arc<dyn Trait>` unsizing coercions that `rustc` resolves.
- No production source was modified; only test files + test module wiring. No commits.
