# Plugin Framework — Implementation Gap Analysis

**Status:** Review finding — not yet actioned
**Date:** 2026-05-31
**Reviewer scope:** Implementation as it stands on branch `feat/ssi-release-test-suite`
(the `plugin-fw` stack is FF-merged and sits beneath the SSI/OCC work).
**Source of truth reviewed against:** `docs/proposals/plugin_framework.md` (v1.2.0,
status block dated 2026-05-24, acceptance criteria annotated 2026-05-26).

---

## 1. Purpose & method

This document records the delta between what `plugin_framework.md` *claims*
(its §19 acceptance scorecard and the §16 "Implementation status" block) and
what the code *actually does today*. Every claim was re-verified by direct
inspection — greps, call-site tracing, and reading the execution paths — rather
than trusting the self-assessment. Each gap below carries file:line evidence so
it can be confirmed independently.

The headline conclusion: **the foundation is real and load-bearing**, the
self-assessment is **largely honest**, but there was **one genuine
integrity-invariant violation at execution time** (G1 — **now fixed, 2026-05-31**),
two **undisclosed surface gaps** (G2, G3), and a cluster of
**documentation-drift discrepancies** (G4) where the proposal now
*under*-reports the shipped state.

**Update 2026-05-31 (release-honesty pass):** G2, G3, and G4 have since been closed
**as documentation** — `config_param` struck from §3.1, the v1 loader-surface scope
disclosed in §1/§5, two ⏳ §19 criteria (29, 30) added to track the deferred
implementations, and the §19 scorecard counts refreshed to match the tree. No
production code changed; the real `config_param` / additional-WASM-world builds are
scoped post-release tickets. See each gap's resolution note and §5.

Severity legend:
- 🔴 **High** — a verified ✅ claim does not hold at runtime, or a load-bearing
  invariant is violated.
- 🟡 **Medium** — a documented surface is undelivered or materially narrower than
  described, and this is *not* disclosed in §19.
- 🟢 **Low** — documentation drift: the code is correct; the proposal text is
  stale or inconsistent.
- ⚪ **Info** — genuinely open work, already disclosed with a ▶/⏳ rating.

---

## 2. What was verified as genuinely correct

Recorded for completeness so the gaps below are read in proportion. All of the
following claims were re-checked and **hold**:

| Claim | Evidence |
|-------|----------|
| 25 surface traits exist as real `pub trait`s | `crates/uni-plugin/src/traits/` (scalar.rs, aggregate.rs, window.rs, procedure.rs, locy.rs, operator.rs, index.rs, storage.rs, algorithm.rs, crdt.rs, hook.rs, trigger.rs, background.rs, types.rs, connector.rs, collation.rs, cdc.rs, catalog.rs) |
| `Plugin` trait + supporting types | `plugin.rs:137`; `manifest.rs:158` (`PluginManifest`), `manifest.rs:39` (`AbiRange`), `qname.rs:54` (`QName`), `capability.rs:169` (`CapabilitySet`) |
| `FoldAggKind` enum deleted | `grep -rn FoldAggKind crates/` → 1 hit, a comment at `uni-locy/src/semiring.rs:7` |
| Procedure dispatch collapsed, zero hardcoded qname arms | `uni-query/src/query/df_graph/procedure_call.rs:591` (`execute_procedure` = `registry.resolve_user_procedure ? invoke : tck_fallback`) |
| Single registration path at `Uni::build` | `crates/uni/src/api/mod.rs:117` (`register_builtin_plugins`), called at `mod.rs:2989` |
| Legacy scalar-UDF path removed | `register_custom_udfs` → 0 hits; `register_plugin_scalar_udfs` sole adapter at `read.rs:438,446` |
| WASM CM loader is real wasmtime Component Model (not stubbed) | `uni-plugin-wasm/src/loader.rs:443` (two-pass), `linker.rs:42` (host-fn omission), `multi_version.rs` (`(major, caps_signature)` keying + 5 tests) |
| Extism capability filtering real | `uni-plugin-extism/src/host_fns.rs:42` (`required_capability`), `loader.rs:184` (`prepare_parsed` filter) |
| Instance pool warms eagerly | `uni-plugin-wasm-rt/src/pool.rs:125` |
| Cross-ABI byte-identical parity is a real test | `crates/uni/tests/m6_cross_abi_parity.rs::cross_abi_haversine_results_match` |
| Cited integration tests exist with real bodies, zero `#[ignore]` | all §19-cited test fns confirmed present; static count ≈395 test attributes across 11 plugin crates |

---

## 3. Gaps

### G1 ✅ FIXED (2026-05-31) — non-recursive `FOLD` now dispatches through the `LocyAggState` trait

**Resolution.** The `LocyAggregate`/`LocyAggState` trait gained a per-group,
context-aware primitive (`ingest_indices` + `FoldContext { strict, epsilon,
semiring }` + `output_type_for_input`); the byte-identical fold math moved into
the built-in states in `uni-plugin-builtin` (noisy-OR complement-product,
bounded-product log-space, `MIN`/`MAX` input-type preservation, `COLLECT`
cypher-codec `LargeBinary`); a dedicated `COUNTALL` aggregate was registered so
trait dispatch needs no name special-case; and `compute_fold_aggregate`
(`locy_fold.rs`) is now a generic driver: `create() → ingest_indices() →
finalize()` per group, assembled via `ScalarValue::iter_to_array`. The
`_ => Err("unsupported aggregate")` arm is gone — user aggregates run. The
TopKProofs DNF path remains an executor-side specialization layered above MNOR
(per the agreed design). Verified byte-identical: uni-plugin/builtin 195,
uni-query 940 (incl. the ~50 fold unit tests + a new
`user_defined_aggregate_runs_in_non_recursive_fold` regression), Locy TCK 497,
Cypher TCK 3934, uni integration 1689 — all green; clippy clean. One
intentional, untested-path change: `MIN`/`MAX` over *non-numeric* columns now
compare via native `ScalarValue` ordering instead of the old `Debug`-string
quirk.

The original finding (for the record):

#### Locy fold trait object was cosmetic on the non-recursive path; the columnar `LocyAggState` contract was dead at runtime

**Proposal claim.** §1 / §4.4 / criterion 7 (✅) / criterion 18 (✅): every
built-in aggregate flows through `Arc<dyn LocyAggregate>`; "built-ins
[are] indistinguishable from user plugins"; a user-defined `LocyAggregate`
"participates in a recursive Locy stratum correctly." The design centers on the
columnar contract `LocyAggregate::create() -> Box<dyn LocyAggState>` then
`LocyAggState::{ingest, merge, finalize}` (§4.4).

**Reality.** There are two distinct fold execution paths, and they treat the
trait object very differently:

1. **Recursive fixpoint path** (`uni-query/src/query/df_graph/locy_fixpoint.rs`)
   — *does* call the trait, but only its **scalar-f64 helper methods**:
   - `binding.aggregate.update_step(*entry, val, strict)` — `locy_fixpoint.rs:256`
   - `binding.aggregate.initial_accum_f64()` — `locy_fixpoint.rs:211`
   - `binding.aggregate.is_probability_aggregate()` — `locy_fixpoint.rs:225, 2149, 2460`
   - `binding.aggregate.is_noisy_or()` — `locy_fixpoint.rs:229, 2465`

2. **Non-recursive batch FOLD path** (`FoldExec::execute`,
   `uni-query/src/query/df_graph/locy_fold.rs:467`) — **ignores the trait object
   entirely** and dispatches on the name string:
   ```rust
   let agg_col = compute_fold_aggregate(
       col.as_ref(),
       binding.name.as_str(),   // <-- only the name string is used
       ...
   )?;
   ```
   `compute_fold_aggregate` (`locy_fold.rs:525`) is a hardcoded
   `match name { "SUM" | "COUNT" | "MAX" | "MIN" | "AVG" | "COLLECT" | "MNOR" |
   "MPROD" => ..., other => Err("compute_fold_aggregate: unsupported aggregate
   `{other}`") }` (`locy_fold.rs:621`).

**The headline columnar contract is never invoked in production code.**
`create()` / `ingest()` / `merge()` / `finalize()` appear only in the trait
definition (`uni-plugin/src/traits/locy.rs:36,113,121,128`) and in a
`#[cfg(test)]` impl (`locy_fold.rs:2655`). No production call site exists.

**Impact.**
- A genuinely new user-registered `LocyAggregate` (e.g. `geo.union_bbox`,
  `MYAGG`) used in a **non-recursive** `FOLD x AS y` resolves at planner time,
  is stored in `FoldBinding.aggregate`, then **hits the `_ => Err("unsupported
  aggregate")` arm** at execution. It cannot run. The "indistinguishable from
  built-ins" invariant fails at execution time.
- §19 criterion 18 characterizes `compute_fold_aggregate` as a "compute
  *fallback*." For the non-recursive FOLD path it is not a fallback — it is the
  *only* executor. The closed enum was deleted as a *type*; its dispatch logic
  survives verbatim as a string match.
- §19 criterion 7 (✅) can only be true via the f64 `update_step` helper. A user
  aggregate that requires columnar/multi-field state (the very thing §4.4's
  `state_fields` / `ingest` / `merge` design exists for) has **no runtime path**.

**Required action — pick one:**
- **(a) Close the gap (preferred, matches the proposal's integrity invariant):**
  route `compute_fold_aggregate` through `binding.aggregate.create()` +
  `LocyAggState::ingest/merge/finalize` so built-ins and user plugins share one
  executor. Keep the existing hardcoded bodies only as the registered built-in
  `LocyAggState` impls in `uni-plugin-builtin`, not as an executor branch.
- **(b) Re-document honestly:** downgrade §4.4, criterion 7, and criterion 18 to
  state that columnar `LocyAggState` is **registration-only** today, that
  non-recursive FOLD is **closed to user aggregates**, and that recursive folding
  supports user aggregates only through the scalar `update_step` helper.

**Acceptance test to add either way:** register a brand-new `LocyAggregate`
whose computation *requires* `LocyAggState` (not expressible via `update_step`),
then `FOLD … AS … WITH MYAGG` both inside and outside a recursive stratum, and
assert correct output (option a) or a clean, documented rejection (option b).
Today the behavior is an opaque "unsupported aggregate" error on the
non-recursive path.

---

### G2 ✅ RESOLVED (doc) 2026-05-31 — `config_param` struck from §3.1, tracked as §19 criterion 29

**Resolution.** `config_param` and the full `ConfigParam` struct were removed from
`plugin_framework.md` §3.1 (they specified an unimplemented surface, so §3.1 now
reflects the 16 registrar methods that actually exist), and a new §19 ⏳
**criterion 29** tracks the GUC config-parameter story (`config_param` +
`SHOW`/`SET <plugin>.<name>` + `host.config_get`) as deferred-pending-need — to be
designed against the first plugin that needs a tunable. Building it now was judged
the wrong release-window risk (it would touch the query parser and all five
loaders) for a feature with no current consumer; a registration-only half-build was
rejected because it would re-create the cosmetic-at-registration / dead-at-runtime
anti-pattern that G1 just eliminated. The original finding follows.

**Proposal claim.** §3.1 lists `config_param(&mut self, param: ConfigParam) ->
&mut Self` among the registrar methods and fully specifies the `ConfigParam`
struct (PostgreSQL-GUC-style config: `SHOW`/`SET <plugin>.<name>`, read via
`host.config_get`).

**Reality.** `PluginRegistrar` (`crates/uni-plugin/src/registrar.rs`) implements
16 of the 17 listed methods. `config_param` is **absent**. No `ConfigParam`
plumbing exists on the registrar. (The registrar does add 9 *extra* methods for
surfaces #4/#12/#15/#16/#21–#24, so the surface is otherwise richer than the
proposal — this is the one method that regressed.)

**Impact.** Plugins cannot declare configuration parameters. The entire
GUC-style config story (`SHOW`/`SET`, validation closures, scope) is
undelivered. This is **not** mentioned anywhere in §19 — it reads as complete.

**Required action.** Either implement `config_param` + a config registry +
`SHOW`/`SET` wiring + `host.config_get`, or strike `ConfigParam` from §3.1 and
add a §19 ⏳ criterion for it so the omission is tracked.

---

### G3 ✅ RESOLVED (doc) 2026-05-31 — scope caveat added, tracked as §19 criterion 30

**Resolution.** A "v1 loader scope" caveat was added to `plugin_framework.md` §1 and
§5.1 stating the non-Rust loaders (CM / Extism / PyO3 / Rhai) author
scalar/aggregate/procedure in v1 and the other 22 surfaces are
compile-time-Rust-only; §19 gained ⏳ **criterion 30** capturing the coverage gap;
and both the §10 WIT-world preview and Appendix A now carry explicit
"planned, not yet implemented — only `scalar-plugin`/`aggregate-plugin`/`procedure-plugin`
exist today" markers. No WIT world was built: `operator`/`storage` are genuinely
infeasible across the Component Model (in-process trait objects, `&Expr` trees, async
streams) and the tractable ones (`crdt`/`connector`, ~250–350 LOC each) have no
waiting consumer — both are deferred pending real need. The original finding follows.

**Proposal claim.** §1: "five loaders … all converge on the same in-process
registry" and the framework opens "every extensibility surface … from day one."
The framing implies any surface is authorable in any loader.

**Reality.** `crates/uni-plugin-wasm/wit/world.wit` defines exactly three worlds:
`scalar-plugin`, `aggregate-plugin`, `procedure-plugin`. Appendix A's
`locy-agg-plugin` and `operator-plugin` worlds, plus the "…index-plugin,
storage-plugin, algo-plugin, crdt-plugin, hook-plugin, type-plugin, auth-plugin,
authz-plugin, connector-plugin all follow the same pattern" note, are **not
implemented**. Consequently:
- **WASM (CM + Extism), Rhai, and PyO3 can author only scalar functions,
  aggregates, and procedures.**
- **Only compile-time Rust plugins can register the other 22 surfaces** (Locy
  aggregates, operators, optimizer rules, index kinds, storage backends,
  algorithms, CRDTs, hooks, triggers, background jobs, logical types, auth,
  authz, connectors, collations, CDC, catalog, replacement scans, Pregel).

**Impact.** The proposal's "five loaders for every surface" promise is true only
for the columnar fn/aggregate/procedure trio. A plugin author wanting a
sandboxed WASM storage backend or CRDT cannot write one today. This is a
reasonable milestone boundary, but it is **not flagged** — §19 has no criterion
capturing "non-fn surfaces are Rust-only in WASM."

**Required action.** Add a scope caveat near §1/§5 (and a §19 ⏳ criterion):
"WASM/Extism/Rhai/PyO3 loaders are scoped to scalar/aggregate/procedure surfaces
in v1; the remaining 22 surfaces are compile-time-Rust-only pending their WIT
worlds." Optionally prioritize storage/index/CRDT WASM worlds if sandboxed
authoring of those is a real near-term need.

---

### G4 ✅ ADDRESSED (doc) 2026-05-31 — §19 / status text refreshed to match reality

**Resolution.** Applied to `plugin_framework.md`: G4.1 — criterion 12 reworded to
"all 38 APOC procedures across 6 namespaces have real bodies" (long tail still
absent, ▶ retained); G4.2 — criterion 1 corrected to **5 CRDTs** (adds RGA) and
**5 collations**, reconciling with the status header; G4.3 — the two dead test-path
citations updated to `tests/common/dispatch/ephemeral_entities.rs` (12) and
`tests/common/planner/pushdown_test.rs` (18, not 10/10); G4.4 — test counts refreshed
to ≈497 static attributes across 11 plugin crates (the prior "269"/"336+" were stale);
G4.5 — `procedure_call.rs` count corrected from the stale 922 to the actual 1115
(2309 → 1115) at both §16 and criterion 3. The original findings follow.

The status block is dated 2026-05-24; the code advanced. None of these are code
bugs — they are stale/incorrect proposal text that should be corrected so the
document stays trustworthy.

| # | §19 text | Reality | Evidence |
|---|----------|---------|----------|
| G4.1 | Criterion 12: "1 namespace has real coverage — `apoc.bitwise.*`; the remaining namespaces are enumerated." | **All 38 procedures across 6 namespaces have real bodies** (bitwise 6, text 13, math 10, number 3, convert 4, create 2). | `uni-plugin-apoc-core/src/procedures/{bitwise,text,math,number,convert,create}.rs` — each `invoke` does real work (e.g. `text.toUpper` calls `to_uppercase()`). The ▶ rating is still *fair* because the APOC long tail (refactor/load/export/periodic.iterate/cypher.run/es/mongo/bolt/path.expand) is genuinely absent — only the "only bitwise" wording is wrong. |
| G4.2 | Criterion 1: "4 CRDTs (LWW / OR-Set / G-Counter / MV-Register), 3 collations." | **5 CRDTs** (adds RGA) and **5 collations** (ASCII-sensitive, ASCII-insensitive, Unicode-codepoint, Unicode-case-insensitive, Natural-numeric). | `uni-plugin-builtin/src/crdts.rs:34`, `collations.rs:19`. Internally inconsistent: the status block at proposal line 28 already says "5 CRDTs." |
| G4.3 | Criterion 16 cites `crates/uni-query/tests/ephemeral_entities.rs`; criterion 17 cites `crates/uni-query/tests/pushdown.rs`. | Both paths **do not exist**; tests moved into `tests/common/...` by the suite-consolidation commit `8a7336a77`. Actual: `tests/common/dispatch/ephemeral_entities.rs` (12 tests) and `tests/common/planner/pushdown_test.rs` (18 tests, not "10/10"). | grep for the cited paths returns nothing; tests confirmed at the new locations. |
| G4.4 | "269 plugin-crate tests"; "336+ plugin-crate tests across the 11 plugin crates." | Static attribute count ≈ **395** across 11 crates; two crates (`uni-plugin-wasm-rt`, `uni-plugin-host`) are workspace members not mentioned in the per-crate breakdown. | `grep -rE "#\[(test|tokio::test|rstest)" <crate>` per crate. (Static count ≥ runtime pass count; directionally the suite grew.) |
| G4.5 | `procedure_call.rs` "922 lines, down from 2309." | **1115 lines** today (grew since the doc; still far below 2309 and still arm-free). | `wc -l` |

**Required action.** Refresh §19 counts, APOC criterion wording, and the two test
path citations. Reconcile the CRDT/collation counts between the status block and
criterion 1.

---

## 4. Open items (disclosed — informational, no action implied by this doc)

These carry ▶/⏳ ratings in §19 and match reality; listed so the gap doc is
exhaustive:

- ⚪ CLI surface `uni plugin {install,list,grant,remove,info,reload,verify,help,declared …}` (criterion 11, ⏳ M12).
- ⚪ OCI install `oci://…` (criterion 22, ⏳) and Extism Hub install `extism://hub/…` (criterion 23, ⏳).
- ⚪ CM capability-gated host-fn body + grant/deny e2e (criterion 6 CM half, ▶/⏳ — `linker.rs` accepts `effective_caps` but currently `let _ = effective_caps;`).
- ⚪ Multi-major two-`.wasm` end-to-end in one query (criterion 10, ▶ — host contract + tests shipped, real v2 artifact pending).
- ⚪ Perf p99 < 50 µs benchmark (criterion 19, ▶ — functional path + warm pool shipped, measurement not run) and the broader `plugin_perf.rs` regression suite (§16.5).
- ⚪ Secrets non-serialization WIT membrane (criterion 20, ▶ — `SecretStore` real; WIT-level leak rejection deferred to Phase D).
- ⚪ OTel plugin-side `host.span_*` imports (criterion 21, ▶ — host OTel layer shipped; plugin propagation deferred).
- ⚪ Ed25519 signature verification behind the `ed25519` feature flag (`uni-plugin/src/verify.rs:84`); Blake3 hash pinning is unconditional and real.
- ⚪ `statistics_refresh` background job is a stub (pending a planner statistics-refresh API); `uni.periodic.iterate` is single-pass v1.
- ⚪ Rhai wall-clock deadline (`WallClockMillisPerCall`) — `Engine::on_progress` hook in place, host-side driver deferred. (Fuel + call-depth limits are real: criterion 28 ✅.)

---

## 5. Prioritized remediation plan

| Priority | Gap | Action | Rough size |
|----------|-----|--------|-----------|
| ~~P0~~ ✅ | G1 | **DONE 2026-05-31** — routed `compute_fold_aggregate` through `LocyAggState` (option a); byte-identical, novel-user-aggregate test added. | landed |
| ~~P1~~ ✅ | G2 | **DONE (doc) 2026-05-31** — struck `config_param`/`ConfigParam` from §3.1; added ⏳ §19 criterion 29. Real implementation deferred to a post-release ticket (do the **full** GUC path, never registration-only) when a plugin needs a tunable. | landed (docs) |
| ~~P1~~ ✅ | G3 | **DONE (doc) 2026-05-31** — added the v1 loader-scope caveat to §1/§5; added ⏳ §19 criterion 30; marked the §10/Appendix-A WIT worlds "planned, not implemented." `crdt`/`connector` WIT worlds deferred pending need; `operator`/`storage` are CM-infeasible. | landed (docs) |
| ~~P2~~ ✅ | G4 | **DONE (doc) 2026-05-31** — refreshed §19 counts (≈497 tests, 1115-line `procedure_call.rs`), APOC wording (38 procs / 6 namespaces), the two test-path citations; reconciled CRDT (5) / collation (5) counts. | landed (docs) |

**G1 was the most important item** — the one place a §19 ✅ ("verified") claim
did not hold at runtime — and it is now **fixed (2026-05-31)**: the fold
executor genuinely dispatches through the `LocyAggState` trait, built-ins and
user plugins share one execution path, and byte-identical output is verified
across the full Locy/Cypher TCK + integration suites. The remaining gaps — the
undisclosed-but-bounded surface gaps (G2, G3) and documentation hygiene (G4) — were
closed **as documentation on 2026-05-31**: §3.1 no longer advertises an unbuilt
`config_param`, §1/§5 disclose the v1 loader-surface scope, two new ⏳ §19 criteria
(29, 30) track the deferred implementations, and the §19 scorecard counts now match
the tree. The proposal is release-honest; the actual `config_param` and additional
WASM-world implementations remain as scoped post-release tickets driven by real need.

---

## Appendix — key evidence anchors

```
# Closed enum gone, but logic survives as a string match
crates/uni-locy/src/semiring.rs:7                      # only FoldAggKind hit (comment)
crates/uni-query/src/query/df_graph/locy_fold.rs:139   # FoldBinding.aggregate: Arc<dyn LocyAggregate>
crates/uni-query/src/query/df_graph/locy_fold.rs:467   # FoldExec passes binding.name.as_str(), not the trait
crates/uni-query/src/query/df_graph/locy_fold.rs:525   # fn compute_fold_aggregate (hardcoded match)
crates/uni-query/src/query/df_graph/locy_fold.rs:621   # _ => Err("unsupported aggregate")
crates/uni-query/src/query/df_graph/locy_fold.rs:2655  # only LocyAggState impl is #[cfg(test)]
crates/uni-plugin/src/traits/locy.rs:36,113,121,128    # create/ingest/merge/finalize — prod-unused

# Recursive path uses only the scalar f64 helpers
crates/uni-query/src/query/df_graph/locy_fixpoint.rs:211,225,229,256

# Registrar method gap
crates/uni-plugin/src/registrar.rs                     # 16/17 methods; no config_param

# WASM worlds
crates/uni-plugin-wasm/wit/world.wit                   # scalar/aggregate/procedure only

# Single registration path (verified correct)
crates/uni/src/api/mod.rs:117,2989
crates/uni-query/src/query/executor/read.rs:438,446
```
