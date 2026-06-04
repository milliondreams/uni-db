# Code-Simplifier Review — 2026-06-02

> **EXECUTED 2026-06-02.** All findings below were applied across the 26 reviewed
> crate sections (the 5 generated python wheel-variants were excluded). A few were
> deliberately **declined with rationale** — they would have been disimprovements
> or behavior changes on stable surfaces (noted inline / in the summary below).
> The work is verified against the full local CI surface: `fmt --all` clean,
> workspace clippy `-D warnings` clean, workspace nextest 4660 pass, TCK 3925×2 +
> Locy TCK 497×2, cloud (LocalStack) 17+1, ONNX bundled 28 + dynamic 25, `doc -D
> warnings` clean, uni-db pytest 807, uni-pydantic 213, 6 flagship notebooks, and
> packaging dry-runs. A latent FU-2 security bug (`decode_batch` skipping the
> secret-handle membrane) was fixed in passing. Changes are **uncommitted**; the
> breaking `pub` removals warrant a 1.3.0 → 1.4.0 minor bump at commit time.
>
> **Declined (with reason):** unifying the `Value` i32/i64 float→int policy (the
> asymmetry is intentional — i64 backs Cypher `toInteger` truncation, i32 is
> strict typed coercion; documented instead); removing the unused `TemporalValue`
> temporal-accessor family (a coherent public API, not dead code); collapsing the
> feature-gated `Lance`/`Storage` pushdown markers (cfg-divergent); folding
> `MergeResult` (consumed by multiple external test suites).

Workspace-wide simplification review. One `code-simplifier` subagent was run per
crate (review-only; no edits), across **28 crates** in 3 waves. The 5 Python
wheel-variant crates (`uni-python-{cuda,metal,onnx,onnx-cuda,onnx-metal}`) are
generated shims of `bindings/uni-db` and were not reviewed separately (same as
the CI exclude set).

All findings are **behavior-preserving** suggestions. Each is tagged
`[high|med|low]`. Severity reflects maintenance/clarity value, not urgency —
nothing here is a correctness bug (the few correctness-adjacent observations are
called out inline). Capability/attenuation/security checks were explicitly out of
scope for loosening and none are suggested for change.

> This document is advisory. Treat the **Cross-cutting themes** as the highest-leverage
> work (one fix retires duplication in several crates at once); the per-crate
> sections are the raw findings.

---

## Cross-cutting themes

These patterns recur across multiple crates — fixing them once (often via a shared
helper) removes duplication in several places and the associated drift risk.

1. **Dead `NotYetImplemented` variant + `not_yet()` constructor.** Now that every
   loader is real, this error variant is unused in `uni-plugin-rhai`,
   `uni-plugin-extism`, `uni-plugin-wasm`, and `uni-plugin-pyo3` (the `lib.rs`
   "load() returns NotYetImplemented" doc comments are stale). Remove the
   constructor; drop the variant unless a `#[non_exhaustive]` slot is wanted.

2. **Adapter helper duplication across loaders.** `acquire(pool)` (→
   `CODE_RESOURCE_LIMIT`), the IPC-error→`FnError` mapper
   (`extism_err_to_fn_err`/`ipc_to_fn_err`), and the qname→export-symbol
   sanitizer (`qname.replace('.', "_")`) are copy-pasted across the three adapter
   files in **both** `uni-plugin-extism` and `uni-plugin-wasm`; `classify_pyerr`
   is triplicated in `uni-plugin-pyo3`. Hoist into a shared `adapter_common`
   module (per-crate, or extend `uni_plugin::adapter_common`).

3. **Extism⇄WASM wire-translation duplication.** `wire_fn_sig_to_internal` /
   `wire_agg_sig_to_internal` / `wire_proc_sig_to_internal` and the
   volatility/null-handling/proc-mode string→enum matches are near-line-for-line
   duplicated between `uni-plugin-extism/src/wire_translate.rs` and
   `uni-plugin-wasm/src/loader.rs`. Lift the pure string→enum parsers into
   `uni_plugin::adapter_common`.

4. **Host-fn body prelude duplication.** The "allow-list check → service-present
   check → dispatch → map_err" prelude repeats across the kms/secret/net host-fn
   bodies in `uni-plugin-rhai` and `uni-plugin-extism`. `uni-plugin-extism`
   already factored `http_dispatch_json` (Gap-2 work) — generalize it to a
   `dispatch_json<Req,Resp>(ctx, json, label, f)` covering kms/secret too; add
   `require_allowed`/`require_service` guard helpers in Rhai.

5. **Signature double-construction (registrar vs runtime).** Procedures build
   their `ProcedureSignature` once inline in `register_into` and again in the
   cached `signature()` accessor — the registrar gets a freshly-allocated,
   sometimes slightly-divergent signature from the one the plugin later hands
   out. Seen in `uni-plugin-builtin` (`system.rs`, `periodic.rs`, `scalar_fns`)
   and `uni-plugin-apoc-core` (per-variant `OnceLock` statics in all six modules).
   Register with the cached signature; cache once per enum, not per variant.

6. **Sync/async mirror duplication.** `bindings/uni-db` `sync_api.rs` and
   `async_api.rs` mirror each other; the async side never got the helpers the
   sync side has. Param-conversion (`HashMap<String,Py> → HashMap<String,Value>`,
   ~15×), the "transaction already completed" guard (~22×, sync has
   `check_active`), `write_lease` mapping, and `LabelInfo`/`EdgeTypeInfo`
   construction are all duplicated. Shared converters remove ~250 lines.

7. **Struct-literal repetition across constructors.** Multi-field structs are
   re-spelled in each constructor/clone, so a new field must be edited in 2–4
   places or silently diverges: `UniInner::{at_snapshot,at_fork}` and the three
   `Session` constructors + `Clone` (`crates/uni`), `AsyncDatabaseBuilder` (×5),
   and the wasm/extism bootstrap-`Prepared*` literal. Introduce a private base
   constructor / `Default` + `..base` spread.

8. **Dead/vestigial code islands** (safe to delete; git preserves history):
   - `uni-bulk`: the entire `Arc<AtomicBool>` write-guard machinery is vestigial —
     every live entry point uses the unguarded/pre-acquired path
     (`new_deferred`/`new_with_guard` unreachable; `Drop` guards never fire).
   - `uni-query`: `locy_program.rs` `execute_query_inline` + `needs_node_enrichment`
     + `expr_has_property_access` (~150-line `#[allow(dead_code)]` cluster).
   - `uni-locy-tck` & `uni-tck`: unused `matcher` + `parser` modules and the
     `SideEffects`/`QueryResult` plumbing in `world.rs` (~430 + ~250 + ~250 lines)
     — confirm not staged scaffolding before removing.
   - `uni-locy`: the "Stage 2" tag-flow surface (`plus_tag`/`times_tag`/…/
     `AggregatorValue`) is untested-in-production API pending Stage 2.

9. **Newtype / alias duplication.** `Vid` and `Eid` are byte-identical newtypes
   (`uni-common`); `LabelSnapshot`/`EdgeSnapshot` identical structs; `NodeRow`/
   `EdgeRow` (`uni-algo`) are two aliases for the same `HashMap` (false type
   safety). Use a generating macro for the ID types (keeps the distinct types);
   collapse the snapshot/row aliases.

10. **Misattributed / stale doc comments.** Doc blocks attached to the wrong
    function or describing absent behavior: `uni-store/storage/manager.rs`
    (write_batch doc on merge_insert), `uni-plugin-builtin/optimizer/pushdown_negotiation.rs`,
    `bindings/uni-db/sync_api.rs:622` (default-grant doc on `wasm_outcome_to_pydict`),
    `uni-plugin-conformance` (semver wording on a check that only inspects `id`),
    `uni-plugin-apoc-core/procedures/mod.rs` (inventory omits number/convert/create),
    `uni-algo/procedures.rs:230` (obsolete "Phase 3.3 placeholder"). Re-attach or
    delete.

---

## Per-crate findings

### Foundation

## uni-common

- **[high]** `crates/uni-common/src/core/schema.rs:1067-1188` — _duplication_: `add_label`/`add_label_with_desc`, `add_edge_type`/`add_edge_type_with_desc`, `add_property`/`add_property_with_desc` are copy-paste pairs differing only in threading `description`. Collapse each non-desc method to a one-line delegation. This also fixes a latent inconsistency: `add_edge_type` guards exhaustion against `VIRTUAL_EDGE_TYPE_ID_START` while `add_edge_type_with_desc` guards against `MAX_SCHEMA_TYPE_ID` (1137 vs 1173) — delegation forces one bound.
- **[med]** `crates/uni-common/src/core/id.rs:18-212` — _duplication_: `Vid` and `Eid` are byte-for-byte identical newtypes (~100 lines). Use a `define_id_newtype!` macro to keep the two distinct types with one source of truth.
- **[med]** `crates/uni-common/src/core/snapshot.rs:22-34` — _duplication_: `LabelSnapshot` and `EdgeSnapshot` are structurally identical (`version,count,lance_version`); collapse to one `EntitySnapshot`.
- **[med]** `crates/uni-common/src/value.rs:993-1044` — _inconsistency_: the `i32 TryFrom` arm inlines `UniError::Type` three times instead of the `type_error` helper, and `i64` silently truncates `Float(f)` (`*f as i64`) while `i32` rejects out-of-range/fractional floats. Pick one float→int policy and share the range check.
- **[low]** `crates/uni-common/src/value.rs:226-269` — _API surface_: `offset()`/`epoch_seconds()`/etc. are thin wrappers; grep for unused ones (`offset_seconds_value`, `week_year`, `day_of_quarter`) and drop if test-only.
- **[low]** `crates/uni-common/src/cypher_value_codec.rs:94-97,304-333,469-473` — _inconsistency_: encode-infallibility panics mix `unwrap_or_else(|_| panic!)`, `.expect(...)` fast-path, and BTIC `.expect(...)`. Route through one `encode_msgpack`/helper idiom.
- **[low]** `crates/uni-common/src/core/schema.rs:913-989,1192-1383` — _inconsistency_: poisoned-lock handling split between `acquire_read`/`acquire_write` helpers and ad-hoc `.read().expect("Schema lock poisoned…")` (duplicated 4+×). Standardize on the helpers.
- **[low]** `crates/uni-common/src/value.rs:1148-1185` — _duplication_: owned `TryFrom<Value> for Vec<T>`/`Option<T>` impls can delegate to the borrowed `&Value` impls.
- _Intentional (leave):_ `SchemaDelta`/`core/fork.rs` empty-delta paths, `cypher_value_codec.rs:68` reserved `TAG_POINT`.

## uni-sidecar

- **[med]** `crates/uni-sidecar/src/lib.rs:206-223` — _error boilerplate_: `store` repeats the `.map_err(|source| SidecarIoError::Write { path: p.clone(), source })` closure 5×. Extract `fn write_err(path) -> impl FnOnce(io::Error) -> SidecarIoError`.
- **[med]** `crates/uni-sidecar/src/lib.rs:161-170` — _needless complexity_: `load` does `exists()` then `read`, double-statting with a race window. Read directly and map `ErrorKind::NotFound` to the default.
- **[low]** `crates/uni-sidecar/src/lib.rs:233-241` — _duplication_: `sync_parent_dir` re-derives the empty-parent guard already in `store`; pass the resolved parent in.
- **[low]** `crates/uni-sidecar/src/lib.rs:204` — _note (intentional)_: fixed `.tmp` temp path races under concurrent `store` to the same sidecar; consistent with the documented "caller serializes writes" contract. Optional hardening: unique suffix.
- _Intentional (leave):_ the `PhantomData<fn() -> T>` generality (documented).

## uni-btic

- **[med]** `crates/uni-btic/src/parse.rs:249-274,154` — _dead code_: `strip_timezone` computes an `offset_secs` that every caller discards (`_tz_offset_secs`), so a `+05:00` offset is silently treated as UTC. Either apply the offset or simplify `strip_timezone` to strip only `Z` and return `&str`.
- **[low]** `crates/uni-btic/src/btic.rs:43-46` — _dead code_: `new_unchecked` is `pub(crate)`, `#[allow(dead_code)]`, no callers. Remove.
- **[low]** `crates/uni-btic/src/btic.rs:139-141` — _API surface_: `version()` is invariantly `0` (INV-4) and used only in a self-test; drop or mark forward-compat.
- **[low]** `crates/uni-btic/src/set_ops.rs:104-120` — _unclear control flow_: `pick_bound_meta`'s guard arm `ord if ord == pick` shadows the `Equal` arm; relies on callers never passing `Equal`. Restructure or document the precondition.
- **[low]** `crates/uni-btic/src/parse.rs:100-108` — _needless complexity_: `strip_bce_suffix` hand-rolls two near-duplicate length branches; collapse via `trim_end` + case-insensitive `strip_suffix`.
- **[low]** `crates/uni-btic/src/parse.rs:160-179` — _needless complexity_: granularity is re-derived by re-scanning for `.` via `infer_sub_second_granularity` though the format list already distinguishes fractional vs not; attach `Granularity` per format entry and delete the helper.

### Storage / query / language

## uni-store

- **[high]** `crates/uni-store/src/storage/manager.rs:122-206` — _duplication_: `merge_insert_batch_with_lance_conflict_retry` and `write_batch_with_lance_conflict_retry` are near-identical 10-attempt retry loops (same `is_conflict` string test, same backoff, same `unreachable!`). Extract `retry_on_lance_conflict<F>(op)` + `is_lance_conflict(err)`.
- **[high]** `crates/uni-store/src/storage/manager.rs:122-134` — _misplaced docs_: the doc paragraph describing `write_batch_with_lance_conflict_retry` is attached to `merge_insert_batch_…`; `write_batch_…` (175) has no doc. Move it.
- **[med]** `crates/uni-store/src/storage/main_vertex.rs:428-573` — _duplication_: `find_all_vids`/`find_vids_by_label_name`/`find_vids_by_labels` share an identical filter+scan+collect body. Factor `scan_vids(backend, filter_body, version)`.
- **[med]** `crates/uni-store/src/storage/main_vertex.rs:310-744` — _duplication_: the six `find_*` methods each repeat the `table_exists` short-circuit and the ` AND _version <= {hwm}` snapshot suffix (8 copies). Add `with_version_bound(filter, version)`.
- **[med]** `crates/uni-store/src/runtime/flush_coordinator.rs:435-439` — _dead code_: `_unused_wal_marker()` + the `WriteAheadLog` import-suppression workaround; delete both.
- **[med]** `crates/uni-store/src/fork/recovery.rs:154-155` — _dead code_: `_btreemap_unused_workaround()` + its `BTreeMap` import; delete both.
- **[low]** `crates/uni-store/src/storage/main_vertex.rs:374-790` — _duplication_: the `ListArray→StringArray→labels` extraction block is duplicated in `find_labels_by_vid` and `find_batch_labels_by_vids`; extract `list_array_to_labels`.
- **[low]** `crates/uni-store/src/runtime/flush_coordinator.rs:387-393` — _clarity_: destructure-and-discard of already-matched `seq`; use `.., ..` field destructure.
- **[low]** `crates/uni-store/src/fork/{mod,registry}.rs` — _noise_: stray `// Rust guideline compliant` markers.
- _Intentional (leave):_ `writer.rs:4176` `schedule_index_rebuilds_if_needed` (`#[allow(dead_code)]` documented instance entry point); feature-gated `allow(dead_code)` in vertex/adjacency/delta.

## uni-query

- **[high]** `crates/uni-query/src/query/executor/procedure.rs:207` — _duplication+divergence_: `value_to_columnar` is a copy of `procedure_call::value_to_columnar` (doc even points at it) that has **diverged** — this copy errors on Vector/Node/Edge while the canonical one encodes them. Delete it; call the `pub(crate)` original.
- **[high]** `crates/uni-query/src/procedures_plugin/{vector.rs:136,fts.rs:91,search.rs:92}` — _duplication_: three identical `ProcedurePlugin::invoke` bodies differing only in proc name + `run_*` fn (~75 lines). Extract one helper parameterized by `(proc_name, sig, run_fn)`.
- **[high]** `crates/uni-query/src/query/df_graph/locy_program.rs:741,798,814` — _dead code_: `execute_query_inline` + `needs_node_enrichment` + `expr_has_property_access` (~150-line `#[allow(dead_code)]` island). Delete.
- **[med]** `crates/uni-query/src/query/df_graph/mod.rs:281-347` — _duplication_: three `GraphExecutionContext` constructors repeat an 11-field initializer; delegate through one private `with_parts(...)`.
- **[med]** `crates/uni-query/src/query/df_graph/mod.rs:573-665` — _duplication_: `get_neighbors` is the single-vid case of `get_neighbors_batch`; factor `neighbors_for_vid(...)`.
- **[med]** `crates/uni-query/src/query/executor/procedure.rs:243` — _reuse_: `arrow_scalar_to_value` hand-rolls Arrow-element decoding that `uni_store…arrow_convert::arrow_to_value` (already imported in `read.rs`) does; delegate or document why not.
- **[med]** `crates/uni-query/src/query/executor/procedure.rs:331-621` — _possible dead code_: the inline `uni.admin.*`/`uni.schema.*` dispatch match may be shadowed by the plugin-path dispatch (`procedures_plugin/schema.rs`); confirm reachability — if shadowed it's dead; at minimum collapse the six `drop*` arms.
- **[low]** `crates/uni-query/src/query/executor/core.rs:47-799` — _duplication (likely intentional)_: two cross-type comparison systems (`cypher_cross_type_cmp` for min/max vs `compare_values` for ORDER BY) with different type-rank orderings; add a comment explaining why they must differ.
- **[low]** `crates/uni-query/src/query/executor/procedure.rs:289-826` — _duplication_: yield-item filtering implemented 3× (`filter_yield_items` + two inline); route all through the helper.
- **[low]** `crates/uni-query/src/query/df_graph/{mutation_create,mutation_merge}.rs:9` — _API surface_: 9-line re-export shim modules; fold into `df_graph/mod.rs`'s re-export block.
- **[low]** `crates/uni-query/src/query/executor/read.rs:660-825` — _clarity_: 165-line `record_batches_to_rows` interleaves three key passes with a duplicated dotted-key drain; extract `promote_vid_placeholder`/`merge_dotted_columns`.

## uni-query-functions

- **[med]** `crates/uni-query-functions/src/rewrite/mod.rs:188` — _dead code_: `get_stats()` always returns `default()` (its own doc admits it); never called. Remove.
- **[med]** `crates/uni-query-functions/src/rewrite/context.rs:9-89` — _dead/over-abstraction_: the `scope`/`VariableInfo`/`PropertyType` machinery is never read by any rule. Drop until a rule needs it.
- **[med]** `crates/uni-query-functions/src/rewrite/context.rs:161-213` — _dead config_: `RewriteConfig` flags `enable_spatial`/`enable_property`/`fallback_to_scalar` + `all_disabled()` are never branched on. Reduce to consulted flags.
- **[med]** `crates/uni-query-functions/src/rewrite/mod.rs:123-221` — _unused API_: `rewrite_statement`/`rewrite_expr`/`rewrite_expr_with_context`/`has_rewrite_rule`/`registered_functions` have no callers outside tests (only `rewrite_query` is used). Collapse the surface.
- **[low]** `walker.rs` vs `function_rename.rs` — _duplication (documented/intentional)_: two ~full `Expr` traversals; the largest mechanical-divergence risk (new `Expr` variant must be added in both). A shared visitor trait would pay off here.
- **[low]** `similar_to.rs:319-378` — _duplication_: `cosine_similarity_f64` is an f64 copy of `cosine_similarity`; `value_to_f64_vec`/`value_to_f32_vec` differ only in cast. Generic-ify (compute in f64).
- **[low]** `spatial.rs:105-138` — _duplication_: `"Cartesian"`/`"Cartesian-3D"` arms differ only in the optional z term; collapse with z defaulting to 0.
- **[low]** `rewrite/error.rs` — _boilerplate_: hand-written `Display`/`Error` for 8 variants; the crate already uses `thiserror` — derive it (~55 lines).
- **[low]** `function_props.rs:31` — _style_: fully-qualified `std::collections::HashMap` written out; add a `use`.
- _Intentional (leave):_ `rules/btic.rs` `BticContainsPointRule` (implemented, deliberately unregistered — documented type-mismatch).

## uni-cypher

- **[med]** `crates/uni-cypher/src/grammar/locy_walker.rs:704-783` — _duplication_: `build_is_rule_reference`/`build_is_not_rule_reference` are identical except `negated`. Collapse to `build_is_reference(pair, negated)`.
- **[med]** `crates/uni-cypher/src/grammar/locy_walker.rs:828-892` — _duplication_: `build_locy_{or,xor,and}_expression` are three copies of the same left-fold; extract `fold_binary(...)`.
- **[med]** `crates/uni-cypher/src/grammar/locy_walker.rs:1407-1757` — _duplication_: `build_goal_query`/`build_derive_command`/`build_abduce_query`/`build_explain_rule_query` share the `rule_name + where + return` extraction; one shared helper.
- **[med]** `crates/uni-cypher/src/grammar/locy_walker.rs:77-1686` — _duplication+divergence_: `build_assume_body` re-implements `build_locy_statement_block`'s dispatch over a subset, silently routing model/calibrate/validate to the Cypher `_ =>` arm. Share the per-clause dispatch.
- **[low]** `locy_walker.rs:164-210` — _duplication_: 8 identical `reparse_as_cypher_*` helpers; one generic `reparse<T>(...)`.
- **[low]** `walker.rs:15` & `locy_walker.rs:216` — _duplication_: `normalize_identifier`/`normalize_locy_identifier` identical; reuse the `pub(crate)` one.
- **[low]** `walker.rs:2286-2364` — _reinvention_: `collect_property_refs_into` hand-enumerates `Expr` variants instead of using `Expr::for_each_child` (ast.rs:1432); ~70 lines.
- **[low]** `mod.rs:63,260` — _duplication_: `error_position`/`locy_error_position` identical but for the `Rule` type; make generic `<R>`.
- **[low]** `walker.rs:1096` & `locy_walker.rs:625` — _duplication_: `unquote_string_literal` is a reduced re-impl of `unescape_string` (drops `\uXXXX`/trailing-backslash); route locy through the shared one.
- **[low]** `locy_walker.rs:1471-1481` — _dead code_: `seen_target_kw` is set then `let _ =`-discarded; remove the flag.
- **[low]** `walker.rs:280-300` — _dead code_: `build_with_recursive_clause` hardcodes `items = vec![]`, field populated nowhere; confirm with consumer.
- **[low]** `walker.rs:2076-2087` — _dead arm_: empty `Rule::OPTIONS => {}` no-op in `build_create_scalar_index`; the `map_literal` arm does the work.
- _Intentional (leave):_ ModelDefinition/Calibrate/Validate/Conformal/Dirichlet Phase B/C/D preview placeholders.

## uni-locy

- **[high]** `crates/uni-locy/src/semiring.rs:577-653` — _duplication_: `merge_top_k_dispatch_owned`/`merge_top_k_dispatch` re-implement `TopKProofs::merge_top_k` (only swapping const-`K` for runtime `k`). Extract `merge_top_k_with(base, additional, k)`.
- **[high]** `crates/uni-locy/src/semiring.rs:398-571` — _dead/over-abstraction_: the "Stage 2" tag-flow surface (`plus_tag`/`times_tag`/`zero_tag`/`singleton_tag`/`weight_of`/`AggregatorValue`) has no non-test callers (~170 lines). Gate behind a feature or remove until Stage 2 wires it.
- **[high]** `crates/uni-locy/src/compiler/typecheck.rs:760-944` — _duplication_: `check_shared_neural_inputs` has three near-identical detect blocks (F2a/b/c). Factor the "group → if ≥2 non-independent emit warning" tail.
- **[med]** `crates/uni-locy/src/compiler/dependency.rs:23-37` — _dead code_: `build_dependency_graph`/`build_dependency_graph_with_external` are uncalled wrappers over `_with_models`. Delete.
- **[med]** `crates/uni-locy/src/compiler/dependency.rs:106-245` — _boilerplate_: `collect_path_context_deps`/`walk_*` thread 6 invariant args through recursion; bundle into a context struct.
- **[med]** `crates/uni-locy/src/compiler/errors.rs:70-92` — _dead code_: `UnknownModel`/`ModelOutputTypeMismatch` variants never constructed. Remove or wire.
- **[med]** `crates/uni-locy/src/semiring.rs` + `top_k_proofs.rs` — _duplication_: the `validate_domain` clamp/warn block is copy-pasted into 3 `LocySemiring` impls (same warn literal). Lift to `validate_probability_domain(raw, op, strict)`.
- **[med]** `crates/uni-locy/src/neural.rs:354-473` — _duplication_: `NeuralProvenanceStore` and `ModelInvocationCache` are the same `RwLock<HashMap<(String,u64),V>>` with identical accessors; share a `KeyedStore<V>`.
- **[low]** `crates/uni-locy/src/types.rs:155-206` — _design debt_: `ModelInvocation` holds parallel `feature_exprs`/`original_feature_exprs`/`feature_names`/`feature_property_refs` vectors kept in lockstep; a `Vec<FeatureArg{...}>` makes the invariant structural.
- **[low]** `crates/uni-locy/src/compiler/typecheck.rs:378-400` — _duplication_: `find_prev_ref` = `collect_prev_refs(...).next()`.
- **[low]** `crates/uni-locy/src/compiler/typecheck.rs:1063-1233` — _duplication_: the property-feature registration block written twice; extract `register_property_feature(v, prop)`.
- **[low]** `crates/uni-locy/src/result.rs:33-46` — _API clarity_: `timed_out` is documented as exactly `incomplete.is_some()`; replace the denormalized field with an accessor (grep consumers first).
- _Intentional (leave):_ `SemiringKind::{TopKProofs,BddExact}` Stage-1 fallback, `TopKProofs::negate` degenerate collapse.

### Plugin framework

## uni-plugin

- **[med]** `crates/uni-plugin/src/surfaces/mod.rs:129-554,1413` — _dead/over-abstraction_: `RecordedKey` enum + `KeyedUniqueSurface::record_key` (8 impls) are never called by non-test code (footprint is tracked via `record_register`). Delete.
- **[med]** `crates/uni-plugin/src/registry.rs:661-753` — _duplication_: `register_virtual_label` (u16) and `register_virtual_edge_type` (u32) are identical but for the ID type + sentinel constants (and a `==` vs `>=` inconsistency). Generic helper over the two inner shapes.
- **[med]** `crates/uni-plugin/src/registry.rs:288-935` — _duplication_: `PluginRecordSnapshot` mirrors the private `PluginRecord` (28 fields) and `iter_for_plugin` hand-clones each. Derive `Clone`/`From<&PluginRecord>` so the field list exists once.
- **[low]** `crates/uni-plugin/src/surfaces/mod.rs:64,1384` — _stale doc_: comments say "26 plugin surfaces" but the enum/test enumerate 25. Fix.
- **[low]** `crates/uni-plugin/src/lifecycle.rs:238-247` — _control flow_: `finalize`'s `loop { match … }` is clearer as `while self.old.state() != Removed { advance() }`.
- **[low]** `crates/uni-plugin/src/secrets.rs:81-100` — _control flow_: the `id == 0` double-`fetch_add` guard would read clearer with `next: AtomicU64::new(1)`.
- **[low]** `crates/uni-plugin/src/surfaces/mod.rs:680-696` — _indirection (intentional-leaning)_: `signature_discriminator`/`discriminator_to_usize` are thin given one versioned surface today.
- _Intentional (leave):_ M11 scheduler/observability skeletons; `DynPendingRegistration::{kind,debug_label}` diagnostics; `TrustRoot::allow`; the three-layer capability enforcement, `wildcard_match` opacity, `normalize_capability_path`, `intersect`/`contains_variant` — **do not loosen**.

## uni-plugin-rhai

- **[med]** `crates/uni-plugin-rhai/src/host_fn_impls/{kms.rs:47,secret.rs:36,net.rs:73}` — _duplication_: the allow-list-check / service-present / dispatch prelude repeats; extract `require_allowed(...)` + `require_service(...)` in `host_fn_impls/mod.rs`.
- **[med]** `crates/uni-plugin-rhai/src/host_fn_impls/{kms,secret,net,fs}.rs` `register()` — _duplication_: the spec-building pattern repeats with inconsistent inline-vs-helper styling; add a `RhaiHostFnSpec` constructor.
- **[med]** `crates/uni-plugin-rhai/src/manifest.rs:137-204` — _duplication_: `parse_{scalar,aggregate,procedure}_entries` are structurally identical; extract `parse_entry_array<T>(map, key, build)`.
- **[med]** `crates/uni-plugin-rhai/src/columns.rs:20-124` — _duplication_: `Float64Column`/`Int64Column`/`Utf8Column` are the same wrapper 3×; generic `ArrayColumn<A>` or a macro.
- **[low]** `crates/uni-plugin-rhai/src/error.rs:32-75` — _dead code_: `CapabilityDenied`/`ResourceLimit`/`NotYetImplemented` + `not_yet` never constructed; trim (NotYetImplemented likely M7 placeholder).
- **[low]** `crates/uni-plugin-rhai/src/adapter_procedure.rs:140-162` — _control flow_: `coerce_for`'s Float64/Int64 arms return `value` either way after the conversion check; simplify.
- **[low]** `crates/uni-plugin-rhai/src/adapter.rs:224-236` — _needless complexity_: `classify_rhai_error` builds a `(code, retryable)` tuple where `retryable` is always false; drop it.
- **[low]** `crates/uni-plugin-rhai/src/loader.rs:268` — _dead param_: `build_procedure_signature(_determinism)` unused; drop it.
- **[low]** `crates/uni-plugin-rhai/src/loader.rs:173-196` — _needless work_: `load()` builds+compiles the engine/AST twice (probe then runtime) from identical inputs; reuse the probe artifacts.
- **[low]** `crates/uni-plugin-rhai/src/adapter_procedure.rs:164-170` — _dead code_: `const _: Option<…> = None` rustdoc-link dummies; use `#[allow(unused_imports)]` or intradoc links instead.
- _Intentional (leave):_ all `fs.rs`/`*_allows` security checks.

## uni-plugin-extism

- **[med]** `crates/uni-plugin-extism/src/loader.rs:278-405` — _duplication_: the host-fn capability filter is written twice (`prepare_parsed` vs `load` pass-1 bootstrap). Extract `allowed_host_fn_names(&self, caps)`.
- **[med]** `crates/uni-plugin-extism/src/loader.rs:341-347` — _redundant API_: `instantiate` is a one-line delegate to `build_plugin` with identical contract; collapse.
- **[med]** `crates/uni-plugin-extism/src/{adapter,adapter_aggregate,adapter_procedure}.rs` — _duplication_: `acquire`, `extism_err_to_fn_err`, and the qname sanitizer are copy-pasted across the 3 adapters; hoist to a shared module (see theme 2).
- **[low]** `crates/uni-plugin-extism/src/adapter_aggregate.rs:180-356` — _duplication_: `call_with_envelope` builds the length-prefixed envelope inline duplicating the `build_envelope` test helper; share one writer.
- **[low]** `crates/uni-plugin-extism/src/error.rs:40-80` — _dead code_: `NotYetImplemented`+`not_yet` (retired per doc) and `MemoryExchange` (test-only) unused.
- **[low]** `crates/uni-plugin-extism/src/checked_call.rs` — _unused module_: `checked_call` is `pub` but has zero non-test callers; the real host_svc bodies enforce attenuation directly. Either wire host_svc through it or remove (do **not** remove the checks themselves — they live in `host_svc`).
- **[low]** `crates/uni-plugin-extism/src/loader.rs:239-247` — _duplication_: `prepare` re-implements the JSON parse+error-map that `exports::parse_manifest_json` already does; reuse.
- **[low]** `crates/uni-plugin-extism/src/adapter_aggregate.rs:219,246` — _needless clone_: `&self.update_export.clone()` only to take `&str`; pass `&self.update_export`.
- **[low]** `crates/uni-plugin-extism/src/host_svc/{kms,secret,net}.rs` — _boilerplate_: the `host_fn!` shells repeat `ctx.get()? → lock → from_str → do_* → to_string`; generalize the new `http_dispatch_json` into `dispatch_json<Req,Resp>` covering all three (see theme 4).
- **[low]** `crates/uni-plugin-extism/src/wire_translate.rs:256-262` — _duplication_: local `argtype_to_arrow` reimplements the imported `uni_plugin::adapter_common::arrow_types::argtype_to_arrow`; reuse or comment the difference.
- _Intentional (leave):_ two-pass `load`, `host_yield`/streaming deferral, `batch_input: None`.

## uni-plugin-wasm

- **[med]** `crates/uni-plugin-wasm/src/loader.rs:1050-1204` — _duplication_: `wire_*_sig_to_internal` are ports of `uni-plugin-extism/wire_translate.rs` (see theme 3). Also `wire_agg_sig_to_internal` re-parses `volatility` instead of reading `internal.volatility`.
- **[med]** `crates/uni-plugin-wasm/src/loader.rs:1168-1176` — _naming_: nested `wire_arg`/`arrow_name` helpers (the latter a 1-line passthrough); use one crate-level `wire_arg_to_argtype`.
- **[med]** `crates/uni-plugin-wasm/src/loader.rs:489-603` — _duplication_: the bootstrap `PreparedComponent` (empty manifest + offered-grants comment) is built verbatim in `load` and `load_as_plugin`; extract `bootstrap_prepared(host_grants)`.
- **[med]** `crates/uni-plugin-wasm/src/loader.rs:675-702` vs `multi_version.rs:85-119` — _duplication_: `select_linker_for_manifest` re-implements `MultiVersionLinker::linker_for`'s major dispatch and never uses the cache. Route through it or share `major_for_abi`.
- **[med]** `crates/uni-plugin-wasm/src/loader.rs:215-366` — _error boilerplate_: 6 copies of the `Ok(Ok)/Ok(Err)/Err` export-call match; extract `map_call<T>(label, r)`.
- **[low]** `crates/uni-plugin-wasm/src/error.rs:8-69` — _dead code_: `MissingCapability`, `NoRecognizedWorld`, `NotYetImplemented`+`not_yet`, `epoch_timeout_marker()` unused; `lib.rs:24` "load returns NotYetImplemented" doc stale.
- **[low]** `crates/uni-plugin-wasm/src/loader.rs:209-231` — _misleading comment_: `invoke_scalar` doc mentions per-call fuel reset but never touches fuel; invoke errors map to `WasmError::Instantiate` (add an `Invoke` variant).
- **[low]** `crates/uni-plugin-wasm/src/loader.rs:716-728` — _dead binding_: `apply_resource_limits` `let _ = ms;` then a no-op epoch deadline; mark TODO(phase-d), drop dead binding.
- **[low]** `crates/uni-plugin-wasm/src/adapter.rs:135` — _dead code_: `wasm_err_to_fn_err` `#[allow(dead_code)]` unused; remove.
- **[low]** `crates/uni-plugin-wasm/src/{adapter,adapter_aggregate,adapter_procedure}.rs` — _duplication_: `ipc_to_fn_err` + `acquire` copied across the 3 adapters (theme 2).
- **[low]** `crates/uni-plugin-wasm/src/linker.rs:159-162` — _stale doc_: `add_host_log` doc references a bindgen helper it doesn't use; match the hand-rolled `func_wrap` reality.
- _Intentional (leave):_ `build_scalar_linker_v2`/`host-log-v2` placeholder (documented).

## uni-plugin-wasm-rt

- **[med]** `crates/uni-plugin-wasm-rt/src/ipc.rs:143` — _inconsistency_: `decode_batch` skips `reject_secret_handles` while `decode_batches` runs it — a single-batch stream bypasses the secret-leak check. Confirm intent; likely should be symmetric.
- **[low]** `crates/uni-plugin-wasm-rt/src/ipc.rs:84,164` — _duplication_: the secret-rejection loop appears 3× in slightly different shapes; add `reject_all(batches)`.
- **[low]** `crates/uni-plugin-wasm-rt/src/pool.rs:221` — _API surface_: `idle_len` is `#[doc(hidden)] pub` but test-only; `#[cfg(test)]`-gate it.
- **[low]** `crates/uni-plugin-wasm-rt/src/pool.rs:188-194` — _boilerplate_: slot-rollback `match` → `.map_err(|err| { live.fetch_sub(1); err })?`.
- **[low]** `crates/uni-plugin-wasm-rt/src/pool.rs:140` — _clarity_: warm-up `let _ = idle.push(...)` hides a can't-fail invariant; add `debug_assert!`/comment.
- **[low]** `crates/uni-plugin-wasm-rt/src/pool.rs:130` — _redundant cast_: explicit `as Box<dyn Fn…>` likely unneeded (coercion).
- **[low]** `crates/uni-plugin-wasm-rt/src/ipc.rs:181` — _over-abstraction_: `estimate_size` is premature optimization for `with_capacity`; inline or drop.

## uni-plugin-host

- **[high]** `crates/uni-plugin-host/src/{cdc_runtime.rs:99,scheduler_persistence.rs:94,triggers.rs:1816}` — _duplication_: three thin wrappers re-implement the same `SystemSidecar<Vec<T>>` load/store + `.map_err(|e| e.to_string())` + `path()`. Add a shared `VecSidecar<T>` or `load_rows`/`store_rows` helpers.
- **[med]** same lines — _needless allocation_: `self.sidecar.store(&rows.to_vec())` clones the whole slice on every persistence write though the caller often already owns a `Vec`.
- **[med]** `crates/uni-plugin-host/src/scheduler_persistence.rs:100-204` — _duplication_: `upsert`/`record_scheduled`/`cancel` repeat the lock→read_all→mutate→write_all skeleton + the cypher-mirror block. Extract `mutate_rows(id, f)` + `mirror_cypher(qname, cypher)`.
- **[med]** `crates/uni-plugin-host/src/triggers.rs:599-1753` — _duplication_: the `catch_unwind` → Ok(Ok(Defer))/Ok(Ok)/Ok(Err)/Err ladder appears 3× (`dispatch_after`, `fire_caught`, `DeferralQueue::tick`). Extract `handle_fire_outcome(...)`.
- **[low]** `crates/uni-plugin-host/src/hooks.rs:214-244` — _needless complexity_: `after_commit` duplicates an 8-field `CommitResult` literal in both arms; derive `Default` + `map(...).unwrap_or_default()`.
- **[low]** `crates/uni-plugin-host/src/triggers.rs:1334-1372` — _over-abstraction_: `EventRowColumns::extend` exists for one caller alongside `push_row`; collapse to `push_row`.
- **[low]** `crates/uni-plugin-host/src/triggers.rs:1441-1454` — _clarity_: `mask_to_discriminant` hand-rolls trailing-zero count; `m.0.trailing_zeros() as u8 + 1`.
- **[low]** `crates/uni-plugin-host/src/cdc_runtime.rs:171-303` — _duplication_: `spawn` and `discover_new_providers` repeat the provider-start ladder; extract `start_stream(name, provider)`.
- **[low]** `crates/uni-plugin-host/src/scheduler.rs:35` — _style_: fold `OnceLock` into the line-30 `std::sync` import.
- **[low]** `crates/uni-plugin-host/src/scheduler_persistence.rs:206-229` — _clarity/smell_: `load_all` drops rows whose qname lacks a `.` and discards the persisted `status` (hardcodes `Pending`); document or parse through `QName`.
- _Intentional (leave):_ in-memory queue / empty-CdcBatch v1 placeholders.

## uni-plugin-custom

- **[high]** `crates/uni-plugin-custom/src/persistence.rs:184-192` — _dead code_: `PersistenceEnvelope` is `pub`, never constructed/used (its doc claims round-trip tests that don't exist). Delete or wire.
- **[high]** `crates/uni-plugin-custom/src/{lib.rs:1003,aggregate.rs:461}` — _duplication_: `type_str_to_arrow`/`declared_plugin_id`/`local_part`/`map_plugin_error` are byte-identical in both files; hoist to a shared module.
- **[high]** `crates/uni-plugin-custom/src/{aggregate.rs:288,scalar.rs:142,eval.rs:316}` — _duplication_: `array_value_at` (comment admits the dup), `stringify`/`stringify_value` (×3), `eval_err_to_fn` (×2). Promote to one shared decoder module.
- **[med]** `crates/uni-plugin-custom/src/lib.rs:1064-1099` — _needless complexity_: `SyntheticScalarPlugin` splits two impl blocks so `manifest()` can `Box::leak` per call (and the comment contradicts itself). Store a `OnceLock<PluginManifest>` field built once.
- **[med]** `crates/uni-plugin-custom/src/lib.rs:954-1001` — _unreachable arm_: `match signature.mode` has both `Read => {}` and `_ => {}`; drop one.
- **[med]** `crates/uni-plugin-custom/src/lib.rs:476-665` — _boilerplate_: five `*_signature()` fns repeat the common-field + `registered` yield; extract `write_signature(args, docs)`.
- **[low]** `crates/uni-plugin-custom/src/lib.rs:1290-1335` — _clarity_: `declare_kind_procedure!` builds opaque positional `arg1`/`arg2` keys then re-extracts `arg1`→`body`; name keys from the signature args.
- **[low]** `crates/uni-plugin-custom/src/lib.rs:1413-1476` — _duplication_: `extract_string_or`/`extract_string` share a matcher (and `extract_string_or`'s `_name` is unused).
- _Intentional (leave):_ `merge_batch` partial-agg rejection, `NullPersistence`, record-only declare path, M9/M11 cutover comments.

## uni-plugin-builtin

- **[high]** `crates/uni-plugin-builtin/src/logical_types.rs` — _duplication_: the 5 Utf8-backed types (Uri/GeoPoint/Email/Ipv4/Ipv6) repeat identical `to_display`/`cast_to`/`cast_from` (~15 copies). Factor `utf8_to_display`/`utf8_cast_to` helpers; keep GeoPoint's `"POINT EMPTY"` override.
- **[med]** `crates/uni-plugin-builtin/src/lib.rs:111,116` — _bug-smell_: `Capability::Index` listed twice in `declared_capabilities()` (deduped by the set, but misleading). Remove the dup.
- **[med]** `crates/uni-plugin-builtin/src/procedures/system.rs:23-69` — _duplication_: echo's `ProcedureSignature` built twice (inline + cached `OnceLock`) with slightly different doc strings. Register the cached one (theme 5).
- **[med]** `crates/uni-plugin-builtin/src/procedures/periodic.rs:282` — _inconsistency_: `register_into` uses `build_signature()` (fresh alloc) while runtime returns `signature_cached()`; register the cached one.
- **[med]** `crates/uni-plugin-builtin/src/procedures/{system.rs:93,periodic.rs:431}` — _duplication_: `extract_first_string`/`extract_utf8` are the same Utf8 extraction; share one helper.
- **[low]** `crates/uni-plugin-builtin/src/algorithms/bridge.rs:261` — _dead code_: `_value_to_uni_value_dead` stub; delete.
- **[low]** `crates/uni-plugin-builtin/src/crdts.rs:79` — _stale comment_: `LwwState::merge` "M5b scaffold: identity merge" but it does a real LWW merge and the crate doc says "no placeholders"; fix the comment.
- **[low]** `crates/uni-plugin-builtin/src/crdts.rs` — _duplication_: `RgaProvider` ≈ `TypedRgaProvider<String>`; consider `impl RgaElement for String` to drop ~80 lines (verify concat-vs-JSON `value()`).
- **[low]** `crates/uni-plugin-builtin/src/{storage.rs:434,storage_table_provider.rs:492}` — _duplication (maybe intentional)_: `LanceFilterPushdown`/`StorageFilterPushdown` identical `SupportsFilterPushdown` impls; collapse unless divergence is imminent.
- **[low]** `crates/uni-plugin-builtin/src/optimizer/pushdown_negotiation.rs:418-456` — _misattributed doc_: doc block for `downcast_markers` sits above `peel_transparent_projection`; re-attach.
- _Intentional (leave):_ `EchoProcedure`/`Identity`/`LoggingHook`/`geo.point` scaffolding; `scalar_fns` Identity duplication (apply theme-5 fix if system.rs lands).

## uni-plugin-apoc-core

- **[high]** `crates/uni-plugin-apoc-core/src/procedures/{bitwise,math,text,number,convert,create}.rs` — _over-abstraction_: `signature_cached()` is reimplemented in all six files as one `OnceLock` static **per variant** + a giant match. Cache once per enum (`OnceLock<HashMap<Self,Sig>>` or a `Vec` indexed by `ALL`), replacing ~25 lines/file with ~5.
- **[high]** same files — _duplication across modules_: six near-identical `register_into` loops, multiple `extract_string/i64/f64` copies (differing only in error prefix), repeated single-row result builders, and the identical `RecordBatch::try_new + RecordBatchStreamAdapter` tail. Hoist an internal `support` module (`register_all`, `extract_*` with a prefix arg, `one_row_stream`).
- **[med]** `crates/uni-plugin-apoc-core/src/procedures/math.rs:214-285` (also `bitwise.rs:183`) — _control flow_: `invoke` nests a second `match self` inside the `_ =>` arm with two `unreachable!()`; compute an `Output{Int|Float}` enum in one flat match.
- **[med]** `crates/uni-plugin-apoc-core/src/procedures/{text.rs,convert.rs,math.rs}` — _inconsistency_: `text.length` counts chars but `text.indexOf` returns a byte index; three `extract_i64` variants have divergent float-coercion policies (silent truncation in math). Consolidate to one documented policy.
- **[low]** `crates/uni-plugin-apoc-core/src/procedures/create.rs:185-239` — _intentional placeholder_: hand-rolled non-crypto xorshift UUID v4 (16-arg `format!`); revisit if a uuid/rand crate is available.
- **[low]** `crates/uni-plugin-apoc-core/src/procedures/mod.rs:7-16` & `lib.rs:9-13` — _stale docs_: inventory omits number/convert/create and advertises absent `apoc.{refactor,schema,atomic}`.
- **[low]** `text.rs:355` & `create.rs:158` — _magic number_: `1_000_000` OOM cap duplicated; hoist `const MAX_SYNTHESIZED_LEN`.
- **[low]** all modules — _error codes_: bare `0x700..0x705` hex literals scattered; name them or fold into `one_row_stream`.

## uni-plugin-conformance

- **[low]** `crates/uni-plugin-conformance/src/lib.rs:267-309` — _duplication_: `registration.commit`/`registration.idempotent` repeat the registry+registrar+register+commit sequence (3×); a `register_once(label)` closure collapses them.
- **[low]** `crates/uni-plugin-conformance/src/lib.rs:258-265` — _stub-as-real_: `capabilities.declared` does `let _ = &manifest.capabilities; Ok(())` — always passes, tests nothing. Exercise the real accessor or note it as a deliberate always-pass.
- **[low]** `crates/uni-plugin-conformance/src/lib.rs:215-224` — _misleading name_: `manifest.parse` doc claims semver validation but only checks `id` non-empty; `version` untouched.
- **[low]** `crates/uni-plugin-conformance/src/lib.rs:215-265` — _clarity_: group the 4 manifest-only probes vs the 2 registration probes into two phases.
- **[low]** `crates/uni-plugin-conformance/src/lib.rs:95` — _boilerplate_: `push_str(&format!())` → `writeln!(msg, …)`.
- _Intentional (leave):_ the `WasmPath` marker arm (dep-graph design, guarded by `wasm_target_returns_runner_pointer`).

## uni-plugin-pyo3

- **[high]** `crates/uni-plugin-pyo3/src/loader.rs:159-282` — _duplication_: `load` and `load_from_builder` share ~50 lines (validate/resolve-id/derive-caps/intersect + the three `if effective.contains{..} register_* else Vec::new()` blocks + `LoadOutcome`). Extract `finalize(manifest, default_id, registrar, caps)`.
- **[high]** `crates/uni-plugin-pyo3/src/{adapter_scalar.rs:311,adapter_aggregate.rs:379,adapter_procedure.rs:291}` — _duplication_: `classify_pyerr` defined identically 3× (and ≈ `From<PyErr> for PyPluginError`); lift one into `adapter_scalar_helpers.rs`.
- **[med]** `crates/uni-plugin-pyo3/src/loader.rs:455-488` & `adapter_aggregate.rs:339-367` — _duplication_: `type_name_to_datatype`/`type_name_to_argtype` and the determinism→volatility match duplicated; one shared table.
- **[med]** `crates/uni-plugin-pyo3/src/loader.rs:141-157` — _dead code_: `module_name_c`/`filename_c` built then `let _ = (...)`-discarded (real names applied in `build_module_with_sink`); delete.
- **[med]** `crates/uni-plugin-pyo3/src/{adapter_scalar.rs:238,adapter_procedure.rs:216}` — _duplication_: `ScalarBuilder` and `ColumnBuilder` are the same 4-variant builder; `ColumnBuilder::push_py` does a redundant scalar round-trip. Share one builder.
- **[low]** `crates/uni-plugin-pyo3/src/error.rs:50-69` — _dead code_: `NotYetImplemented`+`not_yet` unused (theme 1).
- **[low]** `crates/uni-plugin-pyo3/src/loader.rs:591-594` — _over-abstraction_: `PyDecoratorSink::new` one-liner over `from_builder`; the pymethods re-implement the `make_*_trampoline` free fns — delegate.
- **[low]** `crates/uni-plugin-pyo3/src/loader.rs:953-991` — _boilerplate_: the dict-branch repeats `get_item?.ok_or_else?.unbind()` 4×; a closure/loop over the 4 keys.
- **[low]** `crates/uni-plugin-pyo3/src/adapter_scalar_helpers.rs:21-72` — _boilerplate_: `scalar_to_py`/`py_to_scalar` 4 arms differ only by type; macro/generic.

### DB facade / bindings / CLI / tooling

## uni-db (crates/uni)

- **[high]** `crates/uni/src/api/mod.rs:568-765` — _duplication_: `UniInner::at_snapshot`/`at_fork` repeat a ~30-field literal differing in 5 fields. Extract `derived_clone(&self, storage, schema, properties, writer, rule_registry)`.
- **[high]** `crates/uni/src/api/mod.rs:1919-2133` (+ `session.rs:428-503`) — _duplication_: the 4 plugin loaders + python add/finalize repeat build-placeholder-registrar → load → map_err → commit (6×). Extract `with_loading_registrar(registry, placeholder, caps, f)`.
- **[med]** `crates/uni/src/api/mod.rs:1115-1501` — _duplication_: the `storage_uri`+`dataset.lance` join open-coded 4× across fork ops; `dataset_uri(uri, dataset)` (and reuse `recovery::join_uri_with`).
- **[med]** `crates/uni/src/api/mod.rs:2411-2629` — _duplication_: `get_label_info`/`get_edge_type_info` share property/index/constraint projection loops (~80 lines); extract `property_infos_for`/`index_infos_for`/`constraint_infos_for`.
- **[med]** `crates/uni/src/api/mod.rs:1095-1208` — _duplication_: the bounded "wait for holder_count to drain" loop (100/20/10ms) appears in `drop_fork` and `drop_fork_cascade`; extract `wait_for_holders_drained(fork_id)`.
- **[med]** `crates/uni/src/api/session.rs:207-359` (+ Clone at 1539) — _duplication_: three `Session` constructors + Clone repeat a ~20-field literal differing in ~6 fields; introduce a base.
- **[med]** `crates/uni/src/api/session.rs:1226-1419` — _duplication_: the read-only-rejection `UniError::Query{...}` message duplicated in `execute_cached` and `QueryBuilder::fetch_all`; `fn read_only_violation(cypher)`.
- **[low]** `crates/uni/src/api/session.rs:1595-1204` — _dead field_: `PlanCacheEntry.ast` is cloned on every cache hit but the bound `_ast` is discarded; drop the field (confirm no reader).
- **[low]** `crates/uni/src/api/mod.rs:3117-3135` — _needless complexity_: the WAL-construction `if/else if/else` has two arms producing identical `WriteAheadLog::new(...)` and an unreachable `else`; collapse.
- **[low]** `crates/uni/src/api/mod.rs:2329-2358` — _duplication/over-async_: `label_exists`/`edge_type_exists` identical but for `.labels`/`.edge_types`, both `async` with no `.await`; share `element_active(map, name)` (and review the needless `async`).
- **[low]** `crates/uni/src/api/mod.rs:118-239` — _dead construction_: `register_builtin_plugins` builds a "uni" host manifest then `let _ = manifest;`-discards it; wire it or delete until needed.
- **[low]** `crates/uni/src/api/mod.rs:935-969` — _control flow_: `session_with_credentials` materializes a `Vec` of providers then loops; iterate the filter directly.
- _Intentional (leave):_ the per-feature `#[cfg]` `register_provider` ladder in `build()`.

## uni-python (bindings/uni-db)

- **[high]** `bindings/uni-db/src/async_api.rs` (~15 sites) — _duplication_: the `Option<HashMap<String,Py>> → Option<HashMap<String,Value>>` param conversion is hand-rolled 15+×; add `convert_params`/`convert_params_ref` in `convert`.
- **[high]** `bindings/uni-db/src/async_api.rs` (~22 sites) — _boilerplate_: the "transaction already completed" guard is repeated ~22×; add `active_tx(guard)` (sync side already has `Transaction::check_active`).
- **[high]** `bindings/uni-db/src/async_api.rs:3208-3350` — _over-abstraction_: `AsyncQueryCursor::{fetch_one,fetch_many,__anext__}` each rebuild a throwaway `self`-clone to call `next_row_async(&self)`; make it a free async fn over the two `Arc`s.
- **[high]** `bindings/uni-db/src/{sync_api.rs:1244,async_api.rs:513}` — _duplication+dead arm_: `write_lease()` byte-identical in both, with a `WriteLease::Local` arm plus a `_ =>` arm yielding the same `Local`. Share `write_lease_to_py`; drop the dead arm.
- **[high]** `bindings/uni-db/src/{sync_api.rs:804,async_api.rs:326}` — _duplication_: `LabelInfo`/`EdgeTypeInfo` construction copied 4× (~80 lines each); add `From<rust::LabelInfo>`/`From<rust::EdgeTypeInfo>`.
- **[med]** `bindings/uni-db/src/async_api.rs:97-1039` — _duplication_: `AsyncDatabaseBuilder` constructed with all 13 fields 5×; add `Default` + `..Default::default()`.
- **[med]** `bindings/uni-db/src/{sync_api.rs:537,async_api.rs:1824}` — _duplication_: `PyProgressWrapper` + `unsafe impl Send` + the progress closure defined twice; extract `make_progress_callback(cb)`.
- **[med]** `bindings/uni-db/src/{async_api.rs:2052,sync_api.rs:600}` — _duplication_: the optional-setter `if let Some(x) { builder = builder.x(x) }` chains for appender/bulk-writer replicated sync+async.
- **[med]** `bindings/uni-db/src/sync_api.rs:622-628` — _misattributed doc_: a default-grant doc block is stacked on the wrong fn (`wasm_outcome_to_pydict`); remove it.
- **[med]** `bindings/uni-db/src/async_api.rs:3464-3811` — _efficiency_: `fetch_one` runs full `fetch_all` then `.next()`, materializing all rows; use a `limit(1)`/cursor path or document why.
- **[low]** `bindings/uni-db/src/{sync_api.rs:1030,async_api.rs:438}` — _noise_: empty section-divider comment banners.
- **[low]** `bindings/uni-db/src/async_api.rs:1589` — _async boilerplate_: `is_completed` is an awaitable that only locks+reads a bool (sync analog is a plain `bool`); defensible (needs the lock) but the one spot where the async wrapper buys little.
- _Intentional (leave):_ `AsyncSession` decorator methods, `try_lock` "session busy" forks, `#[allow(deprecated)]` hooks.

## uni-algo

- **[high]** `crates/uni-algo/src/algo/algorithms/{closeness,apsp,betweenness,harmonic_centrality}.rs` — _duplication_: the unweighted single-source BFS-over-CSR is hand-reimplemented in ≥3 algorithms with subtly different bookkeeping; extract `bfs_levels(graph, source)`.
- **[med]** `crates/uni-algo/src/algo/cypher/*.rs` (~12 files) — _dead boilerplate_: 12 adapters override `include_reverse() -> true`, identical to the (deprecated) trait default; delete all.
- **[med]** `crates/uni-algo/src/algo/cypher/*.rs` (~11 files) — _duplication_: ~11 `customize_projection` bodies differ only in the `args[N]` weight index; add `fn weight_arg_index() -> Option<usize>` and implement `customize_projection` once.
- **[med]** `crates/uni-algo/src/algo/projection.rs:58-59` — _dead code_: `_node_labels`/`_edge_types` populated in all constructors, never read; drop the fields + plumbing.
- **[med]** `crates/uni-algo/src/algo/algorithms/harmonic_centrality.rs:52` vs `dijkstra.rs:44` — _duplication_: harmonic reimplements weighted-Dijkstra relaxation; factor `dijkstra_distances(graph, source)`.
- **[med]** `crates/uni-algo/src/algo/cypher/{pagerank,wcc,louvain,label_propagation,all_simple_paths,shortest_path}.rs` — _error-handling_: still use `args[i].as_f64().unwrap()`/`as_str().unwrap()` on optional args (panic risk); use `.ok_or_else(|| anyhow!(...))?` per the hardened template contract.
- **[low]** `crates/uni-algo/src/algo/cypher/shortest_path.rs:125` — _duplication_: bespoke `vid_from_value` overlaps `parse_vid_arg`; fold legacy form in or reuse.
- **[low]** `crates/uni-algo/src/algo/cypher/{shortest_path,astar,all_simple_paths}.rs` — _boilerplate_: the `match parse_vid_arg {..} return err_stream` guard repeated 9×; add `err_stream(e)` + parse terminals up front.
- **[low]** `crates/uni-algo/src/algo/id_map.rs:119` — _dead comment_: `compact()` carries a commented-out sort-verification; implement the `debug_assert!` or delete.
- **[low]** `crates/uni-algo/src/algo/traversal.rs:111-127` — _control flow_: `shortest_path_with_hops` has an unreachable third branch + a long prose comment; collapse to if/else.
- **[low]** `crates/uni-algo/src/algo/projection.rs:464` — _false type safety_: `NodeRow`/`EdgeRow` are two aliases for the same `HashMap`; use one `ProjectionRow`.
- **[low]** `crates/uni-algo/src/algo/algorithms/apsp.rs:40-75` — _needless complexity_: `Mutex<&mut Vec>` splice; use rayon `flat_map_iter().collect()` (as `betweenness.rs` does).
- **[low]** `crates/uni-algo/src/algo/procedures.rs:230` — _stale marker_: obsolete "Phase 3.3 placeholder" comment (37 procedures now registered).

## uni-crdt

- **[med]** `crates/uni-crdt/src/vector_clock.rs:42-66` — _needless complexity_: `happened_before` does `other.get(actor)` twice per iteration; bind once.
- **[med]** `crates/uni-crdt/src/vector_clock.rs:74-84` — _naming footgun_: an inherent `partial_cmp` with `PartialOrd`'s signature but no `PartialOrd` impl; implement the trait or rename to `causal_cmp`.
- **[med]** `crates/uni-crdt/src/vc_register.rs:48-69` (+ `vector_clock.rs:87`) — _dead indirection_: double merge aliasing (`merge_clock`/`merge`, `merge_register`/`merge`) and `MergeResult` consumed only in one test; fold or document as public diagnostic.
- **[low]** `crates/uni-crdt/src/orset.rs:64-82` — _duplication_: the `tags.iter().any(|t| !tombstones.contains(t))` visibility predicate repeated; extract `is_visible(tags)`.
- **[low]** `crates/uni-crdt/src/lww_map.rs:36,45,79` — _duplication_: `LWWRegister::new(None,-1)` sentinel literal 3×; name it.
- **[low]** `crates/uni-crdt/src/orset.rs:46-99` — _control flow_: manual insert loops → `tombstones.extend(...)` / `tags.extend(...)`.
- **[low]** `crates/uni-crdt/src/gcounter.rs:50` — _clarity_: `if other > count { count = other }` → `*count = (*count).max(*other)`.
- **[low]** `crates/uni-crdt/src/registry_dispatch.rs:83` — _duplication_: `merge_via_registry`'s discriminant guard partly duplicates `try_merge`; hoist `ensure_same_kind`.
- **[low]** `crates/uni-crdt/src/lib.rs:121-133` — _boilerplate_: the merge-panic message re-appends type names already in `{e}`.
- _Intentional (leave):_ the `for_each_crdt_variant!` macro table; `registry_dispatch` staged bridge.

## uni-fork

- **[med]** `crates/uni-fork/src/diff.rs:184-217` — _dead code_: `diff_edge_type`'s `changed` branch is documented unreachable; the `let _ = (keys_a.intersection(...), out)` discard + two full `HashSet` allocations are noise — iterate maps directly.
- **[med]** `crates/uni-fork/src/diff.rs:148-182` — _duplication_: `diff_label`/`diff_edge_type` share the added/deleted skeleton; `partition_added_deleted(a, b, mk_added, mk_deleted)`.
- **[low]** `crates/uni-fork/src/diff.rs:87-113` — _needless complexity_: `scan_label_nodes`'s `row_label` find/unwrap_or_else always resolves to `label`; use `label.to_string()`.
- **[low]** `crates/uni-fork/src/diff.rs:153-199` — _needless clone_: `b[uid].clone()` then moves fields; `remove(uid)` to move out.
- **[low]** `crates/uni-fork/src/diff.rs:258-393` — _error handling / reuse_: `batch_resolve_primary_vids` swallows errors into empty maps (document or propagate); `.map_err(Internal)` repeated 5×; `uid_to_hex` hand-rolls hex (check `UniId` for a canonical form).
- **[low]** `crates/uni-fork/src/diff.rs:264` — _dead alias_: `use UniId as UniIdT` aliases an already-imported type.
- **[low]** `crates/uni-fork/src/types.rs:373` — _placeholder field_: `PromoteReport::vertices_skipped_no_uid` always 0 (reserved; public). Intentional per doc.
- **[low]** `crates/uni-fork/src/types.rs:302-309` — _needless complexity_: `where_clause`'s two arms do the same; use an or-pattern.
- **[low]** `crates/uni-fork/src/maintenance.rs:47-105` — _duplication_: `spawn_sweeper`/`spawn_index_builder` identical spawn/ticker/select loops; `spawn_ticker(host, interval, …, tick_fn)`.

## uni-bulk

- **[high]** `crates/uni-bulk/src/appender.rs:80-126` — _dead code_: `AppenderBuilder`'s only constructor (`new_from_tx`) hard-codes the pre-acquired path, so the session-path `else` branch, the `is_pinned` check, `write_guard`/`session_id`/`guard_pre_acquired` fields, and `StreamingAppender::session_write_guard` are all unreachable. Strip to the unguarded path.
- **[high]** `crates/uni-bulk/src/appender.rs:237-246` — _dead code_: the `Drop` impl only fires when `session_write_guard` is `Some` (always `None`); delete it.
- **[high]** `crates/uni-bulk/src/bulk.rs:154-173` — _dead code_: `BulkWriterBuilder::new_deferred` has no callers; with it gone the `is_pinned` check + deferred-guard `compare_exchange` block are dead.
- **[high]** `crates/uni-bulk/src/bulk.rs:126-152` — _dead code/API_: `new_with_guard` then has no callers either; `session_write_guard`/`release_guard()`/the guard branch of `Drop` collapse to no-ops. (The whole `Arc<AtomicBool>` write-guard machinery is vestigial — confirm no out-of-tree consumer of these `pub` ctors before deleting.)
- **[med]** `crates/uni-bulk/src/{bulk.rs:86,appender.rs:169}` — _duplication_: the Arrow-row→property-map conversion is duplicated in `IntoArrow::into_property_maps` and `write_batch`; share `record_batch_to_property_maps`.
- **[med]** `crates/uni-bulk/src/bulk.rs:715-758` — _naming/complexity_: `compare_json_values` (no JSON involved) hand-rolls -1/0/1 ordering; rename `compare_values` and use `partial_cmp(...).map(|o| o as i8)`.
- **[low]** `crates/uni-bulk/src/bulk.rs:431` — _API surface_: `get_current_timestamp_micros(&self)` uses no fields; make it associated/free.
- **[low]** `crates/uni-bulk/src/bulk.rs:789` — _consistency_: `check_flush_vertices` `if let/else { false }` → `is_some_and(...)` (as `check_flush_edges` does).
- **[low]** `crates/uni-bulk/src/bulk.rs:1227` — _style_: fully-qualified `chrono::Utc::now()` though `Utc` is imported.
- **[low]** `crates/uni-bulk/src/bulk.rs:357` — _API surface_: `pub type BulkStatsAccumulator = BulkStats` spec-compat alias; drop if unused.

## uni-cli

- **[med]** `crates/uni-cli/src/main.rs:138-160` — _duplication_: the DB-open incantation (with an inconsistent two-step `let builder = …; builder.build()`) repeats across 4-5 arms; extract `async fn open_db(path) -> Result<Uni>`.
- **[med]** `crates/uni-cli/src/repl.rs:92-129` — _duplication_: `execute_query`'s three branches repeat the session-binding block + an identical red-error match arm; inline + `print_query_error(e)`.
- **[low]** `crates/uni-cli/src/main.rs:138` — _needless complexity_: `path.to_string_lossy().to_string()` → `to_string_lossy()` (Cow is `Into<String>`).
- **[low]** `crates/uni-cli/src/main.rs:289-327` — _control flow_: `parse_grants` arms produce a `bool` immediately discarded, with a bogus `false` in `other =>`; make arms statements.
- **[low]** `crates/uni-cli/src/main.rs:212-281` — _duplication (intentional msgs)_: the "M12 not yet supported" scheme-rejections appear 5×; a `(prefix, message)` table.
- **[low]** `crates/uni-cli/src/demo/semantic_scholar.rs:37-41` — _dead code_: `_paper_lbl` insert-or-fetch dance is unused; `let _ = add_label("Paper");`.
- **[low]** `crates/uni-cli/src/demo/semantic_scholar.rs:78-128` — _duplication_: papers/citations load loops share file-open/line-loop/progress/flush scaffolding; `for_each_jsonl(path, label, f)`.
- **[low]** `crates/uni-cli/src/demo/semantic_scholar.rs:99-115` — _needless complexity_: 4 `if let Some(x) = json.get("k") { props.insert(...) }` → loop over the key set.
- _Intentional (leave):_ the M12 scheme/feature rejections.

## uni-tck

- **[high]** `crates/uni-tck/src/steps/when_step.rs:20-81` — _duplication_: `executing_query`/`executing_control_query`/`executing_query_with_params` have byte-identical bodies; delegate to one `run_query_step(world, step)`.
- **[high]** `crates/uni-tck/src/matcher/result.rs:7-201` — _duplication_: the ordered/unordered × ignoring-list-order matchers are 4 near-identical fns differing only by comparator; parameterize on a `fn(&Value,&Value)->bool`.
- **[med]** `crates/uni-tck/src/matcher/result.rs:254-422` — _duplication_: `values_equal`/`maps_equal`/`nodes_equal`/`edges_equal`/`paths_equal` each duplicated by `*_ignoring_list_order` twins differing only in the leaf comparator; share via the comparator param.
- **[med]** `crates/uni-tck/src/parser/value.rs:204-242` — _duplication_: `edge`/`edge_in_path` share the body via `parse_edge_brackets`; only the `edge_type.is_none()` guard differs — extract `build_edge(...)`.
- **[med]** `crates/uni-tck/tests/tck.rs:109-139` — _dead code_: `name_index` map is built/incremented but `idx` is never read (the name uses `@L{line}`); drop the map.
- **[low]** `crates/uni-tck/src/steps/and.rs:43-150` — _boilerplate_: the seven `assert_eq!` side-effect arms repeat the scaffolding; `assert_effect(label, actual, expected, ctx)`.
- **[low]** `crates/uni-tck/src/parser/value.rs:58-85` — _boilerplate_: verbose `nom::Err::Error(nom::error::Error::new(...))` → `.map_err(|_| nom_err(input, kind))`.
- **[low]** `crates/uni-tck/tests/tck.rs:153-169` — _clarity_: `ignored_scenario_reason` has 2 unused params + an inverted if; `is_hanging_literals7.then_some(...)`.
- **[low]** `crates/uni-tck/src/world.rs:322-355` — _duplication_: `collect_node_ids`/`collect_edge_ids` identical but for the query; `collect_ids(query)`.
- **[low]** `crates/uni-tck/src/world.rs:120-136` — _duplication_: manual `Default` alongside `new()`; derive or drop one.

## uni-locy-tck

- **[high]** `crates/uni-locy-tck/src/matcher/result.rs:1-433` — _dead code_: the entire `matcher` module is never referenced by any step/harness (Locy TCK asserts on `result.derived`/`command_results`, not `QueryResult` tables). ~430 lines + a duplicate equality impl. Delete unless staged scaffolding.
- **[high]** `crates/uni-locy-tck/src/parser/{table.rs,value.rs}` — _dead code_: `parse_table`/`parse_value` reachable only via the re-export + own tests; the live steps roll their own `parse_gherkin_value`. Remove the `parser` module (or wire the two duplicate literal parsers through it).
- **[high]** `crates/uni-locy-tck/src/world.rs:99-407` — _dead code/over-abstraction_: the `SideEffects` subsystem (`capture_state_*`, snapshots, `set_result`/`set_error`/`result()`/`error()`, `last_result`/`last_error`) has no callers (~250 lines). Delete unless planned scaffolding.
- **[med]** `crates/uni-locy-tck/src/steps/then_evaluate.rs:1058-1249` — _duplication_: 7 compile-warning presence/absence steps are copy-paste differing only by `WarningCode`; add `assert_compile_warning_present/_absent(world, code, label)`.
- **[med]** `crates/uni-locy-tck/src/steps/then_evaluate.rs:526-775` — _duplication_: command-result assertions repeat the `match expect_command_result {Variant(rows)=>…, other=>panic!}` shape (~10 handlers); a variant-extractor helper.
- **[med]** `crates/uni-locy-tck/src/steps/then_evaluate.rs:920-1056` — _duplication_: calibration/validation lookups repeat the iterate-`command_results`-find-by-name-else-panic pattern; `find_calibration`/`find_validation`.
- **[low]** `crates/uni-locy-tck/src/steps/then_evaluate.rs:1613-1749` — _duplication_: the 3 `explain_child_neural_call*` handlers share a fetch/match/find preamble; extract a finder (and switch to `expect_command_result`).
- **[low]** `crates/uni-locy-tck/src/steps/{then_evaluate.rs:99,then_parse.rs:4,then_compile.rs:4}` — _duplication_: the succeed/fail/error-mentions trio implemented 3×; generic `assert_ok`/`assert_err_mentions<E: Display>`.
- **[low]** `crates/uni-locy-tck/src/steps/given.rs:101-119` — _duplication_: `set_parameter`'s literal parsing is a 3rd copy of `parse_gherkin_value`; share it.
- **[low]** `crates/uni-locy-tck/src/steps/given.rs:9-23` — _duplication_: `an_empty_graph`/`any_graph` byte-identical; one handler for both phrases.
- **[low]** `crates/uni-locy-tck/src/steps/then_compile.rs:78-110` — _duplication_: `stratum_should_be_recursive`/`_not_` differ only by bool; one regex step with a capture.
- **[low]** `crates/uni-locy-tck/tests/locy_tck.rs:153-161` — _stub_: `ignored_scenario_reason` always returns `None` (all params `_`); confirm it's an intentional extension point.

---

## Note on intentional placeholders

Several findings touch code the reviewers flagged as **intentional** — documented
scaffolding, forward-compat slots, or deliberate design markers. These were
explicitly called out per-crate above ("_Intentional (leave)_") and should **not**
be "simplified away" without revisiting the rationale. In particular: the
capability/attenuation/security machinery in `uni-plugin` and the loaders, the
`uni-plugin-conformance` `WasmPath` marker arm, the `for_each_crdt_variant!` table,
and the various Phase-B/C/D / Stage-2 preview surfaces.
