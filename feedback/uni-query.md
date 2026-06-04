# Code Simplifier Feedback: `crates/uni-query/`

Scope: recently modified files in the `plugin-fw` worktree
(`lib.rs`, `query/df_graph/mod.rs`, `query/df_graph/procedure_call.rs`,
`query/df_planner.rs`, `query/df_udfs.rs`, `query/executor/procedure.rs`,
`query/executor/read.rs`), with light context from adjacent files
(`query/executor/procedure_host.rs`, host crate call sites).

The recent (uncommitted) deltas are small and focused — they thread an
outer-tx `writer` handle and a `CURRENT_PRINCIPAL` task-local down to
the plugin-procedure invocation sites. The deltas themselves are
correct and reasonably scoped; the simplification opportunities they
surface are mostly *cross-cutting duplication* introduced by adding
the same boilerplate at two sibling sites.

---

## 1. Duplicated "build ProcedureContext + writer + principal + invoke" block (HIGH VALUE)

**Files / lines:**
- `crates/uni-query/src/query/executor/procedure.rs:659-686`
- `crates/uni-query/src/query/df_graph/procedure_call.rs:655-680`

**Description.** Both procedure entry-points now contain a nearly
identical 25-line block:

1. construct a `QueryProcedureHost` (one via `from_components`, the
   other via `from_graph_ctx_with_request`),
2. attach the optional `writer`,
3. read `current_principal()`,
4. build a `ProcedureContext::new().with_host(&host)`, conditionally
   chaining `.with_principal(p)`,
5. call `entry.procedure.invoke(ctx, &columnar_args)`.

The duplicated FU-1 / M11 #6 comment blocks are also pasted in two
places, with slight wording divergences ("outer executor's writer"
vs "outer transaction's writer") that will drift further over time.

**Suggestion.** Introduce one small helper in
`procedure_host.rs` (or a new `procedure_invoke.rs`) — e.g.

```
fn build_procedure_context<'a>(
    host: &'a QueryProcedureHost,
    principal: Option<&'a Principal>,
) -> ProcedureContext<'a>
```

…and call it from both sites. The `writer` attachment can either move
into a `QueryProcedureHost::with_optional_writer(Option<...>)`
convenience or stay at each call site (it differs slightly in source
of the writer — `self.writer` vs `graph_ctx.writer()`), but the
principal-reading + `with_principal` chaining is pure boilerplate
that belongs in one place.

**Effort.** ~30 min. Mechanical extract; no behavior change.

---

## 2. `scoped_with_session_context` adds a third API for what is effectively one call

**File / lines:** `crates/uni-query/src/query/df_udfs.rs:299-339`,
re-exports in `crates/uni-query/src/lib.rs:40-43`.

**Description.** Three new public symbols were added
(`CURRENT_PRINCIPAL`, `scoped_with_principal`, `current_principal`)
plus a thin convenience wrapper `scoped_with_session_context` that
just dispatches to those primitives. The wrapper is only useful at
host-crate boundaries — every internal call in `uni-query` uses
`current_principal()` directly, and call-site usage in
`crates/uni/src/api/session.rs` already standardizes on the wrapper.

The `match principal` inside `scoped_with_session_context` is
slightly heavier than it needs to be: it forces an `async fn` (with
its boxed-future overhead at call sites) purely to express
"optionally wrap in a second task-local scope." This shape also
prevents the compiler from collapsing the two scopes into one frame
when `Some` is hot.

**Suggestion.**
- Consider collapsing `scoped_with_principal` + `current_principal`
  + the `CURRENT_PRINCIPAL` static into a small `principal_scope`
  submodule of `df_udfs.rs` (or, better, alongside
  `SESSION_PLUGIN_REGISTRY` in a dedicated `task_locals.rs` — the
  pattern is identical, and grouping them advertises the contract).
- Replace the `async fn scoped_with_session_context` with a synchronous
  function that returns an `impl Future` — same body, but it removes
  the `.await` on the inner scope and the implicit boxing at call
  sites. The two branches can share a `let inner = ...` style.
- Drop the unused public re-export of `CURRENT_PRINCIPAL` from
  `lib.rs:40` unless external consumers genuinely need raw access to
  the `LocalKey`; the `scoped_with_*` / `current_*` pair is enough.

**Effort.** ~20 min for re-export trim; ~45 min for the
`async fn` -> `impl Future` refactor (needs a sanity build).

---

## 3. `HybridPhysicalPlanner` builder explosion + `take_graph_ctx` rebuild

**File / lines:** `crates/uni-query/src/query/df_planner.rs:317-405`
and the matching builders on `GraphExecutionContext`
(`crates/uni-query/src/query/df_graph/mod.rs:354-405`).

**Description.** Each new field on `GraphExecutionContext` requires
*three* mechanical edits:

1. add `Option<...>` field + `with_*` builder on
   `GraphExecutionContext`,
2. add a mirrored `with_*` builder on `HybridPhysicalPlanner` that
   calls `take_graph_ctx()` then re-attaches via
   `ctx.with_*(value)`,
3. extend `take_graph_ctx()` to clone-and-reattach the new field
   (`df_planner.rs:317-351`).

`take_graph_ctx` is now ~35 lines of "snapshot every Option, replace
the Arc with a placeholder, try_unwrap, reattach in order." Every
addition risks forgetting step (3) — the new `writer` field is
correctly added but the pattern is fragile.

The `Self` builders are themselves a 4-line copy/paste:
```
let ctx = self.take_graph_ctx().with_X(value);
self.graph_ctx = Arc::new(ctx);
self
```
(see lines 358-365, 369-378, 384-391, 394-398).

**Suggestion.**
- Extract a single private helper `fn mutate_graph_ctx(&mut self, f: impl FnOnce(GraphExecutionContext) -> GraphExecutionContext)`
  on `HybridPhysicalPlanner`. Then each `with_*` becomes a one-liner:
  ```
  pub fn with_writer(mut self, w: Arc<RwLock<Writer>>) -> Self {
      self.mutate_graph_ctx(|c| c.with_writer(w));
      self
  }
  ```
- Longer-term: replace the per-field
  `Option<Arc<X>>` + builder pattern on `GraphExecutionContext` with
  a single `GraphRegistries { algo, procedure, xervo, plugin, writer }`
  struct, then `take_graph_ctx()` only needs to snapshot/restore one
  value. Doc-comments on `with_writer` (`df_graph/mod.rs:348-353`,
  `df_planner.rs:353-356`) are also near-duplicates that will fall
  out naturally from this consolidation.

**Effort.** Helper extraction: ~30 min. Registries struct: ~2 h,
touches more files and warrants its own commit.

---

## 4. `with_writer` field added but never read at the `GraphExecutionContext` level except by the planner re-emit

**File / lines:**
- `crates/uni-query/src/query/df_graph/mod.rs:184-191` (field), `351-365`
- consumer: `crates/uni-query/src/query/df_graph/procedure_call.rs:666`
- propagated by: `crates/uni-query/src/query/df_planner.rs:322,347-349`

**Description.** The `writer` field on `GraphExecutionContext` exists
only so that the deeply-nested
`execute_plugin_procedure(graph_ctx, ...)` can fish it back out.
That's a legitimate use, but the field is being threaded through
`take_graph_ctx` and four other constructors that have no use for it
— it is always `None` except along one specific code path. This
makes the abstraction noisier than necessary.

**Suggestion.** Consider passing the writer explicitly as a
parameter to `execute_plugin_procedure` (which already has a
custom signature with `#[allow(clippy::too_many_arguments)]`-class
ergonomics — adding one more `Option<&Arc<RwLock<Writer>>>` parameter
is a wash and keeps `GraphExecutionContext` lean). If staying with the
field approach is preferred for symmetry with `procedure_registry`,
that's fine — but then it should ride on top of the registries-struct
refactor in §3 so the bookkeeping cost is paid only once.

**Effort.** ~45 min if passing as parameter; folded into §3 otherwise.

---

## 5. Doc-comment duplication around the FU-1 / M11 #6 plumbing

**Files / lines:**
- `crates/uni-query/src/query/executor/procedure.rs:664-677`
- `crates/uni-query/src/query/df_graph/procedure_call.rs:662-676`
- `crates/uni-query/src/query/executor/read.rs:441-446`
- `crates/uni-query/src/query/df_planner.rs:353-356`
- `crates/uni-query/src/query/df_graph/mod.rs:184-191, 347-352`

**Description.** Six locations carry slight variants of the same
M11 #6 / FU-1 commentary explaining "thread the outer writer so
WRITE-mode declared procedures can mutate via the inner-query host."
The risk of drift is real; one site already calls it "outer executor's
writer" while five call it "outer transaction's writer."

**Suggestion.** Keep *one* canonical paragraph as a module-level
rustdoc on a new `mod writer_threading;` (or as a `///` block on the
`GraphExecutionContext::writer` field), and reduce the per-site
comments to one-liners referencing it (e.g. `// see GraphExecutionContext::writer (FU-1 / M11 #6)`).

**Effort.** ~15 min.

---

## 6. Minor consistency / style nits

- `crates/uni-query/src/query/df_graph/procedure_call.rs:667` uses
  `std::sync::Arc::clone(writer)` while line 672 of
  `executor/procedure.rs` uses `Arc::clone(writer)` (relying on the
  module-level `use`). Pick one; the `Arc::clone(...)` form already
  dominates the crate.
- `crates/uni-query/src/query/df_udfs.rs:316-319` —
  `pub fn current_principal() -> Option<Arc<Principal>>` is formatted
  with the return type on its own line; the matching
  `current_session_plugin_registry` on line 274 fits on one line.
  Trivial fmt symmetry.
- `crates/uni-query/src/lib.rs:39-43` — the re-export group is now
  six names across three lines. Consider sorting alphabetically
  (`current_principal`, `current_session_plugin_registry`,
  `scoped_with_principal`, `scoped_with_session_context`,
  `scoped_with_session_plugin_registry`, plus the two statics) to
  match the rest of the file's style.
- `crates/uni-query/src/query/df_planner.rs:347-349` — the
  `if let Some(w) = writer { ctx = ctx.with_writer(w); }` pattern is
  the fifth such block in `take_graph_ctx`; a `macro_rules!` or the
  registries struct from §3 would erase the pattern.

**Effort.** All combined: ~20 min.

---

## Non-issues observed (deliberately *not* recommending changes)

- The dual-registry / session-plugin-registry contract
  (`df_udfs.rs:241-275`) is already well-documented and consistent
  with the new `CURRENT_PRINCIPAL` symmetry; the parallel structure
  is good.
- The `procedure_host::QueryProcedureHost::with_writer` mutation gate
  with the explicit error at `procedure_host.rs:328` is a healthy
  invariant — keep it loud.
- The `7000+ LOC` files (`df_planner.rs`, `df_udfs.rs`) have known
  long-running consolidation work (UDF macroification, planner
  rewrites) that is well out of scope for the FU-1 / M11 #6 surface.

---

## Suggested action ordering

1. (§1) Extract `build_procedure_context` helper — biggest payoff,
   smallest blast radius.
2. (§5) Consolidate the FU-1 comment to a single canonical doc.
3. (§3 helper extraction only) Add `mutate_graph_ctx` to
   `HybridPhysicalPlanner` so future fields don't multiply boilerplate.
4. (§2) Trim `scoped_with_session_context` async overhead and the
   `CURRENT_PRINCIPAL` public re-export.
5. (§6) Style nits.
6. (§3 registries struct, §4) Defer until another field needs to
   join `writer` on `GraphExecutionContext`.
