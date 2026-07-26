# GraphCompute × uniscape stepped-dynamics — requirements analysis and design response

> **Status:** design proposal + Phase 0 landed (repro harness only; no behaviour changed).
> Part I is the analysis and design; Part II (§12–§14) is the validation and implementation plan;
> **Part III (§15–§18) is the measured Phase 0 result, and it corrects Part I — see §16.**
> **Baseline:** `254b4c26c` (12-gap closure), uni-db 3.0.2.
> **Input:** `uniscape/packages/uniscape-dynamics/docs/UNIDB_REQUIREMENTS.md` (2026-07-25),
> seven ranked asks REQ-D1…D7 from the SD / ABM / hybrid simulation track.
> **Method:** every claim below is either verified against live source (file:line given) or
> executed as a test. Hypotheses are labelled **UNVERIFIED** and carry their falsifying test.

---

## 1. Headline

**The item ranked HIGH and "blocks a milestone" is not blocked.** Elementwise comparison and
conditional selection are expressible with kernels that shipped in 3.0.0. Proven by
`reqd1_compare_and_select_are_composable_from_shipped_kernels`
(`crates/uni-plugin-builtin/src/algorithms/graph_compute/differential_tests.rs`), which passes:

```
compare(a, b, "gt")  ==  set_to_map(map_to_set(ewise(a, b, "axpy", -1.0), "gt", 0.0), 1.0)
step(a, θ)           ==  set_to_map(map_to_set(a, "gt", θ), 1.0)
select(m, a, b)      ==  ewise(b, ewise(m, diff, "mul"), "add")        // b + m·(a−b)
```

`map_to_set` (`session.rs:1049`) carries the closed predicate enum
`Predicate { IsZero, Gt(f64), Lt(f64), Eq(f64) }` (`session.rs:151-160`); `set_to_map`
(`session.rs:1037`) lifts the resulting bitset back to a `[V]` f64 map. Both are `AllLoaders`
(`kernel_id.rs:105-106`) and registered in Rhai as `gc.map_to_set(m, op, threshold)` /
`gc.set_to_map(set, value)` (`uni-plugin-rhai/src/graph_compute.rs:226`, `:218`).

REQ-D4's fallback ask — a `gc.edge_mask(g, property)` selecting edges where a property is
non-zero — is likewise already shipped as `edge_mask_window(sel, 0.5, 1.5)`
(`session.rs:1980`), which is an *exact threshold*, not a sampler. Proven by
`reqd4_edge_mask_window_is_an_exact_selector_without_sampling`.

That makes **five of seven asks answerable without an engine change** (D1, D3-questions, D4,
D6, D7), one a small localized fix (D2), and one a genuine additive feature (D5).

**But the recurrence is the real finding.** This is the second consecutive field report whose
top-ranked asks were *discoverability* failures rather than capability failures — the
2026-07-22 verification found the same for G4 (set args) and G8 (fork-free snapshots), and
concluded then that "the two MISCHARACTERIZED gaps are discoverability failures, not capability
failures." Two rounds is a pattern, not an accident. §3 treats it as the primary design problem.

---

## 2. Verified findings, per ask

| ask | reported as | verified status |
|---|---|---|
| **D1** comparison→mask | HIGH, blocks M5 | **Not blocked.** Composable today (test-proven). A real but *narrow* gap remains: edge-shaped compare — `map_to_set` ignores `Shape` and always allocates a `VertexSet` (`session.rs:1072`), which `spmv_masked` rejects with `KIND_MISMATCH`. |
| **D2** multi-emit | MEDIUM | **Real, and mis-diagnosed.** Not "first emit lands, later ones vanish" — the *first* emit fails. Validation (`session.rs:1637-1659`) demands every declared column be present *in that one call*, and runs before any capture (`:1662`). The host trait `emit(cols: &[(&str, Handle)])` (`session.rs:660`) is all-columns-one-call by design; every guest shim exposes one-column-per-call. The combination is unsatisfiable for ≥2 non-`nodeId` yields. |
| **D3** rare wrong value | MEDIUM, open question | **Both questions answerable.** Process-global mutable state in the GC+Rhai path: exactly one `AtomicU16` session-epoch counter (`mod.rs:240`), inert on the Rhai path. Handle recycling: structurally ruled out — generation bumped *before* free-list push (`table.rs:143`), six independent resolution gates (`table.rs:82-96`, `:241-246`). Free-before-emit fails loud with `0x860`, it cannot return a recycled value. Candidate causes in §6. |
| **D4** sampler endpoints | LOW, contract request | **Already a guaranteed contract in code and test.** `rng.rs:123-128` early-returns `false` for `prob <= 0.0` and `true` for `prob >= 1.0` *before* hashing; pinned by `degenerate_probabilities_are_exact` (`rng.rs:177`). Undocumented, not unguaranteed. |
| **D5** second projection | LOW, ergonomic | **Real, and cheaper than expected.** The session layer already supports N projections: `bind_graph` is `pub` and re-callable (`session.rs:293`), `HandleTable` holds a generational arena of graphs (`table.rs:207`), and `arena_freeze` already mints a second graph handle mid-session (`session.rs:2815`). Only the four loaders bind exactly one. |
| **D6** work budget | LOW, observability | **Formula confirmed exactly**, with a caveat they did not observe: `min(10_000·(V+E+1), 1_000_000_000)` (`mod.rs:323-327`, constants `:198`, `:204`) — the absolute ceiling clamps above ~100k V+E. A `Capability::GraphComputeWork` grant is *authoritative and replaces* the size default (`mod.rs:371`). Documented at `UNI_BLACK_BOOK.md:5862`; absent from `skills/` and from every guest-facing surface. |
| **D7** `edge_property` NaN | LOW, doc clarification | **Real, with a specific structural cause.** Five distinct NaN preconditions exist (§7). The one that explains their conflicting evidence is a silent degrade: when the procedure host is built with `L0Context::empty()` (`procedure_host.rs:167`, `read.rs:398-401`), `l0_manager` is `None`, so `projection.rs:285` pins no L0 snapshot and property reads see flushed storage only. |

---

## 3. The three structural themes

Treating seven asks as seven tickets would close them and guarantee a third field report. They
fall into three classes, each with a systematic fix.

### Theme A — A closed catalog needs a published composition proof

The design deliberately keeps the kernel set coarse and the parameter enums closed
(`graphcompute_plugin_api_2026-07-10.md` §4.3, decision D4), and the accepted gate for adding a
kernel is *"is this composition-blocked?"* — the precedent set by G2, where `Sqrt`/`Exp` were
added precisely because `√` was not composable while `div = recip·mul` was.

That gate is sound. What is missing is the other half: **nothing tells a guest author what *is*
composable.** The consequence is measurable — across two field reports, four top-ranked asks
(G4, G8, D1, D4) were already expressible. Each cost the consumer a milestone-scoping decision
and cost us an investigation.

The fix is not another doc section. Docs are read before writing code; this failure happens
*during* writing, at the moment the engine rejects an op. **Put the recipe in the error
message** (WS-1a). `bad ewise op 'gt'` is a dead end; `bad ewise op 'gt' — comparison is
composable: set_to_map(map_to_set(ewise(a,b,"axpy",-1.0),"gt",0.0),1.0)` is a fix. This is the
cheapest and highest-leverage change in this document.

### Theme B — Guest shims drifted from the host trait, and the parity tests can't see it

D2's root cause is exactly this shape: the trait is multi-column-single-call, the shims are
single-column-per-call, and the validator enforces the trait's contract against the shim's
capability. No test caught it because **every loader fixture declares exactly one value column**
and the parity tests check *reachability* (does `hasattr(GcSession, name)` hold —
`uni-plugin-pyo3/src/graph_compute.rs:947`, `uni-plugin-rhai/src/graph_compute.rs:1092`) rather
than *expressiveness* (can the shim drive the trait through its domain).

The same class is open elsewhere: the typed WASM `host-arena` WIT surface
(`uni-plugin-wasm/wit/world.wit:105`, `linker.rs:337`) is hand-maintained with **no parity test
at all** — omitting an entry silently drops WASM guests to the ~32× slower JSON path.

### Theme C — "Fail loud" was applied one layer too shallow

3.0.0 made the projection contract "scoped-or-loud, L0-consistent"
(`UNI_BLACK_BOOK.md:5870`), closing G12 by threading the L0 overlay into property reads. But
the *upstream* construction can still hand the projection no L0 manager at all
(`L0Context::empty()`), and `projection.rs:285` degrades to a storage-only read **silently** —
producing exactly the plausible-but-wrong values the contract was written to eliminate. This
plausibly explains both D7 and D3.

---

## 4. WS-1 — Comparison, selection, and the discoverability fix

### WS-1a (P0, hours) — Recipes in the rejection path

Every closed-enum rejection gains a composition hint. Concretely, at the three `bad ewise op`
sites (`dispatch.rs:461`, `uni-plugin-rhai/src/graph_compute.rs:287`,
`uni-plugin-pyo3/src/graph_compute.rs:295`) and the three `bad map op` sites (`dispatch.rs:222`
and peers), replace the bare rejection with a table lookup: unknown-op → known recipe, else the
list of valid ops.

Recipes to seed the table:

| requested | recipe |
|---|---|
| `gt`/`ge`/`lt`/`le`/`eq`/`ne`, `cmp`, `compare` | `set_to_map(map_to_set(ewise(a, b, "axpy", -1.0), "gt", 0.0), 1.0)` |
| `step`, `heaviside`, `threshold` | `set_to_map(map_to_set(a, "gt", θ), 1.0)` |
| `select`, `where` | `ewise(b, ewise(m, ewise(a, b, "axpy", -1.0), "mul"), "add")` |
| `sub` | `ewise(a, b, "axpy", -1.0)` |
| `relu` | `ewise(a, set_to_map(map_to_set(a, "gt", 0.0), 1.0), "mul")` |
| `abs` | `ewise(a, map_apply(a, "scale", -1.0), "max")` |
| `clip`/`clamp` | `ewise(ewise(a, lo, "max"), hi, "min")` |
| `pow` (integer k) | repeated `mul` |
| `neg` | `map_apply(a, "scale", -1.0)` |

The recipe table is the *same artifact* as the doc, generated from one source into the error
strings, `website/docs/plugins/graph-algorithms.md`, and `skills/uni-db/references/`. That
removes drift by construction — the failure mode where the doc says composable and the engine
says no.

**The three sites triplicate the op-string parse today** (agent finding). Fold them into one
`EwiseOp::from_op_name` / `MapOp::from_op_name` in `session.rs` so the recipe table has exactly
one home. This also fixes a latent parity hazard: three hand-maintained match arms can diverge.

### WS-1b (P0, hours) — Publish the executable cookbook

The two tests added in §9 are the cookbook's proof. Extend to a `composition_recipes.rs`
suite where each published identity is a passing differential test against a scalar oracle. A
recipe that stops being true then breaks CI, rather than silently misleading a guest author.

Document the two sharp edges the composition carries:

- **NaN.** `map_to_set` predicates are false on NaN, so a NaN input masks to 0. But
  `select` via `b + m·(a−b)` **poisons on NaN in the *unselected* branch** — `0.0 * NaN = NaN`.
  Where either branch may be NaN, blend with `scatter` over the `VertexSet` instead of
  multiplying by the mask.
- **Budget.** The composition charges `3n` where a native compare would charge `n`
  (`axpy` `session.rs:1033`, `map_to_set` `:1071`, `set_to_map` `:1045`). At their scale this
  costs ~3 report-steps of headroom per chunk — real but not structural.

### WS-1c (P1) — Add `compare`, and make `map_to_set` shape-polymorphic

Composability answers "is it blocked", not "is it good". Three arguments justify the kernel
under the established G2 gate:

1. **The `[E]` case is genuinely blocked.** `map_to_set` allocates a `VertexSet`
   unconditionally (`session.rs:1072`), ignoring `Shape`. Feed it an `[E]` tensor and you get a
   `HandleKind::Set` that `spmv_masked` (`session.rs:2151`) and `expand_masked` (`:2040`)
   reject with `KIND_MISMATCH`. There is no composition around this.
2. **3× budget** on the hottest inner-loop op of an entire workload class.
3. **Most-requested op across two independent field reports.**

Design, respecting the closed-enum invariant (§4.3: "the parameter enums are part of the ABI"):

```rust
pub enum CmpOp { Gt, Ge, Lt, Le, Eq, Ne }
fn compare(&mut self, a: Handle, b: Handle, op: CmpOp) -> Result<Handle, FnError>;
```

- Host-evaluated, closed enum → no per-element guest logic, honouring §5.6.
- **Shape-preserving**: `[E]` in → `[E]` out, fixing the asymmetry above.
- Charge `n` **before** the loop, matching `spmv`'s admission-control discipline rather than
  `ewise`'s charge-after-compute (`session.rs:1030`) — a pre-existing inconsistency worth not
  propagating.
- Validate `Shape` equality, not merely length. `ewise` today compares `len()` only
  (`session.rs:1004`), so a `[V]` and an `[E]` tensor of coincidentally equal length silently
  compute garbage. That is a live silent-wrong-answer bug independent of this ask, and it
  becomes materially more likely once WS-4 lands multiple projections.

And separately, **make `map_to_set` shape-polymorphic**: `[E]` in → `EdgeSet` out. This
delivers REQ-D4's alternative ask (`edge_mask`) with no new kernel, and converts a
currently-useless result (a wrong-kind handle that fails downstream) into the right one.

Optionally `select(m, a, b)` as a kernel — *not* requested, but it removes the NaN trap above,
which is the G2-shaped argument (the composition is not semantically equivalent under NaN).

---

## 5. WS-2 — Multi-emit (P0, small)

The trait was designed multi-column; only the shim and the placement of one validation loop
break it.

1. **Split the validation.** Keep per-call checks in `emit` (no duplicate within the call, no
   undeclared name, no duplicate against already-captured names, `[V]`-length consistency vs
   `primary_graph.vertex_count()` — `session.rs:1686`). Move the "every declared column present"
   loop (`session.rs:1652-1658`) to session close, so it fires once, after the guest returns.
2. **Accumulate.** `self.emitted = captured` (`session.rs:1695`) becomes an upsert/extend.
3. **Batch shim**, keeping the single-name form: Rhai `gc.emit(#{"a": ha, "b": hb})` as an
   arity-1 overload (precedent: `zero_map_typed`, `graph_compute.rs:1019`); Python
   `gc.emit({...})`; a `names[]`/`handles[]` variant on the JSON wire (`dispatch.rs:107`).
4. **Fixture change (the Theme-B fix).** Every loader conformance fixture
   (`crates/uni/tests/common/loaders/*_graph_compute.rs`,
   `bindings/uni-db/tests/test_graph_compute_plugin.py`) gains a ≥2-yield case. Update
   `emit_validates_against_declared_columns` (`differential_tests.rs:743`), which currently
   pins the broken behaviour.

Result assembly needs **no change** — `build_batch` in all four adapters already iterates all
declared schema fields and looks each up by name
(`uni-plugin-rhai/src/adapter_algorithm.rs:205`, and peers). N value columns already work.

Their fallback ask (reject multi-field `yields` at load) becomes unnecessary and should not be
implemented; it would foreclose the fix.

**This also closes a D3 failure class structurally.** With one CALL per (step, stock), a single
bad CALL yields an *inconsistent* `(x, y)` pair — which is how a Brusselator leaves its limit
cycle. One CALL emitting both columns cannot produce a torn pair, whatever D3's root cause
turns out to be.

---

## 6. WS-3 — D3: answers, then instrumentation

### The two questions, answered

**Q1: is anything process-global across independent `Uni` instances?**

Essentially no. The full audit of `graph_compute/`, `uni-plugin-rhai/`, and `uni-algo/`:

- **One** process-global mutable item: `static SESSION_EPOCH: AtomicU16` (`mod.rs:240`). It is
  **inert on the Rhai path** — `GcSession` owns its `Arc<Mutex<AlgoSession>>` directly
  (`uni-plugin-rhai/src/graph_compute.rs:44`), there is no session-id routing, and Rhai's
  `Scope` is fresh per CALL (`adapter_algorithm.rs:149`) so a handle cannot survive a CALL. The
  epoch is load-bearing only on the JSON registry path (Extism/WASM), where it wraps every
  65 536 sessions without rejection.
- **No global arena pool.** `Arena` (`mod.rs:471`) is four `usize` counters — pure accounting,
  holds no memory. `GraphArena` (`arena.rs:56`) lives inside the session's own handle table.
  Both are constructed per CALL (`adapter_algorithm.rs:136`).
- **Rhai `Engine`** is per-plugin `Arc<Engine>` (`runtime.rs:26`), built with the `sync`
  feature, `DummyModuleResolver` (deny-all, `engine.rs:62`), `eval` disabled (`:58`), and
  stateless kernel registrations that capture no session.
- **No rayon/thread pool** in GraphCompute kernels — the whole surface is serial under the
  session mutex. (`uni-algo` uses rayon, but the only path reachable from a GC session,
  `RandomWalk`, is order-independent by construction via the stateless counter-hash.)
- **No** memoization cache, interner, buffer pool, thread-local scratch, or `unsafe` in any of
  the three crates.
- `GraphComputeRegistry` is a struct field per loader, not a `static`, despite its
  "per-process registry" doc wording (`dispatch.rs:226`); session ids are UUIDv4.
- The one genuine global nearby, `projection_store.rs:189`, is correctly isolated per DB by
  `Arc` allocation identity with `Weak` + `ptr_eq`.

**Q2: is there an arena/handle integrity check?**

Yes, six unconditional gates on every resolution: epoch (`table.rs:242`), kind-tag (`:245`),
slot bound (`:83`), retired-slot with a distinct code `0x86B` (`:87`), generation (`:92`), live
value (`:95`). Handles pack as `[epoch:16 | kind:4 | gen:12 | slot:32]` (`handle.rs:20`).

**Handle recycling is structurally ruled out.** `Slab::free` bumps `generation` *before*
pushing the slot to the free list (`table.rs:143`), and `Slab::insert` returns the already
incremented generation (`:69`) — a recycled handle can never equal the stale one. Their
free-before-emit ordering is safe: `emit` resolves every column through `get_tensor`
(`session.rs:1665`) *before* capturing, so a freed handle fails loudly with `0x860`. Pinned by
`h5_generation_wrap_retires_slot` and `h6_double_free_and_stale_rejected` (`table.rs:411`,
`:439`).

So: **cross-db contamination, handle aliasing, and use-after-free are eliminated for the Rhai
path.** The fault is elsewhere.

### Candidate causes — all UNVERIFIED, each with its falsifying test

> **Superseded by §16.** Phase 0 ran these. C1 is **substantially weaker** than stated below:
> the `l0 = None` degrade is real at the bridge API, but both user-reachable routes to it
> hypothesized here measured green (Cypher `graphRef` and forks), and the sole production caller
> takes its L0 from the query context. Read §16 before acting on C1. C2 and C3 stand.

**C1 (was: strongest) — L0-invisible property reads.** If a CALL's procedure host is constructed
with `L0Context::empty()`, `l0_manager` is `None`, `projection.rs:285` pins no L0 snapshot, and
property reads see **flushed storage only** — i.e. an *older step's* value. For a Brusselator
write-back march that is precisely a plausible-but-wrong number from a different phase of the
limit cycle, which matches `y = 4.32` against an expected `0.889`.

> **Their flush experiment does not falsify this.** They forced `db.flush()` / 5.5 s sleeps and
> got byte-identical results — but on *passing* runs. At a 3-in-14 failure rate, byte-identity
> on a clean run carries no information about the failing one.

*Objection to state honestly:* if the read path were storage-only for **every** CALL, failures
would be near-constant, not rare. C1 therefore requires that only *some* CALLs take the
empty-L0 host construction — plausible given two distinct paths (`procedure_call.rs:770` DF
streaming vs `procedures_plugin/algo.rs:153` row path) selected by plan shape, but unproven.

*Falsifying test:* WS-6 makes the degrade loud. Running their suite against that build either
fires the assertion or exonerates the hypothesis in one run. **The fix and the diagnostic are
the same change** — which is why WS-6 should land before any further D3 forensics.

**C2 — stale named projection.** `procedures_plugin/algo.rs:648-654` returns a cached
`ProjectionStore` entry with **no staleness check** against current graph state. Correct math
over stale topology = a plausible wrong number, no error. *Applies only if they resolve a named
graphRef* — worth confirming, and worth fixing regardless.

**C3 — `projectAll` slot drift.** Under `projectAll: true`, concurrently-inserted unrelated
nodes shift slot numbering between CALLs, so any slot-keyed cross-CALL state reads
plausible-but-wrong. 3.0.0 made unscoped projection fail loud, so this requires an explicit
`projectAll` — likely excluded, but one question to them settles it.

### Instrumentation to ship (P1)

- **`gc-paranoid` cargo feature**: per-slot magic word + `debug_assert` in
  `Slab::get`/`get_mut`/`free` (`table.rs:32-39`, `:69-79`); a live-epoch registry detecting
  collision at `HandleTable` construction (`:161`).
- **Fail closed on epoch wrap** — `next_session_epoch` (`mod.rs:250`) returns `Result`. This
  closes the only true global and is already flagged in-tree as a follow-up.
- **`UNI_GC_TRACE` ring buffer**: every kernel call with `(epoch, kind, gen, slot)`, dumped on
  demand. This is the forensic tool their `chunkguard2.py` guard wants on our side of the
  boundary, and it does not perturb timing the way their inline check does.

---

## 7. WS-4 — D5: pre-declared multi-projection (P2)

The session layer needs nothing (§2). The constraint is that `AlgorithmHost::project`
(`bridge.rs:428`) is **async and HostQuery-gated**, and a Rhai kernel is a synchronous call —
so an in-kernel `gc.graph(scope_map)` would need async inside sync, which is the actual blocker
behind "Function not found".

**Design: declare N scopes at the CALL site; the host builds them all eagerly before the guest
runs.** This preserves every existing invariant — eager projection
(`adapter_algorithm.rs:109-123`), the capability gate, and the synchronous guest boundary —
and needs no new async machinery.

- CALL-site config gains a named-scope map; the adapter loops the existing
  `project_for_graph_compute` and `bind_graph`s each result.
- Guest surface: `gc.graph()` unchanged (primary); `gc.graph_named("agg")` → handle.

**Slot-consistency guarantee (the part they said they'd actually build on): we can give it, but
only for label/type projections.** `ProjectionBuilder::build` sorts and dedups vids
(`projection.rs:446-447`) and `IdMap::compact` asserts the sorted invariant
(`id_map.rs:77-83`), so slot *i* is the *i*-th smallest Vid — identical across any two
projections over the same node set, independent of scan order or L0/Lance split. **But
`GraphProjection::from_rows` (`projection.rs:700`), used for Cypher and Named projections,
interns in row order and deliberately does not sort** (`:751-757`). The guarantee must be
documented as scoped to label/type projections, and Cypher-projections either excluded from
multi-projection or fixed to sort.

**Ship-blocker: tensor provenance.** Multiple projections create a new silent-wrong-answer
class — a tensor from projection A used against projection B. `ewise` checks length, not shape
or origin (`session.rs:1004`), so two projections with coincidentally equal vertex counts would
silently compute garbage. WS-4 must therefore ship with a graph-provenance tag on tensor
handles and a cross-graph rejection. **Do not land multi-projection without it.**

Given they ship M3 on edge masks today and WS-1c gives them exact edge masks, this stays P2.

---

## 8. WS-5 and WS-6 — budget and L0 honesty

### WS-5 — D6, work budget (P1, mostly docs)

- **Document the formula and the charge table.** The full per-kernel charge model is now
  assembled (`spmv` charges `E`, `expand` charges Σ frontier degree, the O(V) family charges
  `n`, bitset ops charge *words* not elements, `emit` charges `rows·cols`) and is publishable
  as-is into `website/docs/plugins/graph-algorithms.md` and `skills/uni-db/references/` — where
  the budget is currently **entirely absent**.
- Correct their model on two points: the absolute ceiling `1e9` clamps above ~100k V+E, and a
  `GraphComputeWork` grant **replaces** rather than raises the size-derived default.
- **Add zero-charge introspection kernels** `work_budget()` / `work_spent()` /
  `work_remaining()`. `WorkBudget::remaining()` already exists (`mod.rs:406`) and is simply not
  reachable from any guest surface.
- **Determinism note to publish:** branching on `work_remaining()` makes a kernel's *result*
  depend on the capability grant, so the same program under two grants may differ. That does
  not violate the determinism contract (which is per-configuration) but it forfeits
  cross-grant reproducibility — and their NumPy oracle would see it as drift. For sizing chunks
  *before* a run, the documented formula is the better tool; the accessor is for adaptive
  kernels.

This removes the pattern they rightly disliked: discovering the budget by deliberately
triggering aborted CALLs.

### WS-6 — D7 and Theme C: make the L0 degrade loud (P0)

The five NaN preconditions for `edge_property`, to document precisely:

1. **L0-detached host** — `L0Context::empty()` upstream ⇒ no snapshot pin ⇒ flushed-storage-only
   reads. *This is the one that explains their conflicting evidence, and it is a silent degrade.*
2. **Uncommitted transaction writes** — invisible **by contract** (`projection.rs:494-496`);
   surfaced via `pending` on the DF path only (`procedure_call.rs:772`).
3. **Pinned version HWM** — `property_manager.rs:907` skips the transaction L0 entirely, `:935`
   drops entries above the HWM. Note a *plain fork* does not trigger this
   (`manager.rs:594-596`), so "it's a fork" is not itself the answer.
4. **Tombstoned edge in a newer L0 generation** (`property_manager.rs:928`).
5. **Non-numeric value type** — `as_f64` accepts only `Float`/`Int` (`value.rs:624`), so
   `Bool`, `String`, `Null`, and `Bytes` all read NaN.

**The fix, and the Theme-C fix generally:** replace the silent `Option`-degrade at
`projection.rs:285` with an explicit state. `L0Context::empty()` becomes a deliberate
`Detached` variant that the projection **rejects unless the caller opted in**, rather than
quietly reading stale storage. That is the same discipline 3.0.0 applied one layer down when it
closed G12 — applied where the decision is actually made.

Also add the missing **fork-path GraphCompute projection contract test** — the existing
coverage (`crates/uni/tests/common/graph_algo/projection_contract.rs:229`) is primary-only,
which is why this class survived the 12-gap sweep.

---

## 9. What is already in the tree

Three regression tests, **uncommitted**:

`crates/uni-plugin-builtin/src/algorithms/graph_compute/differential_tests.rs`
(`EwiseOp` added to the module's `use` list):

- `reqd1_compare_and_select_are_composable_from_shipped_kernels` — pins the compare/step/select
  identities against a scalar oracle.
- `reqd4_edge_mask_window_is_an_exact_selector_without_sampling` — pins `edge_mask_window` as an
  exact selector and its agreement with the degenerate sampler.

`crates/uni-plugin-rhai/src/graph_compute.rs`:

- `reqd1_compare_and_select_compose_through_the_rhai_guest_surface` — the same identities driven
  from a **Rhai guest script**, not the native trait. This is the surface the consumer writes
  against, so the trait-level proof alone would not have settled the question.

`cargo nextest run -p uni-plugin-builtin -E 'test(graph_compute)'` → **126/126 pass**.
`cargo nextest run -p uni-plugin-rhai -E 'test(reqd1_compare)'` → **1/1 pass**.

## 10. Priority

| ws | ask | size | why this rank |
|---|---|---|---|
| **WS-1a/b** | D1, D4 | hours | Unblocks their M5 *today* with no engine change; attacks the recurring Theme-A failure at its actual point of occurrence. |
| **WS-6** | D7, D3 | small | Silent-wrong-answer class; the fix *is* the D3 diagnostic. |
| **WS-2** | D2 | small | Removes n× redundant work and structurally kills the torn-pair failure mode. |
| **WS-1c** | D1 (`[E]`) | small | The one genuinely blocked case; also fixes `ewise`'s shape-blind length check. |
| **WS-3** | D3 | medium | Instrumentation; the two questions are already answered above. |
| **WS-5** | D6 | small | Docs + three zero-charge accessors. |
| **WS-4** | D5 | medium | They ship on masks today. **Must not land without tensor provenance.** |

## 11. Questions back to the consumer

1. Do your CALLs resolve a **named/Cypher graphRef**, or label/edgeType scopes? (Decides C2, and
   decides whether the WS-4 slot-consistency guarantee can cover you.)
2. Do you pass **`projectAll: true`**? (Decides C3.)
3. Are the failing CALLs issued **inside an open transaction**? (Decides D7 precondition 2.)
4. Does your kernel read state via `node_property` on values written by a prior `SET` in the
   same session — and is that read expected to see unflushed L0? (Decides C1.)

---

# Part II — Validation plan and implementation phases

## 12. Validation and reproduction plan

### 12.1 The gating rule

**No implementation lands before its reproduction is red and named in the commit.** Three
categories, and they are not interchangeable:

- **REPRO** — must **fail** on baseline `254b4c26c`. It is the proof a gap exists and the proof
  a fix worked. A "fix" with no red-first repro is unfalsifiable.
- **PIN** — passes today; locks in behaviour we are about to claim in writing (the D1/D4
  composition identities, sampler endpoint determinism). These exist because *documenting* a
  behaviour promotes it to contract, and contracts need tests.
- **PROBE** — outcome genuinely unknown until run. Listed honestly as such; the result decides
  scope. Do not predict these in review.

### 12.2 Reproduction inventory

| id | gap | level | expected on baseline |
|---|---|---|---|
| **PIN-D1a** | compare/step/select identities vs scalar oracle | native | pass *(landed)* |
| **PIN-D1b** | same identities through a Rhai guest script | guest | pass *(landed)* |
| **PIN-D4a** | `edge_mask_window` is an exact selector | native | pass *(landed)* |
| **PIN-D4b** | `sample_edges` bitwise reproducible across sessions + matches the per-edge oracle | native | pass — **test gap**: `sample` has S-1/S-2, `sample_edges` has neither |
| **REPRO-D1E** | `map_to_set` on an `[E]` tensor → `VertexSet`, rejected by `spmv_masked` | native | fail (`KIND_MISMATCH`) |
| **REPRO-EW** | `ewise` over a `[V]` and an `[E]` tensor of equal length silently computes | native | fail — silent wrong answer |
| **REPRO-PROV** | tensor from projection A applied against projection B | native | fail — silent wrong answer |
| **REPRO-D2/r,p,w,e** | manifest with 3 `yields`, kernel emits per column — one per loader | loader e2e | fail `0x869` |
| **REPRO-C1** | L0-detached host: write via `SET`, project without flush, read the property | integration | fail (NaN or stale) — **this is the D3/D7 repro** |
| **REPRO-C2** | named projection: project under a name, mutate the graph, re-CALL | integration | fail (stale, no error) |
| **REPRO-C3** | `projectAll: true` + concurrent unrelated insert → slot drift across CALLs | integration | fail or documented-as-designed |
| **REPRO-D6** | guest calls `gc.work_remaining()` | guest | fail (Function not found) |
| **REPRO-D5** | guest binds and reads a second, differently-scoped projection | guest | fail (Function not found) |
| **REPRO-NAN** | `sample_edges` with a NaN probability silently never fires | native | fail — silent, should be loud |
| **PROBE-FORK** | GraphCompute projection contract on a **fork** session | integration | **unknown** — no such test exists; `projection_contract.rs:229` is primary-only |
| **PROBE-D7** | each of the five NaN preconditions (§8) isolated | integration | **unknown per case** |

### 12.3 REPRO-C1 is the centrepiece

It is simultaneously the D7 answer, the strongest D3 candidate, and the Theme-C fix's
acceptance test. Construct it as a **shrinkable** reproduction, in this order:

1. Plain in-memory `Uni`, one label with one numeric property, `SET` then `CALL` a GC kernel
   reading it via `node_property` — **no flush**. Assert non-NaN and equal to the written value.
   (Expected to pass; `projection_contract.rs:229` covers this.)
2. Same, but routed through the host construction that yields `L0Context::empty()` — reach it
   via the executor path where `get_context()` returns `None`
   (`executor/core.rs:409`, `read.rs:398`). Assert the same. **This is the one expected to fail.**
3. Same on a **fork** session (PROBE-FORK).
4. Same inside an **open transaction** (expected to fail *by contract* —
   `projection.rs:494`; the test documents the contract rather than asserting a bug).
5. Same on a **pinned/time-travel** session (`property_manager.rs:907`).

Bisecting 1→5 names the precondition rather than guessing it. Only step 2's failure supports C1.

### 12.4 The forensic protocol we ask the consumer to run

D3 cannot be closed from our side — we cannot reproduce it and neither can they. What we can do
is make one run decisive:

1. Answer the four questions in §11 (they cost minutes and eliminate C2/C3 outright).
2. Re-run their suite against a **Phase 2a** build, which tags every L0-detached projection with
   a metric and a warning. One suite run either fires the tag on a failing CALL — confirming C1 —
   or exonerates it.
3. Keep their post-hoc `chunkguard2.py` guard armed. Our `UNI_GC_TRACE` ring buffer (Phase 6)
   records kernel calls and handle identity **on our side of the boundary**, without the timing
   perturbation that hides the bug when they check inline.

Phase 3 (multi-emit) is worth landing regardless of D3's root cause: with both columns emitted
from one CALL, a torn `(x, y)` pair becomes structurally impossible.

### 12.5 Oracle discipline

Every numeric REPRO/PIN compares against an oracle that **shares no code with the kernel** — the
existing `differential_tests.rs` invariant (§9.0 oracle-independence). For the composition
recipes that means a plain scalar loop in Rust, and for the consumer it means their NumPy replay
continues to be the real acceptance test: the op list lowers twice and byte-exactness is
structural. Where we add a kernel (Phase 4 `compare`), it must be differential-tested **against
the composition it replaces**, not only against an oracle — that is what proves the ergonomic
shortcut is semantically identical to the recipe we published in Phase 1.

---

## 13. Implementation phases

Ordered by consumer value per unit of risk, not by ask number. Phases 1–3 are the bundle that
matters to uniscape; 4–7 are engine hygiene and ergonomics.

### Phase 1 — Discoverability (WS-1a/1b) · ~2–3 days · no behaviour change

The unblock. Ships independently of everything else.

1. **De-triplicate the op-string parse.** Add `EwiseOp::from_op_name` / `MapOp::from_op_name` in
   `session.rs`; delegate all three sites (`dispatch.rs:461`/`:222`,
   `uni-plugin-rhai/src/graph_compute.rs:287`/`:117`,
   `uni-plugin-pyo3/src/graph_compute.rs:295`/`:144`). Three hand-maintained match arms are a
   drift hazard independent of this work, and the recipe table needs exactly one home.
2. **Recipe table in the rejection path** — unknown op → composition recipe; else the valid-op
   list. Seed from §4's table.
3. **`composition_recipes.rs`** differential suite: every published identity is a passing test
   vs a scalar oracle. A recipe that stops being true breaks CI.
4. **Single-source** the table into `website/docs/plugins/graph-algorithms.md` and
   `skills/uni-db/references/graph-algorithms.md`.

*Gate:* recipes suite green; 126 existing green; error-message assertions.
*Release:* patch — error text only. **Notify uniscape at this point; M5 is unblocked.**

### Phase 2 — L0 honesty (WS-6) · ~4–6 days · split 2a/2b

The correctness phase, and the D3 diagnostic. **Split, because fail-closed on a path we have not
surveyed is how you break working callers.**

- **2a (safe, ships first):** `L0Context::empty()` becomes an explicit `Detached` variant.
  Projection **warns + emits a metric + tags the result** when it builds against a detached
  context. Document the five NaN preconditions. Land PROBE-FORK and the REPRO-C1 ladder.
- **2b (breaking, ships after a release of telemetry):** projection **rejects** a detached
  context unless the caller explicitly opts in. Gate on 2a's metric showing no legitimate
  callers, or on having migrated the ones that exist.

*Gate:* REPRO-C1 step 2 red → green at 2b; PROBE-FORK result recorded either way; full
`uni-query` + `uni` suites; TCK run (this phase touches query executor paths).
*Release:* 2a minor, 2b `feat!`.
*Risk:* the survey is the work. If 2a's telemetry shows detached projections are common and
legitimate, 2b becomes an opt-in strictness flag instead — decide on evidence, not now.

### Phase 3 — Multi-emit (WS-2) · ~3–4 days

1. Move the "every declared column present" loop (`session.rs:1652-1658`) out of `emit` to
   session close. Keep all per-call checks.
2. `self.emitted = captured` (`:1695`) → upsert/extend; widen the duplicate check to already
   captured names.
3. Batch shims: Rhai `gc.emit(#{...})` arity-1 overload (precedent `zero_map_typed`, `:1019`);
   Python `gc.emit({...})`; `names[]`/`handles[]` on the JSON wire (`dispatch.rs:107`).
   Single-name form keeps working.
4. **Fixtures (the Theme-B fix):** a ≥2-yield case in every loader conformance fixture.

**On `emit_validates_against_declared_columns` (`differential_tests.rs:743`):** it currently
pins the broken behaviour. It gets **retargeted, not weakened** — after the fix a missing column
is still an error, just detected at session close rather than mid-call. The assertion that a
declared-but-unemitted column fails must survive verbatim in spirit; only its trigger point
moves. Anything less is weakening a test to make a change pass.

*Gate:* REPRO-D2 red → green on all four loaders; result assembly untouched (`build_batch` is
already N-column).
*Release:* minor — strictly relaxing.

### Phase 4 — Tensor identity discipline + `compare` (WS-1c) · ~4–5 days

Grouped because they are one idea: a tensor's *shape and origin* are part of its identity, and
today neither is checked.

1. **`ewise` validates `Shape` equality, not just `len()`** (`session.rs:1004`) — fixes
   REPRO-EW, a live silent-wrong-answer bug.
2. **Graph-provenance tag** on tensor handles + cross-graph rejection — fixes REPRO-PROV and is
   the **prerequisite for Phase 7**.
3. **`map_to_set` becomes shape-polymorphic**: `[E]` in → `EdgeSet` out. Fixes REPRO-D1E and
   delivers REQ-D4's `edge_mask` with no new kernel.
4. **New `compare(a, b, CmpOp)` kernel** — closed enum `{Gt,Ge,Lt,Le,Eq,Ne}`, host-evaluated,
   shape-preserving, charges `n` **before** the loop (matching `spmv`'s admission control rather
   than `ewise`'s charge-after-compute). Differential-tested against the Phase 1 recipe.
5. **Optional `select(m,a,b)`** — not requested; justified only by the NaN-poisoning trap
   (`0.0 * NaN = NaN` in the blend), which is the G2-shaped composability-blocked argument.
   Defer if review disagrees.

*Gate:* the three REPROs flip; catalog parity is auto-enforced (`dispatch.rs` exhaustive match
compiles, rhai/pyo3 reachability tests fire).
*Release:* `feat!` — items 1 and 3 change behaviour.
*Note:* adding to the `kernels!` catalog (`kernel_id.rs:91`) is one line and the compiler plus
two reachability tests drive the rest. WASM and Extism need **zero** changes.

### Phase 5 — Budget (WS-5) · ~2 days

1. Publish the formula `min(10_000·(V+E+1), 1e9)` and the **full per-kernel charge table** into
   `website/docs/` and `skills/` (currently absent from `skills/` entirely).
2. Correct two consumer misconceptions: the `1e9` ceiling clamps above ~100k V+E, and a
   `GraphComputeWork` grant **replaces** rather than raises the default.
3. Zero-charge kernels `work_budget()` / `work_spent()` / `work_remaining()` —
   `WorkBudget::remaining()` (`mod.rs:406`) already exists and is simply unreachable.
4. Document the determinism caveat: branching on `work_remaining()` makes results depend on the
   capability grant, forfeiting cross-grant reproducibility — which their NumPy oracle would
   read as drift. For pre-sizing chunks the formula is the right tool.

*Gate:* REPRO-D6 flips; a test asserting the documented formula equals the code (so the doc
cannot rot).
*Release:* minor.

### Phase 6 — Instrumentation (WS-3) · ~3 days

1. **`gc-paranoid` feature**: per-slot magic word + `debug_assert` in `Slab::get`/`get_mut`/`free`
   (`table.rs:32-39`, `:69-79`); live-epoch registry detecting collision at `HandleTable`
   construction (`:161`).
2. **Epoch-wrap fail-closed** — `next_session_epoch` (`mod.rs:250`) returns `Result`. Closes the
   only true process-global; already flagged in-tree as a follow-up. Live for Extism/WASM even
   though inert for Rhai.
3. **`UNI_GC_TRACE` ring buffer** — kernel call + `(epoch, kind, gen, slot)`, dumped on demand.

*Gate:* an epoch-wrap test; `gc-paranoid` builds and runs in CI.
*Release:* minor, feature-gated.

### Phase 7 — Multi-projection (WS-4) · ~5–7 days · **blocked on Phase 4**

1. CALL-site config gains a named-scope map; the adapter loops the existing
   `project_for_graph_compute` and `bind_graph`s each result — **eagerly, before the guest runs**,
   preserving the synchronous guest boundary and the HostQuery gate. An in-kernel
   `gc.graph(scope_map)` would need async inside a sync Rhai call, which is the real reason it
   returns "Function not found".
2. Guest surface: `gc.graph()` unchanged; `gc.graph_named("agg")`.
3. **Slot-consistency guarantee**, scoped honestly: label/type projections sort+dedup vids
   (`projection.rs:446`, asserted by `id_map.rs:77`) so slot *i* is the *i*-th smallest Vid.
   `from_rows` (Cypher/Named, `:700`) interns in row order and does **not** — either sort it or
   exclude it from the guarantee, and say which in the docs.
4. Provenance rejection from Phase 4 must hold across the new handles.

*Gate:* REPRO-D5 flips; REPRO-PROV still green with two live projections.
*Release:* minor.
*Do not land without Phase 4 item 2.* Multiple projections plus a length-only equality check is
a new silent-wrong-answer class, which is the exact defect family this whole document is about.

---

## 14. Verification gates, sequencing, and risk

### 14.1 Per-phase gate (all must pass before merge)

```
cargo nextest run                       # workspace
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo doc -D warnings
```

Plus, conditionally: Python bindings via **`uv`** (not poetry — this repo is uv end-to-end) for
any phase touching PyO3 (3, 4, 5); `scripts/run_tck_with_report.sh` for any phase touching
query-executor paths (2); the four loader e2e suites for any phase touching the kernel surface
(3, 4, 5, 7).

Run `rustup update stable` before trusting clippy/fmt — CI's stable floats and has bitten this
repo before.

### 14.2 Sequencing

```
Phase 1 ──────────────► (uniscape unblocked; independent of all else)
Phase 2a ─► Phase 2b    (2b gated on 2a telemetry)
Phase 3  ──────────────► (independent)
Phase 4  ─────┬────────► (independent)
              └──────► Phase 7   (needs provenance)
Phase 5  ──────────────► (independent)
Phase 6  ──────────────► (independent; helps D3 forensics)
```

Phases 1, 3, 4, 5, 6 are mutually independent and parallelisable. The only hard edge is
4 → 7. Phase 2a should precede the consumer's next D3 hunt.

### 14.3 Risks

| risk | phase | mitigation |
|---|---|---|
| Fail-closed L0 breaks legitimate detached callers | 2b | 2a telemetry first; decide on evidence. Fall back to an opt-in strictness flag. |
| `ewise` shape equality breaks a caller relying on length-only | 4 | Survey call sites; the behaviour it forbids is a silent wrong answer, so any caller depending on it is already broken. |
| Retargeting the emit test reads as weakening it | 3 | The assertion survives; only its trigger point moves. Call this out in the commit body explicitly. |
| Multi-projection ships without provenance | 7 | Hard dependency stated; gate PR on Phase 4 item 2 merged. |
| PROBE-FORK reveals a larger fork-path gap | 2a | Genuinely unknown. If it fails, it is scoped as its own work item, not absorbed silently into Phase 2. |
| Recipes drift from engine behaviour | 1 | Recipes are tests, and docs are generated from the same table. Drift breaks CI. |

### 14.4 What ships when, from the consumer's view

| phase | what uniscape gets |
|---|---|
| **1** | **M5 unblocked** — thresholds, Schelling, bounded confidence, SD `IF-THEN-ELSE`, all expressible today. Their `UnsupportedRuleError` boundary moves. |
| **2a** | A decisive D3 experiment: one suite run confirms or kills C1. |
| **3** | One CALL per report step instead of n. Torn `(x,y)` pairs become structurally impossible. |
| **4** | `compare` at 1× budget instead of 3×; exact edge masks without the sampler idiom. |
| **5** | Exact chunk sizing without provoking aborts. |
| **7** | The second scoped projection, with a stated slot-consistency guarantee. |

---

# Part III — Phase 0 results (measured 2026-07-25)

## 15. Predicted vs measured

Every row below was run. Where a prediction from Part I was wrong, the row says so
rather than being quietly reclassified.

| id | predicted | measured | note |
|---|---|---|---|
| PIN-D1a compare/select identities | GREEN | **GREEN** | native trait level |
| PIN-D1b same via Rhai guest script | GREEN | **GREEN** | the surface the consumer writes |
| PIN-D4a `edge_mask_window` exact selector | GREEN | **GREEN** | |
| PIN-D4b `sample_edges` reproducible across sessions | GREEN | **GREEN** | closed a real coverage gap |
| PIN-D4b `sample_edges` vs per-edge oracle | GREEN | **GREEN** | ditto |
| REPRO-D1E `map_to_set` on `[E]` | RED | **RED** | `0x861` "expected EdgeSet" |
| REPRO-EW `ewise` shape-blind | RED | **RED** | silently computed |
| REPRO-PROV cross-projection tensor | RED | **RED** | silently computed |
| REPRO-NAN NaN probability | RED | **RED** | silently never fires |
| REPRO-D2 two-column emit | RED | **RED** | `0x869`, **at the first emit** |
| D2 multi-field `yields` at load | — | **accepted** | constraint undiscoverable until CALL |
| REPRO-C1 rung 2 (`l0 = None` bridge) | RED | **RED**, but see §16 | projects an *empty* graph, not stale values |
| PROBE-FORK rung 3 | unknown | **GREEN** | forks project identically to their parent |
| P0.4 Cypher `graphRef` L0-blind | RED | **GREEN — hypothesis falsified** | §16 |
| P0.5 bindings G9 drift | probe | **INCONCLUSIVE** | §17 |

## 16. Correction: candidate C1 for REQ-D3 is substantially weaker than Part I claimed

§6 named an L0-detached projection "the strongest candidate" for the rare wrong-value report,
and §3 Theme C framed it as a live, user-reachable silent degrade. Phase 0 supports the first
half and refutes the second.

**What holds.** A bridge built with `l0 = None` really is blind: rung 2 projects a graph with
**zero vertices** against committed-but-unflushed data, with no error. (Note the failure mode is
*invisibility*, not staleness — staleness is what a *partially* flushed database would produce.)

**What does not.** Both user-reachable routes to that construction that Part I hypothesized are
green:

- A Cypher-mode `graphRef` sees committed-unflushed rows correctly. The theory was that
  `QueryProcedureHost::from_components` hardcoding `L0Context::empty()`
  (`procedure_host.rs:167`) would leave the resolver blind. It does not: the Cypher/Named path
  routes projection through an **injected resolver** (`run_algorithm_provider`,
  `procedures_plugin/algo.rs:688`, issue #151 P3), so that empty context never serves the
  selection queries.
- A forked session projects identically to its parent.

Furthermore there is exactly **one** production caller of `host_bridge_from_storage`
(`procedures_plugin/algo.rs:693`), and its `l0_manager` argument comes from the query context —
`None` only when `Executor::get_context()` yields `None`, i.e. a read-only session, which cannot
hold unflushed writes of its own.

**Revised status of C1: no user-reachable path found.** The degrade is real at the bridge API and
worth closing on fail-loud grounds (Phase 2 still earns its place), but it should no longer be
presented to the consumer as the leading explanation for their wrong value. **REQ-D3's cause
remains open**, and the L0 theory should not absorb their debugging time. C2 (stale named
projection) and C3 (`projectAll` slot drift) are untouched by this and remain live — the four
questions in §11 still discriminate them.

Theme C survives as a *code-health* finding (a silent `Option`-degrade at a boundary whose
contract is "fail loud") rather than as an active-bug finding.

## 17. P0.5 could not be answered

`bindings/uni-db/tests/test_graph_compute_plugin.py` issues unscoped CALLs (`:128`, `:130`,
`:153`) that the G9 scoped-or-loud contract should reject. Both tests pass — but against a
prebuilt `uni_db/_uni_db.abi3.so` dated **2026-07-21 02:55**, while the G9 closure landed in
`254b4c26c` on **2026-07-23**. The wheel predates the contract, so the passing result says
nothing about current `main`.

Answering this needs a fresh maturin build. Folded into Phase 3, which touches the PyO3 surface
anyway. Recorded here so the open question is not mistaken for a clean bill of health.

## 18. Where the Phase 0 tests live

| file | contents |
|---|---|
| `crates/uni-plugin-builtin/…/graph_compute/differential_tests.rs` | PIN-D1a, PIN-D4a, PIN-D4b ×2, REPRO-D1E, REPRO-EW, REPRO-PROV, REPRO-NAN |
| `crates/uni-plugin-rhai/src/graph_compute.rs` | PIN-D1b |
| `crates/uni/tests/common/loaders/rhai_multi_emit_repro.rs` | REPRO-D2, load-time acceptance, first-emit diagnosis |
| `crates/uni/tests/common/graph_algo/projection_contract.rs` | REPRO-C1 (rung 2), shared `seed_property_graph` / `property_spec` helpers |
| `crates/uni/tests/common/graph_algo/graph_compute_l0_ladder.rs` | PROBE-FORK, the falsified-then-pinned `graphRef` test |

Every REPRO carries `#[ignore]` with a reason naming the phase that flips it, so the default
suite stays green while the evidence stays runnable:

```
cargo nextest run --run-ignored all -E 'test(repro_)'     # must be RED until its phase lands
```

Two tests deliberately pin *current* behaviour as Phase 0 evidence and are scheduled for
deletion by the phase that fixes them: `d2_diagnosis_the_first_emit_is_what_fails` (Phase 3) and
the `#[ignore]` reasons themselves.
