# Proposal: Capability-based embedding alias model (fixes #129 + #130)

Status: **Implemented** (rev. 3 — open-validation + write-routing landed with tests; §4.2/§4.3/§4.4
mark what was built and how it diverged from rev. 2's recommendation)
Author: (design)
Date: 2026-06-29
Scope: `uni` (open-time validation), `uni-store` (auto-embed write routing + `embed_caps` helper).
Follow-ups: `uni-query` (query-time routing), #132 (consolidation hang).

## 1. Problem

uni-db supports three **text** embedding flavors, each backed by its own vector column and
auto-embeddable from a text column via an `EmbeddingConfig { alias, .. }`:

- **dense** (`DataType::Vector`) — pooled vector;
- **sparse** (`DataType::SparseVector`) — learned-sparse term weights (SPLADE / BGE-M3 sparse);
- **multi-vector** (`DataType::List<Vector>`, ColBERT/MaxSim) — per-token vectors.

The alias names a model in the Uni-Xervo catalog. In uni-xervo 0.17.0 each model implements
**exactly one** embedding trait, fixed by its `ModelTask` (verified — the trait families are
mutually exclusive, `traits/hybrid.rs:1-18`):

| `ModelTask` | trait | heads |
|---|---|---|
| `Embed` | `EmbeddingModel` | dense |
| `EmbedSparse` | `SparseEmbeddingModel` | sparse |
| `EmbedMultiVector` | `MultiVectorEmbeddingModel` | multi-vector |
| `EmbedHybrid` | `HybridEmbeddingModel` | **1–3 heads from one forward pass** (BGE-M3, `aapot/bge-m3-onnx`) |
| `EmbedImage`/`EmbedAudio`/`EmbedMultimodal` | image/audio/multimodal | dense, **non-text input** |

A hybrid model is the *only* multi-head mechanism: it runs one shared encoder pass and
post-processes the requested heads (`HybridEmbeddingModel::embed(texts, HeadSet)`).

Two filed issues — **#129** (enhancement/perf) and **#130** (bug) — are the **write-side** and
**validation-side** of one underlying defect: **routing and validation are keyed on a rigid
one-task-per-alias assumption instead of on model capability** ("which heads can this alias's model
produce, and do they cover what this column needs?"). A third issue, **#132** (intermittent
consolidation hang), is aggravated by the workaround the defect forces.

### Observed symptoms

- **#130** — A KB with a `Vector` auto-embed index whose alias task is `EmbedHybrid` is *created*
  successfully but **cannot be reopened**:
  `Internal error: Uni-Xervo alias 'embed/hybrid' must be an embed task`.
- **#129** — A hybrid model cannot auto-embed a *lone* dense `Vector` column on its alias; `CREATE`
  fails: `Capability mismatch: Model for alias 'embed/hybrid' does not implement EmbeddingModel`.
  Workaround: register the model under two aliases (one `Embed`, one `EmbedHybrid`). The model
  registry is keyed by `ModelRuntimeKey { task, provider_id, model_id, revision, variant_hash }`
  (verified, `uni-xervo api.rs:158-192`) — **`task` is part of the key** — so the two aliases are
  two distinct registry entries: the weights **load twice** and run **two forward passes**.
- **#132** — The two-alias workaround doubles embedding work on the heavy sparse/multi-vector path
  during the post-ingest consolidation sweep (separate lost-wakeup root cause; see §7).

## 2. Root cause

| # | Face | Site | Mechanism |
|---|------|------|-----------|
| **#130** | open-time validation | `crates/uni/src/api/mod.rs:3517` | `if spec.task != ModelTask::Embed { return Err("must be an embed task") }`, applied to **every** `IndexDefinition::Vector` alias (verified). Multi-vector is *also* `IndexDefinition::Vector`, so an `EmbedMultiVector`/`EmbedHybrid` alias is rejected. Sparse aliases are **not validated at all** (`mod.rs:3488` collects `Vector` only). Latent at create (empty persisted schema → no aliases collected), fires at reopen. |
| **#129** | write-time routing | `crates/uni-store/src/runtime/writer.rs:115-164` | `embed_group` routes by **head count**: `heads_wanted > 1` → `hybrid_embedder` (line 129); `heads_wanted == 1` → a **narrow single-task facade** — lone-dense → `runtime.embedding(alias)` (line 162), lone-multi → `multi_vector_embedder` (156), lone-sparse → `sparse_embedder` (159). A hybrid model implements only `HybridEmbeddingModel`, so the lone-head narrow call fails with `CapabilityMismatch`. |

**Inseparability.** Fixing only #130 is incoherent: once open-validation accepts an `EmbedHybrid`
alias on a dense `Vector` index, the lone-dense column reaches `writer.rs:162` at ingest and fails
with #129's mismatch. The error merely moves from reopen-time to write-time. **#129 and #130 must
ship together.**

## 3. Current architecture (verified against source)

### 3.1 Schema / type layer — `crates/uni-common/src/core/schema.rs`
- `DataType`: `Vector { dimensions }` (180), `SparseVector { dimensions }` (185), multi-vector as
  `List(Box<Vector>)` — **no dedicated multi-vector `DataType`**.
- `IndexDefinition`: `Vector(VectorIndexConfig)` (844) covers **both dense and multi-vector**
  (resolved from the column's `DataType`); `Sparse(SparseVectorIndexConfig)` (850) is its own variant.
- `EmbeddingConfig { alias, source_properties, batch_size, document_prefix, query_prefix }` (965-979).
  **The schema persists only the alias name — never the task or head set.** Task is known only by
  catalog lookup at runtime, which is why #130 is create/reopen-asymmetric.
- **The alias is per-column**: each `VectorIndexConfig` / `SparseVectorIndexConfig` carries its own
  `Option<EmbeddingConfig>`. Different columns on the same label can freely use different aliases.

### 3.2 Write / auto-embed — `crates/uni-store/src/runtime/writer.rs`
- `collect_embed_groups` (167-210): groups a label's auto-embed targets into a
  `BTreeMap<(alias, source_properties), EmbedGroupSpec>`. **The grouping key already does the right
  thing for every topology** (see §4.0): same alias + same source text → one group → one embed call;
  different alias or different source → separate groups → separate calls.
- `EmbedGroupSpec { source_properties, document_prefix, dense, multi, sparse: Vec<String> }` (70-76):
  the three head buckets are independent `Vec`s, so a group can legitimately want any subset.
- `embed_group` (115-164): picks the facade by head count (the #129 defect for 1 head).
- Source is **text-only** (verified): inputs are collected via `val.as_str()` (writer ~3676/3777);
  non-text values are silently skipped. There is **no** image/audio/binary auto-embed path. This is
  why image/audio embedding tasks are *not* valid auto-embed targets (§4.1), independent of the
  dense vector shape they happen to produce.
- Head results are split into the correct columns; user-supplied values are never overwritten
  (`contains_key`/`user_touched` guard).

### 3.3 Uni-Xervo runtime surface — `uni-xervo 0.17.0` (verified)
- **Trait ↔ task is 1:1 and mutually exclusive** (`traits/hybrid.rs:1-18`,
  `provider/local_onnx`): a model implements one of `EmbeddingModel` / `SparseEmbeddingModel` /
  `MultiVectorEmbeddingModel` / `HybridEmbeddingModel`. So routing capability == `task`.
- `HeadSet` is a `bitflags` over `DENSE | SPARSE | MULTI_VECTOR` (+ `ALL`) (`traits/hybrid.rs:25-43`).
  **This is the head-set type we reuse end to end** (validation, routing, and the `embed` call all
  speak `HeadSet`).
- `HybridEmbeddingModel` (`traits/hybrid.rs:78-104`):
  - `embed(texts, heads: HeadSet) -> HybridEmbedResult` — populates `heads ∩ available_heads()` from
    one forward pass; `HybridEmbedResult { dense, sparse, multi_vector: Option<…> }`.
  - `available_heads() -> HeadSet` — **per-model ground truth**. A hybrid model may expose a *subset*
    (e.g. dense+sparse only). **Requesting an absent head yields `None`, not an error** (verified,
    hybrid.rs:79-94). ← the silent-loss hazard the design must close (§4.3).
- Facade methods and their downcast-failure errors (verified, `runtime.rs`):
  `embedding()` → `CapabilityMismatch`; `sparse_embedder()` / `multi_vector_embedder()` /
  `hybrid_embedder()` → `ProviderCapabilityMissing`. (Asymmetry matters for routing-option B, §4.2.)
- **No public accessor returns an alias's `ModelTask`/spec** (`lookup_spec` is private; only
  `contains_alias` exists). `available_heads()` is reachable **only after loading** the model via
  `hybrid_embedder(alias)`. This constrains where capability signals come from (§4.1/§4.3).
- Registry dedup is by `ModelRuntimeKey` **including `task`** (`api.rs:158-192`) — confirming the
  two-alias workaround double-loads.

## 4. Design

One **shared capability model**, expressed in uni-xervo's own `HeadSet`, drives both fix sites (and,
later, query-time routing): map an alias's catalog `ModelTask` → the heads it can produce from
**text**, and require that set to cover the heads its bound columns need.

```
text_embedding_heads(task) -> HeadSet:
  Embed            -> DENSE
  EmbedSparse      -> SPARSE
  EmbedMultiVector -> MULTI_VECTOR
  EmbedHybrid      -> DENSE | SPARSE | MULTI_VECTOR   (UPPER BOUND — see §4.3)
  (EmbedImage | EmbedAudio | EmbedMultimodal | Rerank | Generate | Raw | Nlp | …) -> empty
```

`ModelTask` is `#[non_exhaustive]`, so this is an **allow-list** (`matches!`) — unknown future
variants map to empty (rejected) rather than silently passing.

**Why image/audio/multimodal map to empty here.** They emit a dense *vector*, but they consume
**images/audio**, not text — and auto-embed's source is always a text column (§3.2). Including them
in `DENSE` would let a dense column bind an `EmbedImage` alias, pass open-validation, then fail at
write (`runtime.embedding` downcasts to `EmbeddingModel`, which an image model does not implement).
That is exactly the create→reopen asymmetry this proposal exists to kill — relocated to a new task.
The table keys on **(head shape, for a text source)**, not on output shape alone. (If
image/audio/blob auto-embed is ever wanted, it is a separate modality-routing feature with its own
source-column type and facade.)

A column's required head is derived exactly as the writer already does:
dense `Vector` → `DENSE`, `List<Vector>` → `MULTI_VECTOR`, `SparseVector` → `SPARSE`.

**Invariant (open-time, necessary):** for each alias,
`union(required heads of its bound columns) ⊆ text_embedding_heads(alias.task)`.

**Invariant (write/load-time, sufficient):** for a hybrid alias,
`required ⊆ available_heads(loaded_model)` — closes the partial-hybrid silent-loss gap (§4.3).

### 4.0 Topology coverage (what this enables)

The grouping key `(alias, source_properties)` + capability routing supports every mix of separate
and shared models. The four cases, and what each needs from this change:

| # | Topology | Grouping | Routing | Passes | Status today | Fix needed |
|---|----------|----------|---------|--------|--------------|------------|
| **T1** | 3 separate single-task models (dense=`Embed`, sparse=`EmbedSparse`, multi=`EmbedMultiVector`) | 3 groups (distinct aliases) | each → its narrow facade | 3 | **works** | open-validation per-alias (already passes); no routing change |
| **T2** | one `EmbedHybrid` for all 3 (shared source) | 1 group | `hybrid_embedder`, `HeadSet::ALL` | **1** | routing works; **#130 blocks reopen** | open-validation (§4.1) |
| **T3** | hybrid for some + other model(s) for the rest (e.g. dense+sparse on `EmbedHybrid`, multi on a separate `EmbedMultiVector`) | ≥2 groups | hybrid group → `hybrid_embedder` w/ `DENSE\|SPARSE`; multi group → `multi_vector_embedder` | 2 | routing works; **#130 blocks reopen of the hybrid group** | open-validation (§4.1) |
| **T4** | hybrid used for a **single** head (e.g. only the dense column points at the `EmbedHybrid` alias) | 1 group, `heads_wanted==1` | **→ narrow `embedding()` → `CapabilityMismatch`** | — | **broken (#129)** | routing (§4.2) |

Note the single-pass benefit in T2/T3 holds only when columns share **both** alias **and**
`source_properties` (same text in → same encoder pass). Different source text → different groups →
different passes, by design.

### 4.1 Open-time validation (#130) — `crates/uni/src/api/mod.rs:3483-3523`

Replace the blanket `task != Embed` check with the (necessary) capability invariant:

1. Collect required heads **per alias** across **both** `IndexDefinition::Vector` (classifying
   dense vs multi-vector by the column `DataType`) **and** `IndexDefinition::Sparse` (closing the
   unvalidated-sparse gap).
2. For each alias: resolve its catalog spec (existing "missing alias" error stays); assert
   `required ⊆ text_embedding_heads(spec.task)`, else error naming the alias, its task, the required
   heads, and the offending column.

The catalog (`Vec<ModelAliasSpec>`, with `.task`) is already in scope here (`mod.rs:3509`), so no
plumbing is needed at this site. This is a **task-level** check and intentionally does **not** load
models; the partial-hybrid case is caught later (§4.3).

### 4.2 Write-time routing (#129) — `crates/uni-store/src/runtime/writer.rs` `embed_group` *(IMPLEMENTED — Option B)*

The `heads_wanted > 1` branch is unchanged (it already routes to `hybrid_embedder`). The three
**lone-head** branches now **try the narrow facade first, then fall back to the hybrid embedder on a
capability error**:

```
embedding(alias) | sparse_embedder(alias) | multi_vector_embedder(alias)
  Ok(model)                                   -> use it
  Err(CapabilityMismatch | ProviderCapabilityMissing)
    -> hybrid_embedder(alias).embed(texts, <single-head HeadSet>)  // reuses the loaded model
       then extract that head (None -> hard error, never a silent skip)
  Err(other)                                  -> propagate (real load / inference failure)
```

**Why Option B rather than Option A (plumb `alias → ModelTask`).** Routing by task needs the task at
the writer, but uni's most common path — and the entire test suite — builds the `ModelRuntime`
externally and passes it via `.xervo_runtime(prebuilt)`. On that path uni never sees a
`Vec<ModelAliasSpec>`, and the runtime exposes no task accessor (`lookup_spec` is private). Option A
would therefore fix #129 only for `.xervo_catalog(..)` users and leave the prebuilt-runtime path
(including the repros) broken. Option B is path-independent.

The error-asymmetry worry that rev. 1 raised against B is handled by matching **both**
`CapabilityMismatch` *and* `ProviderCapabilityMissing` as "wrong facade, try hybrid" (helper
`is_capability_mismatch`); a model that is genuinely incapable simply fails again at
`hybrid_embedder`, surfacing a clear error. Crucially this is **not** a double-load: a failed narrow
call loads the model keyed by its real task (`EmbedHybrid`), so the hybrid retry is a cache hit
(`ModelRuntimeKey` includes `task`). Verified by the #129 test asserting `load_count == 1`.

### 4.3 Partial-hybrid head-availability (correctness) *(guard already existed; preserved)*

`HybridEmbeddingModel` may expose only a subset of heads, and **requesting an absent head returns
`None`, not an error** (§3.3). Rev. 1 feared the writer would then silently write nothing. In fact
the existing `heads_wanted > 1` path **already** converts a missing requested head into a hard error
(`res.dense.ok_or_else(..)` in `embed_group`). So there was no silent-loss bug to close — the work is
to **preserve that guard** on the new lone-head→hybrid fallback path. The implemented fallback does
exactly this: it extracts the one requested head with the same `ok_or_else`, so a hybrid model
lacking that head fails loudly (test `partial_hybrid_missing_head_errors`). An eager
`available_heads()` pre-check (fail before the forward pass) was deemed unnecessary given the
post-embed guard already gives a hard error; it remains an optional future nicety.

### 4.4 Shared helper — home (open question 2, resolved) *(IMPLEMENTED — uni-store)*

`text_embedding_heads(task)` takes a `ModelTask` and returns a `HeadSet`. It lives in
**`crates/uni-store/src/runtime/embed_caps.rs`** (a registry `uni-xervo` is not editable here without
a publish cycle), alongside `required_embed_heads(schema)` (the per-alias `DataType → HeadSet`
requirement) and the relocated `is_multivector_property` classifier. uni-store already depends on
uni-xervo, and uni-db depends on uni-store, so both the writer (routing) and `api/mod.rs` (open-time
validation) consume one definition, and validation/routing/`embed` all speak the same `HeadSet`.
Promoting the task→heads mapping upstream into uni-xervo (`impl ModelTask`) remains a clean future
refactor.

### 4.5 Follow-up (not in this change): query-time routing — `uni-query`

`auto_embed_query` (`crates/uni-query/src/query/df_graph/similar_to_expr.rs:633-711`) always calls
`runtime.embedding(alias)` and returns a dense `Vec<f32>`; its fallback scans only `Vector` indexes
(verified, line 656). So `similar_to(text, multivector_or_sparse_property)` silently embeds as dense
(wrong shape). Sparse/multi-vector **text-query** auto-embed is effectively unimplemented. Fixing
this means routing query embedding by the target column kind (reusing the same `HeadSet` classifier)
and returning the matching shape — tracked separately to keep this change reviewable.

## 5. Test plan

Run with `cargo nextest run` (per repo convention). Each test below states what it asserts **and**,
where relevant, **what it does on 2.4.1** — a regression test that doesn't fail before the fix proves
nothing.

### 5.0 Test infrastructure (reuse, don't reinvent)

Real doubles already exist; the plan builds on them:

- **Load vs forward-pass counters.** uni-xervo `mock.rs` `MockProvider` exposes `load_count`
  (`AtomicU32`) and per-model `call_count`. These two counters are the crux of "verify model
  loading": `load_count` answers *were the weights instantiated?*, `call_count` answers *how many
  forward passes ran?* They move independently and must be asserted separately.
- **Configurable hybrid double.** `crates/uni/tests/hybrid_autoembed.rs::CountingHybrid` is a
  `HybridEmbeddingModel` with a shared `Arc<AtomicUsize>` embed counter and a chosen
  `available_heads()`. Extend it so `available_heads()` and a `load` counter are both
  test-configurable (today it returns a fixed `DENSE|MULTI_VECTOR` subset — already a subset, which is
  exactly what §5.5 needs). Register via `ModelRuntime::builder().register_provider(..).catalog(vec![spec(alias, task, ..)])`.
- **Reopen idiom.** `Uni::open(path).build()` across `drop` scopes (see `recovery_index_no_rebuild.rs`,
  `autoembed_parity.rs`). **Reopen is mandatory** for the #130 class — see §5.4.
- **Homes for the new tests.** Extend `hybrid_autoembed.rs` (hybrid routing, load counts),
  `sparse_autoembed.rs` (sparse validation gap), `multivec_autoembed.rs` (multi-vector #130 face),
  `autoembed_parity.rs` (cross-modality + reopen); add `embedding_alias_capability.rs` for the open-
  validation matrix.

### 5.1 Reported-issue repros (must fail on 2.4.1)

- **#130** — create a KB with a dense `Vector` auto-embed index on an `EmbedHybrid` alias, write a
  row, `drop`, **reopen**. *2.4.1:* reopen errors `… must be an embed task`. *Fixed:* reopen
  succeeds and the persisted embedding is intact. Mirror for the **multi-vector** index on
  `EmbedMultiVector` (the multi-vector face of the same blanket check) and a **sparse** index on
  `EmbedHybrid`.
- **#129** — single dense column on an `EmbedHybrid` alias (T4), no second alias. *2.4.1:* `CREATE`
  fails `… does not implement EmbeddingModel`. *Fixed:* succeeds; **`load_count == 1` and
  `call_count == 1` per batch** on that alias (proves no narrow-facade detour and no double work).
- **#129 perf, head-to-head** — the *old* two-alias workaround (one `Embed` + one `EmbedHybrid` over
  identical weights) vs the *new* single `EmbedHybrid` alias, same data. Assert the workaround does
  **2** loads + **2** passes (distinct `ModelRuntimeKey` because `task` is in the key) while the fix
  does **1** + **1** — quantifying the redundancy the fix removes (and the work #132 no longer doubles).
- **#132** — `#[ignore]` time-bounded repro of the consolidation hang on the dense+sparse+multi
  schema; documents the lost-wakeup so it's tracked. Out of scope to fix (§7); kept red.

### 5.2 Model-loading & forward-pass accounting (explicit focus)

Each asserts `load_count`/`call_count` on a counting provider+double:

- **One alias, N heads → one load, one pass.** Hybrid alias serving dense+sparse+multi from one
  source: `load_count == 1`, `call_count == 1` per batch (not one pass per head).
- **Cross-alias dedup (same weights, same task).** Two *different* aliases with identical
  `(provider, model_id, revision, options, task)` → **one** load (`ModelRuntimeKey` dedup). Guards the
  invariant that aliasing is free when the task matches.
- **Task splits the key (regression characterization).** Same weights, tasks `Embed` vs `EmbedHybrid`
  → **two** loads. This is *why* the workaround is wasteful; the test pins the behavior so a future
  uni-xervo key change is noticed.
- **Facade-wrapper reuse.** Two writes on the same alias → `load_count` stays `1` (second write hits
  the cached wrapper). Two passes (`call_count == 2`) but one load.
- **Reopen does zero loads during validation.** Point an `EmbedHybrid` alias at a provider whose
  `load()` panics/sleeps; open + validate + reopen must **not** call `load()` (`load_count == 0`).
  Verified-cheap-validation invariant (§4.1) — protects open latency and fail-open behavior when a
  model is unavailable at open. First load happens only on the first auto-embed write.
- **Mixed topology (T3) pass accounting.** Hybrid(dense+sparse) + separate multi model →
  exactly **2** loads, **2** passes (one shared hybrid pass for two heads + one multi pass), not 3.
- **Shared vs distinct source.** Same hybrid alias, two columns, **same** `source_properties` → 1
  pass; **different** `source_properties` → 2 passes (different text in ⇒ different group). Documents
  the single-pass precondition.
- **Batch boundary.** Rows > `batch_size` → `call_count == ceil(rows/batch_size)` per `(alias,
  source)`, and reassembled vectors map to the right rows (no off-by-batch drift).

### 5.3 Open-time validation matrix (`embedding_alias_capability.rs`)

`{dense Vector, multi-vector Vector, Sparse}` × each `ModelTask`; accept iff
`required ⊆ text_embedding_heads(task)`. Drive it as a data-table test. Required cells:

| column | task | expect |
|--------|------|--------|
| dense | `Embed` | accept |
| dense | `EmbedHybrid` | accept (#129/#130 core) |
| dense | `EmbedSparse` / `EmbedMultiVector` | reject |
| dense | `EmbedImage`/`EmbedAudio`/`EmbedMultimodal` | **reject** (text-source modality, §4.1) |
| dense | `Rerank`/`Generate`/`Raw`/`Nlp` | reject |
| multi-vector | `EmbedMultiVector` | accept (multi face of #130) |
| multi-vector | `EmbedHybrid` | accept |
| multi-vector | `Embed`/`EmbedSparse` | reject |
| sparse | `EmbedSparse` | accept |
| sparse | `EmbedHybrid` | accept |
| sparse | `Embed` | **reject** — *new* (was unvalidated; verify it now rejects at open) |
| sparse | missing-from-catalog alias | **reject** at open — closes the unvalidated-sparse gap |

Plus:
- **Union per alias.** dense+sparse columns sharing one `EmbedHybrid` alias → accept; sharing one
  `Embed` alias → reject (union `{DENSE,SPARSE} ⊄ {DENSE}`). This is the **mixed-on-narrow** guard
  that makes write-time `facade = f(task)` total (§4.2).
- **Missing alias entirely** → existing "missing Uni-Xervo alias" error preserved (don't regress it).
- **Error-message quality** — assert the rejection names the alias, its task, the required head(s),
  and the offending column. Users debug from this string; pin it.
- **`#[non_exhaustive]` safety** — a unit test over `text_embedding_heads` confirming the `matches!`
  allow-list returns `HeadSet::empty()` for non-embedding tasks (so a future variant rejects, not
  silently passes).

### 5.4 Topology end-to-end with reopen (T1–T4)

For **each** topology: create → ingest → **drop** → **reopen** → read back → ingest more → reopen
again. The reopen cycles are the point — they catch the create/reopen asymmetry that defines #130.

- **T1** three separate single-task models — each column embedded by its own narrow facade;
  `load_count == 1` per alias.
- **T2** one `EmbedHybrid` for all three, shared source — one load, one pass/batch, all three columns
  populated; survives reopen (the headline #130 fix).
- **T3** hybrid(dense+sparse) + separate `EmbedMultiVector` — two groups, correct columns, 2 loads;
  reopen-clean.
- **T4** hybrid for a single dense head — one load/pass via `hybrid_embedder`, not the narrow path
  (the #129 fix), reopen-clean.

### 5.5 Partial-hybrid / `available_heads` silent-loss (§4.3 — correctness)

Using a `CountingHybrid` whose `available_heads()` is a **strict subset** of what the columns need:

- **≥2-head path** — `available_heads() == DENSE|SPARSE`, group needs `DENSE|SPARSE|MULTI_VECTOR`:
  assert a **hard error** naming the missing `MULTI_VECTOR` head + alias + column. *Without the fix the
  multi column is silently empty* — the test must assert it is **not** silently written as
  empty/absent (inspect the persisted column).
- **1-head path (new exposure from §4.2)** — `available_heads() == SPARSE`, lone **dense** column
  routed to the hybrid: assert hard error (required `{DENSE} ⊄ {SPARSE}`), not a silent `None`.
- **Happy subset** — `available_heads() == DENSE|SPARSE`, group needs exactly `DENSE|SPARSE`:
  succeeds, one pass, both columns written, multi column untouched. (Confirms the check rejects only
  genuinely-missing heads.)
- **Pre-check vs post-check** — verify the error fires from the `available_heads()` pre-check
  (before `embed`, so `call_count == 0` on the failing write), and a defensive post-`embed`
  `None`-on-requested-head assertion also trips if the pre-check is bypassed.
- **Eager variant (open q4)** — *only if* eager-warmup validation is added (it does not exist in the
  open path today): partial-hybrid + `WarmupPolicy::Eager` + `required` → fails at startup.

### 5.6 Write-correctness / data-integrity adjacencies

The routing change rewrites the head-splitting path; guard the invariants around it:

- **Head→column wiring.** Hybrid result's dense/sparse/multi land in the *correct* columns (a
  cross-wire would pass shape checks but corrupt data). Assert per-head values against the double's
  deterministic output.
- **User-supplied value preserved.** Column with an explicit value + auto-embed config → user value
  kept (`contains_key`/`user_touched` guard). Existing `*_autoembed.rs` "explicit-not-overwritten"
  tests cover narrow facades; add the hybrid-routed case.
- **Partial / missing source rows.** Some rows lack the source text (null/non-string): those rows
  skip embedding, others embed, and output vectors stay aligned to their rows (classic off-by-one
  risk when filtering inputs before a batch `embed`).
- **Empty input batch.** A batch with no embeddable rows → uni-xervo returns `Some(empty)` per
  requested head; assert no panic, no spurious "missing head" error.
- **Multi-property + ordering.** `source_properties = [title, body]` assembles stable text, and the
  grouping key treats `[title,body]` distinctly from `[body,title]` (different group ⇒ may differ).

### 5.7 Persistence & backward-compatibility

- **Existing two-alias workaround schema still opens.** A schema authored with the old workaround
  (valid `Embed` + valid `EmbedHybrid` aliases) must still reopen and ingest on the new code — the fix
  must not invalidate KBs people built to dodge #129.
- **Alias dropped from catalog before reopen** → clear "missing alias" error at open (not a write-time
  panic).
- **Multi-cycle reopen** under deferred-embeddings config (`UniConfig.defer_embeddings`) — embeddings
  materialize on the consolidation/flush sweep and survive repeated open/close.

### 5.8 Adjacent known-gap guards (tracked, not fixed here)

- **Query-time shape gap (§4.5)** — `#[ignore]`/xfail repro: `similar_to(text, sparse_property)` and
  `similar_to(text, multivector_property)` currently embed dense (wrong shape) via
  `auto_embed_query`. A red-but-tracked test prevents the gap from drifting silently and flips green
  when the follow-up lands.
- **#132** — see §5.1 (kept `#[ignore]`).

## 6. Risks / compatibility

- **More permissive validation (mostly).** Schemas that previously failed at reopen now succeed; no
  previously-valid schema becomes invalid. Two *new* open-time rejections, both for configs that
  never worked (they failed at write before) and so fail earlier and clearer: (a) a sparse alias whose
  task can't produce a sparse head; (b) a dense column on an image/audio/multimodal alias. Call both
  out in release notes.
- **New write/load-time hard error for partial-hybrid (§4.3).** Previously this silently produced an
  empty column (data-quality bug); it now errors. This is a behavior change worth a release note, but
  silent-empty embeddings are never desirable.
- **Allow-list vs `#[non_exhaustive]`.** `matches!` keeps us safe against future task variants.
- **Plumbing (option A).** `alias → ModelTask` flows from `UniBuilder` to the writer; uni-store
  already depends on uni-xervo so naming `ModelTask` is free. No new crate dependency.
- **Upstream helper.** §4.4 adds a small method to uni-xervo (a crate we own); coordinate the version
  bump.

## 7. Out of scope (separately tracked)

- **#132 hang** — strongest lead is a per-path **`postings_write_guard`** held across a Lance
  `apply_incremental_updates` consumption in the incremental sparse/inverted index update
  (`crates/uni-store/src/storage/index_manager.rs:987-990`, mirrored ~900-904 — verified: guard
  acquired then held across the `.await`). A fully-parked runtime with a stream "in flight" is a
  lost-wakeup signature, not a lock-contention deadlock. (Note: this is the *postings* path guard, not
  the table-level `lock_table_for_write` at ~449, which is the #96 FDE-splice path.) Needs a minimal
  repro + check for re-entrant acquisition of the same postings path. This proposal *reduces* the
  aggravating redundant work but does not fix the hang.
- **Query-time routing** (§4.5).

## 8. Open questions (resolved at implementation)

1. Write-routing mechanism: **Option B** (try-narrow-then-hybrid-fallback) — reversed from rev. 2's
   Option A because the prebuilt-runtime path gives uni no catalog to read tasks from (§4.2).
2. Helper home: **uni-store** (`runtime/embed_caps.rs`), not upstream uni-xervo — avoids a publish
   cycle for a registry dependency (§4.4). Upstreaming remains a clean future refactor.
3. Unvalidated-sparse-alias case: **hard error at open** (implemented) — the config never worked.
4. Partial-hybrid check (§4.3): **lazy** — the existing post-`embed` `ok_or_else` guard already gives
   a hard error on the new fallback path; no eager `available_heads()` warmup check was added.
