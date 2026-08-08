# uni-db 3.3.0

**Release focus: making wrong answers impossible to ship quietly.** Every headline item in this
release is a defect that returned plausible data with no error — a Locy `IS NOT` that silently
matched every row, a recursive `FOLD` that under-counted, a full-text search that ranked its
*worst* match first. The pattern behind them is the same, and so is the fix: find the surface that
had no compiler between the claim and the truth, and give it one.

Three of those surfaces got one in this release. Locy's two evaluators became one, so `QUERY` and
`.derived` now agree *by construction* rather than by a parity check. Retrieval scores became
monotone in relevance, so `ORDER BY score DESC` means what it says. And the documentation — ~190
files of it — got an actual parser: every documented Cypher and Locy snippet is now parsed against
the real pest grammar on every run, including snippets embedded in Rust and Python string literals,
where a mistake is a *runtime* failure rather than a rendering one.

Two more themes join them. A registered Locy rule used to execute in **every** program, so one
rule taking a parameter made that parameter mandatory for unrelated queries — work done eagerly
that should have been demand-driven. And a Xervo model runtime could not be shared from Python,
so N databases meant N resident copies of the same weights, with no workaround available in the
binding.

This release covers everything since **3.2.0**: **30 commits**, 263 files changed,
+22,836 / −15,377. The version was bumped to **3.3.0** at `d8f6f3abc` but never tagged, so
everything after that bump is folded in here.

The full CI matrix was replicated locally at `639eeab24`: the **6,268-test workspace suite** green,
plus `fmt`, `clippy -D warnings` and `rustdoc -D warnings`. openCypher TCK **3925/3925** in both
schemaless and sidecar modes; Locy TCK **519/519** in both. Python: `uni-pydantic` **269 passed**,
pyo3 loader **35**, WASM/Extism lane **39**. Reranker ONNX **29** bundled + **26** load-dynamic,
LocalStack cloud **18**, loom **4**, metamorphic oracles **12**, and all six flagship notebooks
executed against a built wheel.

The seven commits after that point were gated on the suites covering their blast radius rather than
the whole matrix: `uni-db` **2,408 integration tests**, `uni-locy` + `uni-query` **926**, Locy TCK
**519/519** in both lanes, Python `uni-db` **934 passed**, the documentation-snippet harness, and
`fmt` + `clippy -D warnings` on the touched crates. Re-run the full matrix before tagging.

---

## ⚠️ Breaking changes

Five, each with a `BREAKING CHANGE` footer on its own commit. All five are corrections to results
that were previously wrong — so "breaking" here means *your output changes to the right answer*,
not that an API moved.

### Locy — an unresolvable `IS NOT` subject errors instead of matching everything

`9d3ac7c16`. An `IS NOT` whose subject could not be resolved silently degraded to "no rows
excluded", so the rule returned **every** row and looked like it had simply found a lot of matches.
It now fails loudly. Closes **#158**, **#159**, **#160**.

**Migration:** a program that previously returned a suspiciously large result set will now error.
The error names the unresolvable subject; bind it, or drop the `IS NOT`.

### Locy — a non-node `IS NOT` subject is rejected at compile time

`baed1e98f`. `IsNotSubjectNotANode` is raised during compilation rather than producing undefined
behaviour at evaluation.

**Migration:** the subject of an `IS NOT` must be a node variable. Restructure the rule if you were
passing a scalar.

### Locy — recursive `FOLD` aggregates across derivations, not distinct values

`fbe60af91` and `43577f034`. A recursive `FOLD`/`ALONG` collapsed derivations that carried equal
values, under-counting the aggregate. The second commit completes the fix: a child's *value* is
folded, not its derivations.

**Migration:** programs whose derivations carried equal values now return **larger** sums, counts
and probabilities. These are the correct figures; re-baseline any fixture that captured the old
ones.

### Retrieval — scores and thresholds are monotone in relevance

`ed965c716`. Three defects in one area, all silent:

* `uni.fts.query` pushed BM25 through the L2 arm of `calculate_score`, producing `1/(1+bm25)` —
  strictly *decreasing* in relevance. The documented `ORDER BY score DESC` therefore returned the
  **worst** matches first. A regression test reports `[("low", 0.693), ("high", 0.571)]` against
  the old code: the weaker match ranked first, with the higher score.
* `threshold` on `uni.fts.query` filtered the raw BM25 value while the caller saw the transformed
  score — a different range *and* direction.
* `threshold` on `uni.vector.query` meant *minimum similarity* on the multi-vector branch and
  *maximum distance* on the dense branch **of the same procedure**. One argument meant opposite
  things depending on which branch a query took, so moving a property to a `List<Vector>` column
  silently inverted the filter.

A new `RetrievalScore` type makes the mapping explicit — `Distance(metric)` inverts, `Bm25 { k }`
saturates — and `threshold` is now a minimum on the yielded `score` scale everywhere.
`options.fts_k` tunes BM25 saturation, matching `similar_to`, which had always used the correct
transform.

**Migration:** a query passing `threshold` as a distance bound must be inverted — it is now a
similarity floor, so a *higher* value is more restrictive. Any stored ordering of `uni.fts.query`
results flips. The five Python tests that encoded the old meaning are updated in `0597a7a0e` and
show the shape of the change.

---

## Highlights

### 🔹 Locy — one evaluator instead of two

`2f3c0802f`. `QUERY` was answered by an independent top-down SLG re-derivation with weaker
recursion and stratification than the semi-naive fixpoint that produced `.derived`. Two evaluators
of unequal power was the root cause of **#160**, and the parity guard added earlier existed only to
police the gap.

`QUERY` now reads `derived_store` — the same rows `.derived` exposes — so the two agree by
construction, which is a stronger guarantee than any parity check. SLG survives for exactly one
case: rules containing a generator, which the columnar fixpoint has no row-explosion operator for.

Note this inverts the old performance guidance. `QUERY` is a projection over already-materialised
output, not a cheaper path that avoids materialising it.

### 🔹 Locy — a registered rule runs only when a program references it

`1071ce7e9`, with `4303b8e8a` extracting the name predicate it needs. Closes **#157**.

`merge_registered_rules` prepended **every** registered stratum to every compiled program. A
`Stratum` owns its rules and the runtime evaluates the vector end to end, so every registered rule
*executed* in every program. One rule taking a parameter therefore made that parameter mandatory
everywhere — including in programs that name no rule at all:

```
LocyRuntimeError: Execution error: Sub-plan error: Unresolved parameter: $needed
```

The reporter's mitigation — bind the union of every registered rule's parameters at every call
site — does not compose: two independently authored rules each break the other's queries. In
practice, registering one parameterized rule cost you ad-hoc Locy for the life of the database.

The registry is now filtered to the strata a program can transitively reach, seeded from every
construct that names a rule: `QUERY`, `DERIVE`, `EXPLAIN RULE`, `ABDUCE`, `VALIDATE`, `IS` and
`IS NOT` references, an `ASSUME` body, and a model's `path_context`. Two properties make pruning
safe rather than merely smaller: unknown names already fail at *compile* time against the
registry, so pruning cannot turn a missing rule into a silently empty result; and a retained
stratum is expanded whole, since its rules form one SCC and a co-member's own references have to
resolve too.

Negated references seed retention exactly like positive ones. An anti-join against a dropped
relation admits every row, which is the one way this change could have produced *more* results.

**Behaviour change:** `LocyResult.derived` no longer carries registered rules the program never
referenced, and `profile()` lists only the retained strata. Both were previously polluted by
unrelated registered rules. A program that wants a rule's facts names it; there is no opt-out flag.

### 🔹 Locy — `QUERY` resolves a rule name module-aware

`1593ab701`. Inside `MODULE m`, `QUERY adult` names the rule bare while the catalog and the derived
store key it `m.adult`. The compiler validated the reference module-aware, so the program compiled
and then failed at run time with `rule 'adult' not found` — a plain lookup missed, fell through to
the SLG path, and that path reported the rule as absent even though the fixpoint had derived it.

Both the catalog and the store lookup now resolve through the shared name policy. Ambiguity is
refused rather than guessed: a bare name matching two modules errors instead of silently picking
one. That refusal is a known limitation, not the target behaviour — the compiler *can* resolve it
from the enclosing `MODULE`, but `GoalQuery::rule_name` keeps the bare spelling, so the runtime has
only suffix matching to work with. Threading the compiler-resolved name through to `evaluate_query`
is tracked separately.

Found while writing the coverage the #157 fix depended on: `DERIVE`, `EXPLAIN RULE` and `ABDUCE`
against a *registered* rule, `MODULE` resolution, and the `PreparedLocy` call site had no
end-to-end tests at all. `c0c76aa7b` strengthens two of them — they asserted only `is_ok()`, so a
resolution to an empty relation would have passed.

### 🔹 Xervo — one model runtime, many databases

`ea6cc8fed`, with `1028fae86` extracting the machinery. Closes **#155**.

A `ModelRuntime` owns its cache of loaded models, so each database built from a catalog held its
own copy of the weights. Python had neither half of the Rust round trip — no `raw_runtime()`
accessor, no `xervo_runtime()` setter — so N databases on the same catalog meant N resident copies
with no workaround available in the binding. Measured with `all-MiniLM-L6-v2` (22M parameters, the
*smallest* useful embedder), four databases in one process:

| | first instance | each additional |
|---|---|---|
| `xervo_catalog_from_str` | 205 MiB | **+107 MiB** |
| `xervo_runtime` | 194 MiB | **+5 MiB** |

526 MiB total drops to 210 MiB. The residual 5 MiB is the database itself; the model is no longer
duplicated. The saving scales with the model, so a reranker or a generator turns this from wasteful
into OOM-avoiding at small N.

```python
runtime = uni_db.ModelRuntime.from_catalog_str(catalog_json)
a = uni_db.UniBuilder.open("./kb/a").xervo_runtime(runtime).build()
b = uni_db.UniBuilder.open("./kb/b").xervo_runtime(runtime).build()
```

The handle is opaque and compares by pointer identity, so "these databases really do share a
runtime" is directly assertable. One handle type serves both the sync and async builders — the
object is an inert `Arc` with no sync/async character — with `_async` constructors for callers that
must not block an event loop, which matters when the catalog warms eagerly. The three Xervo sources
are mutually exclusive: each setter clears the other two, rather than exposing the Rust builder's
silent runtime-beats-catalog precedence.

Underneath, the 11-provider `#[cfg]` chain moved out of `UniBuilder::build` into
`uni_db::xervo::build_model_runtime`. Being inline was the reason *Rust* had no standalone runtime
constructor either; it now has exactly one definition, so the enabled-provider set cannot drift
between a runtime a database builds for itself and one built to be shared.

### 🔹 Xervo — a prebuilt runtime is checked against the schema

`79f335170`. Opening a database whose schema binds a vector index to an embedding alias validated
that alias — but the check sat inside the catalog branch, so injecting a runtime via
`xervo_runtime()` skipped it and the runtime was accepted unexamined. A runtime whose catalog
lacked the alias opened fine and failed much later, as an `AliasNotFound` from inside the writer on
the first auto-embedded write. Pre-existing, and made to matter more by the API above.

Alias *presence* is now verified on both paths, with the same message. The per-alias
head-capability check still runs only on the catalog path: it needs `spec.task`, and uni-xervo
keeps `lookup_spec` private, so a task mismatch on a shared runtime still surfaces at first
inference. Documented at the check site and in the black book; closing it needs a public catalog
accessor upstream.

### 🔹 A parser for the documentation

`7bfff4abb`. Two tests in the `uni-cypher` integration binary:

* every fenced ` ```cypher ` / ` ```locy ` block across `website/docs`, `docs` and `skills` must
  parse against the real grammar — **511 snippets**;
* every Cypher string literal embedded in a Rust or Python doc block must parse — **385 literals**.
  These are the dangerous ones: the doc block renders fine and the query fails at `execute()`.

Both are green. Getting there fixed ~250 non-parsing snippets, including 22 embedded literals that
were live runtime failures. Three opt-outs exist for notation, fragments and deliberate
counter-examples, all visible in the doc source; the preferred one is an HTML comment that leaves
rendering untouched.

### 🔹 A generated Python API reference

`0d591ca20`. The tree carried **two** hand-maintained references for one surface — ~10k lines
between them, neither generated. They drifted independently, which is how each ended up documenting
methods the other lacked *and that never existed* (`xervo.rerank` in the published set,
`session.profile_with` in the other).

`reference/python-api-symbols.md` is now generated from `bindings/uni-db/uni_db/__init__.pyi` —
190 classes, the full surface, versus the 76% / 87% the hand-written pages reached — and CI fails
if the checked-in copy is stale. The three `docs/complete_*.md` duplicates are retired to stubs,
6368 lines to 52.

Four new gates ship with it, each verified to fail against the defect that motivated it: documented
Python methods must exist in the stubs; documented counts must match
`assert_eq!(kinds.len(), 22)`, the fact the code already asserted and the prose was never wired to;
`uni_pydantic.__version__` is derived from package metadata rather than hand-written (it had
drifted to `2.5.0` against a `3.3.0` package); and the generated reference must be current.

### 🔹 Plugin artifact hash-pinning

`bf4d50bad`. `verify_hash_pin` had **zero call sites** while the trust module claimed it "is
applied separately at load sites". Wiring it in as written would not have helped: it reads a digest
from the artifact's own manifest, which is self-certifying — anyone who can rewrite the payload can
rewrite the digest beside it.

`PluginTrustConfig::pinned_artifacts` adds the externally-supplied form. Empty by default; when
populated, every loader entry point that receives payload bytes — WASM component, Extism, Rhai,
PyO3 — rejects a payload whose Blake3 digest is absent, before instantiation and before any
capability is granted.

### 🔹 `SHOW CONSTRAINTS ON (...)` no longer panics

`db1826325`. The walker consumed an inner pair for the `(`, but parentheses in `constraint_target`
are bare string literals that pest never surfaces — so it ate the identifier and the next
`.unwrap()` hit `None`. Grammatically valid input unwound the process instead of returning an
error. Found while documenting the correct DDL syntax; nothing had covered it.

### 🔹 Query engine — `LargeUtf8` and four silent degradations

`c7ec71ef8`. `LargeUtf8` columns now decode, and four paths that had been failing open now fail
closed. Plus `31d069108`: schemaless `YIELD` property columns infer as `LargeBinary`, and
`fe7048569`: `IS NOT` subjects resolve by identity so IS-refs bind correctly.

---

## Documentation

`f39ae75c7` is a ~190-file correction sweep, every finding verified against source rather than by
prose review. Beyond the snippet fixes above:

* **Fabrications removed.** The query-planning chapter documented `QueryOptimizer`,
  `OptimizationRule`, `CostEstimator`, a ten-variant `PhysicalPlan` and eight more types that exist
  nowhere — optimization is delegated to DataFusion. `identity.md` described VID/EID bit-packing
  with accessors and a two-argument `Vid::new`, when VIDs are pure auto-increment and `Vid::new`
  takes one `u64`. An 18-function `crdt.*` table, none registered. "Parser built on sqlparser" in
  four places; it is pest. `python-api.md` taught Datalog `head(x,y) :- body.` syntax Locy does not
  have.
* **Front doors.** `bindings/uni-db/README.md` — the `pip install uni-db` landing page — was still
  on the removed 1.x API. `getting-started/quickstart.md` failed on the first thing anyone runs.
* **Cross-language confusion.** `hybrid()` / `cloud_config()` were documented on the *Rust* builder
  in five files; they exist only in the Python bindings, where the Rust equivalent is
  `remote_storage(url, config)`.
* **The fork API** was added to the published Python reference, where it had **zero** mentions
  despite being a headline feature, and `ssi_enabled` is documented (SSI is default-on and it
  appeared nowhere).
* Status headers on six dated review reports still leading with fixed CRITICAL findings, and
  provenance on unattributed benchmark tables — including a demo spec citing a 2.3M-paper corpus
  its own generator sets to 5,000.

`ea6cc8fed` adds a **Sharing models across databases** section to the Python API reference, with the
measured figures above and a note on what the shared-runtime path does and does not validate. The
generated `python-api-symbols.md` grows to **191 classes**.

`97d2a0b05` restores `docs/migrations/0.9.0-wheel-matrix-collapse.md`, linked from all five variant
wheel READMEs and rendered on their PyPI pages. It was added by the commit that performed the
wheel collapse and deleted as collateral by an unrelated cleanup, so six links had been dead. Notes
for 2.4.0, 3.0.1 and 3.1.0 — released without any — are backfilled from their commit ranges.

---

## Housekeeping

* `225d0502d` — dead code removed, oversized modules split.
* `b44dfaeb0` — the 3.2.0 release notes, written after that release shipped.
* `639eeab24` — `uni-tck`'s integration binaries consolidated from four to two, restoring the
  documented 3-binary cap.
* `f4474c819` — the local CI runbook records the four new release-guard checks, and the fact that
  `maturin build --release` yields a wheel ~1.4× the published size (115.8 vs 83.6 MiB) because the
  shipping `dist` profile uses `codegen-units = 1`. Profiles live in `.cargo/config.toml`, not
  `Cargo.toml`.
