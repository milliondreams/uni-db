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

This release covers everything since **3.2.0**: **21 commits**, 249 files changed,
+20,790 / −15,308. The version was bumped to **3.3.0** at `d8f6f3abc` but never tagged, so
everything after that bump is folded in here.

Gates at the release commit — the full CI matrix, replicated locally: the **6,268-test workspace
suite** green, plus `fmt`, `clippy -D warnings` and `rustdoc -D warnings`. openCypher TCK
**3925/3925** in both schemaless and sidecar modes; Locy TCK **519/519** in both. Python:
`uni-db` **923 passed**, `uni-pydantic` **269 passed**, pyo3 loader **35**, WASM/Extism lane **39**.
Reranker ONNX **29** bundled + **26** load-dynamic, LocalStack cloud **18**, loom **4**,
metamorphic oracles **12**, and all six flagship notebooks executed against a built wheel.

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
