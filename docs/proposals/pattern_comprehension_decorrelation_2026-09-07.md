# Pattern-comprehension decorrelation: what landed, and why the rest is blocked

Written 2026-09-07, against #206. Records the two increments that shipped, the
design that was assessed and rejected, and the reason the remaining half is a
scoping problem rather than an implementation one.

## What shipped

Two commits, **local to `main` and not yet on `origin/main`** — so nothing here
is a claim about merged code.

1. **The uncorrelated hoist.** A comprehension whose body reads nothing from
   the outer row is evaluated once and its encoded values reused for every
   subsequent row. `free_variables` decides it; the cache holds encoded values
   rather than a finished Arrow list so the offsets are built by the same code
   that builds them per row.
2. **A tighter `free_variables`.** A binding form (`reduce`, a list
   comprehension, a quantifier) names a variable of its own, and counting that
   name as a reference to the outer row made every body containing one decline.
   Subtracting the bindings fixes a false negative without widening what is
   treated as safe.

Alongside them, `subquery_executions` on `QueryMetrics`: the defect's shape is a
*count* — N executions where one would do — and a count is the honest instrument
for it.

Measured on the issue's own probe, release, uncorrelated shape:

| N | before | after | ratio before | ratio after |
|---:|---:|---:|---:|---:|
| 25 | 12.7 ms | 3.3 ms | 2.2x | 1.0x |
| 50 | 17.5 ms | 3.8 ms | 4.4x | 1.2x |
| 100 | 41.5 ms | 3.9 ms | 4.4x | 1.2x |

## Two corrections to the issue as filed

**The correlated ratio cannot be flattened, and not because of a defect.** The
probe compares `size([(n)-[:KNOWS]->(b) | 1])`, which yields O(1) elements per
row, against `size([(a:P)-[:KNOWS]->(b:P) WHERE a.idx > n.idx | 1])`, which
yields O(N). The second produces quadratic output. The ratio between them must
grow with N whatever the implementation does. "The probe's ratio column stops
growing with N" is achievable for the uncorrelated column — it now does — and is
not physically available for the correlated one. The real target there is the
constant factor.

**The uncorrelated shape is not pathological.** An earlier reading of a 900 s
timeout attributed it to the query; the timeout was dominated by the release
relink. Measured, the uncorrelated form was *faster* than the correlated one at
the same N (41.5 ms against 119.4 ms at N=100) before any of this work.

## The remaining half, and why it stops here

IC14's comprehension is correlated, so neither increment touches it. The
tractable design is to hoist the *pattern* — which is uncorrelated even when the
predicate is not — and apply the correlated predicate per row over a cached
result.

**The value-level route is rejected outright.** Evaluating the body per row with
`Executor::evaluate_expr` would require manufacturing an `Executor` on a read
path that has none. Every field it could not populate — `transaction_l0_override`,
`writer`, `l0_manager`, the session `config` — is a place where the comprehension
would read a different graph than the enclosing query; inside a transaction it
would miss the private L0 buffer. That is a silent wrong answer, not a crash. It
also hard-errors on a nested `PatternComprehension`, regressing a case the
current fallback handles.

**The vectorized route is sound and already half-written.**
`PatternComprehensionExec` replicates the outer row per expanded row, evaluates
a compiled predicate over the combined batch, filters, maps and rebuilds list
offsets. That is the decorrelation design, tested, fed by CSR expansion instead
of by a cached pattern result. `build_inner_schema` already synthesizes
outer-fields-plus-pattern-columns, and `entity_scoped_compiler` already registers
pattern variables so `startNode`/`endNode` resolve.

**What blocks it is memory, not correctness.** `expand_pattern` consumes an
anchor array aligned 1:1 with outer rows. Feeding it an unanchored pattern means
replicating the outer batch once per candidate vid and materializing
`|outer| x |pattern rows|` *before* filtering, then remapping the filtered
indices back through that replication to rebuild offsets. That is a new
unbounded per-query allocation in `df_graph` — Class 3, the class #214 and #242
describe. `PatternComprehensionExec` avoids it today only because an anchored
pattern's expansion is bounded by the anchor's degree; remove the anchor and the
bound goes with it.

Bounding it needs chunking over outer rows, which re-opens the same "how wide a
chunk" question that blocked #214 Phase 3 — and that was backed out because the
gate needs a cached statistic this system does not have.

## Recommendation

Scope decorrelation as its own effort with the memory bound designed in, and
sequence it behind whatever answers the cardinality question for #214/#224.
Doing it now means shipping a fresh unbounded materialization into the operator
layer while the issue describing that exact hazard is open.

Two things worth doing in the meantime, neither of them this:

- `UNI_HYPOTHESIS_PROFILE` appears in no workflow file, so the deep Hypothesis
  profile — `max_examples=500`, `stateful_step_count=50`, the one that found
  #181 — has never run in CI. One line in `nightly.yml`.
- #215 is verified unfixed and is the third instance of a widening shape whose
  two siblings are already fixed in the same file.
