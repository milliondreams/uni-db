# Finding silent wrong answers by class, not by customer report

**Date:** 2026-09-28 · **Status:** W1 done (all confirmed defects fixed, plus four found on the way); W2–W6 open · **Trigger:** issues #293, #294

## Why

Four silent-wrong-answer bugs were fixed on 2026-09-28 (`d8964d1a4`,
`b863c7b79`, `c4d24b15f`, `0993d4f68`). Each one was cheap to catch *after*
the fact with an oracle that needs no reference implementation — the engine
compared with itself under a transformation that must not change the answer:

| Bug | Class | Oracle that catches it | Input shape it needs |
|---|---|---|---|
| #294 recursive FOLD kept one of two parallel edges | identity key omitted a component | recursive form ≡ non-recursive form | parallel edges, diamonds |
| anonymous VLP returned one row per endpoint | optimization with an unchecked precondition | named relationship ≡ anonymous relationship | ≥ 2 equal-length paths to one endpoint |
| chunked scan dropped unflushed rows past a vid gap | decision made from a partial view (L1 only) | before flush ≡ after flush | > 8192 rows of a label, a vid gap, then L0 rows |
| seeded FOLD type mismatch | — (loud) | recursive ≡ non-recursive | seed + fold clauses with differing literal types |

None of the existing harnesses could have caught any of them, for structural
reasons (measured, see §4): the DQP fixture is a bipartite one-hop graph with
no parallel edges, diamonds or cycles; its flush lever runs only on a
1000-row tier smaller than one scan slice; `querygen` never emits
variable-length paths, named relationships, `OPTIONAL MATCH`, `DISTINCT`,
`EXISTS` or `UNWIND`; no generated Locy program ever passes through a lever;
and the naive Locy oracle's generator has three fixed templates. The tests
were written by people who know the idioms, over data shaped the way they
expected.

## 1. The three classes

1. **Identity** — a key built for dedup / merge / group / "seen" omits a
   component, so two distinct things collapse, often keep-last and therefore
   nondeterministically.
2. **Partial view** — a read-path decision consults one of {L1, L0 current,
   L0 transaction, L0 pending-flush, fork branch, pinned snapshot} when the
   answer depends on their union.
3. **Unchecked precondition** — a rewrite or fast path is valid only if the
   consumer cannot observe something (multiplicity, a constraint, a NULL row),
   and that is assumed rather than checked.

## 2. Audit results (2026-09-28)

Four read-only audits enumerated candidates per class; the top suspects were
then probed differentially (`crates/uni/tests/common/bugs/class_audit_probes.rs`,
each probe = suspect query vs a differently-formulated control). **Confirmed**
means measured; everything else is a hypothesis.

| # | Class | Confirmed defect | Control that exposed it |
|---|---|---|---|
| 1 | precondition | Vectorized pattern predicate / pattern comprehension drops the anchor label, relationship property maps and relationship uniqueness (`WHERE (n:Person)-[:R]->()` also matches a Robot) | same pattern inside `EXISTS { MATCH … }` / `COUNT { MATCH … }` |
| 2 | identity | `OptionalFilterExec` groups by VID only: `UNWIND [2,3] AS x MATCH (a) OPTIONAL MATCH (a)-->(b) WHERE b.v = x` drops the `(3, null)` row | expected bag |
| 3 | identity | Locy: facts of a multi-rule stratum are stored under each rule's name, so `QUERY even` over mutually recursive `even`/`odd` returns odd's rows too | expected bag |
| 4 | identity | Locy fixpoint rounds every Float64 fact to 1e-12: an ALONG product of 1e-14 becomes 0.0 | expected value |
| 5 | partial view | FTS returns a stale L1 hit after an unflushed `SET` | expected bag |
| 6 | partial view | Vertex UNIQUE rejects re-creating a key after an unflushed delete | Cypher semantics |
| 7 | partial view | Vertex UNIQUE rejects reusing a key after a flushed change (append-mode rows, no MVCC dedup in the probe) | Cypher semantics |
| 8 | partial view | Non-DETACH `DELETE` of a node with an edge created in the same transaction succeeds | Cypher semantics |
| 9 | partial view | Adding a NOT NULL property to an all-L0 label is accepted, then **every flush fails** | flush |
| 10 | (loud) | `EXISTS { MATCH (n:Label)-… }` with `n` bound fails: `No field named "n._labels"` | — |
| 11 | (loud) | Unlabelled `MATCH (n {ext_id:'x'}) RETURN n.name` fails with a schema error | `count(n)` works |

Found and fixed during W1, beyond the table: label disjunction on a traversal
target matched nothing; inline element `WHERE` was ignored everywhere; vector
search had the FTS stale-version defect; a vertex `UNIQUE` key moved by an
unflushed `SET` stayed taken. The invariants are recorded in the Black Book,
Appendix B2.

Not reproduced on the probe graphs (weak evidence only): the reachability
BFS first-predecessor trail check, `WHERE` pushdown through a shadowing
`WITH`, `count(DISTINCT m)` over maps carrying `_id`. Remaining unprobed
candidates are listed in the audit notes and should be probed before they are
dismissed.

## 3. The harness: metamorphic relations the engine must satisfy

Each relation is an oracle that needs no reference implementation. Every one
is justified by a bug it would have caught.

| Relation | Catches | Status |
|---|---|---|
| named relationship ≡ anonymous relationship | anonymous VLP multiplicity | hand test exists (`vlp_anonymous_path_multiplicity`) |
| pattern predicate ≡ `EXISTS { MATCH … }`; pattern comprehension size ≡ `COUNT { MATCH … }` | #1 | new |
| recursive rule ≡ non-recursive rule on acyclic data of depth ≤ k (unroll) | #294, seed typing | hand tests exist |
| Locy non-recursive rule ≡ equivalent Cypher `MATCH … RETURN key, agg(...)` | FOLD grouping, #293 | new |
| before flush ≡ after flush, **on fixtures with vid gaps and > 1 slice per label** | scan L0 gap, FTS stale, UNIQUE | lever exists; fixture does not |
| inside transaction ≡ after commit | non-DETACH delete | new |
| `target_partitions` ∈ {1, 2, 8}, batch size ∈ {1, 8192}, fresh session × N | #294's nondeterminism, keep-last merges | **blocked**: config not wired (§4) |
| adding a parallel edge with value v changes MSUM by exactly v; splitting a node does not change totals | identity keys | new |

## 4. Work items, in order

**W1 — fix the confirmed defects** (§2), each with its probe promoted to a
regression test that fails on the old code. Suggested order by blast radius:
9 (flush wedge), 8, 2, 1, 6/7, 5, 3, 4, then 10/11.

**W2 — make batch size and partition count settable.** `UniConfig.batch_size`
reaches only the cursor (`impl_query.rs:797`); `UniConfig.parallelism` has no
reader. Sessions build `SessionConfig::new()` at `api/mod.rs:2161` and
`executor/read.rs:548/552/557`. Wire both through, then add a Tier-3 lever and
a determinism check that runs the Locy TCK and the Cypher TCK under
`target_partitions ∈ {1, 8}` and `batch_size ∈ {1, 8192}`. This reuses ~4450
existing hand-written expectations.

**W3 — widen the DQP fixture and generator.**
- Fixture: add a second relationship type over one label with parallel edges,
  diamonds, self-loops and a cycle; add a label with > 8192 rows and an
  interleaved second label so vids have gaps; keep a slice-sized L0 delta.
- `querygen`: emit variable-length relationships (named and anonymous),
  `OPTIONAL MATCH`, `DISTINCT`, `EXISTS { }` and pattern predicates, `UNWIND`
  over a literal list, and min/max/avg/collect.
- New Tier-2 levers that rewrite the *query* (the driver currently passes the
  same query to both sides, `driver.rs:390`): named↔anonymous,
  predicate↔`EXISTS`, comprehension↔`COUNT`.

**W4 — Locy through the levers.** A program-text case type and generator
(FOLD with composite keys, seeds, recursion, ALONG, parallel edges), an
`observe_locy` on `session.locy()`, a bag over derived facts, and the
recursion-unrolling and Locy↔Cypher relations. Verify first that
`LocyResult::metrics()` counters actually move, or the activation floor is
vacuous.

**W5 — extend the naive Locy oracle** to FOLD and monotonic aggregates over
**multisets** (so duplicate contributions are not collapsed), with a random
program generator in place of the three templates.

**W6 — fail loud by default.** Each class member found so far was silent.
Debug-build invariant checks at merge/dedup sites (as `merge_fold_contributions`
now has), and compile-time rejection of programs that would otherwise be
silently degraded (as #293 now does), turn the next member into a failing test
instead of a wrong answer.

## 5. What would have caught today's bugs earliest

W2's determinism check (for #294) and W3's fixture with parallel edges and vid
gaps (for #294, the VLP bug and the scan gap) are the highest-yield items: both
reuse existing expectations or existing levers, and neither needs a reference
implementation.
