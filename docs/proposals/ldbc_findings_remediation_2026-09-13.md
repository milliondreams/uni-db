# Remediation plan for the LDBC SNB SF1 readiness run — 2026-09-13

A fresh SF1 load on `main` (`caada7420`) first returned **0 of 14** Interactive
complex reads: the load was clean, but every query died before executing, in
parameter derivation. One derivation query was at fault. With it corrected
(0.2 below) the same store returns **11 of 14**, with IC3 and IC9 over the 300 s
budget and IC14 refused by the memory pool at 4.2 GB.

This orders what the run found. The 11/14 figures are the first Interactive
measurements taken on current `main`, and they are **upper bounds, not
LDBC-comparable latencies** — see 2.1.

> Scope note: this plan supersedes nothing in
> [`ldbc_findings_remediation_2026-08-27.md`](ldbc_findings_remediation_2026-08-27.md).
> That document's open items still stand. This one covers what the
> 2026-09-13 run added, and one item (#227) that both share.

## Ordering principle

Carried over unchanged from the 2026-08-27 plan, because it held up:

1. **Wrong answers before crashes before gaps before speed.**
2. **Anything that stops the system defending itself outranks optimization.**
3. Within a tier: cheap-and-unblocking first.

With one addition this run earned:

4. **An unexplained measurement is not a closed one.** Two figures below are
   reproducible and have no mechanism attached. They are listed as open
   questions with a first test, not as items with a fix.

---

## What the run actually did

Fresh load into `~/uni-bench-tmp/ldbcdb9`, release profile, 22 cores / 62 GB.
Store signature verified complete afterwards: 15 `adjacency_*_fwd.lance`,
14 `vertices_*.lance`, 0 WAL files — an exact match for the known-good
`ldbcdb8`.

| phase | rows | seconds |
|---|---:|---:|
| nodes | 3,181,724 | 79.9 |
| edges | 17,256,038 | 347.7 |
| flush | — | 0.0 |
| **total** | **20,437,762** | **442.2** |

`[ldbc] loaded 3181724 nodes and 17256038 edges, no unresolved endpoints`.

Then, before 0.2: `14 of 14 queries failed to execute`, every one with
`aborted the process — child exited with exit status: 101`. Every child died at
the same derivation line, so the supervisor paid a full restart to learn one
fact fourteen times (3.2).

After 0.2, same store, same binary:

| query | rows | ms | index scans | comparisons | scans reported | peak RSS (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| IC1 | 20 | 1152.2 | 15 | 458,220 | 18 | 2254 |
| IC2 | 20 | 49350.7 | 54 | 122,814,874 | 54 | 2254 |
| IC3 | — | — | — | — | — | 2254 | *over the 300 s budget* |
| IC4 | 10 | 8102.3 | 36 | 12,116,194 | 36 | 2807 |
| IC5 | 20 | 117348.2 | 315 | 21,959,580 | 703 | 10662 |
| IC6 | 10 | 9526.1 | 153 | 2,316,040 | 153 | 10926 |
| IC7 | 20 | 2876.0 | 6 | 5,832,008 | 10 | 10926 |
| IC8 | 20 | 1877.5 | 6 | 2,745,324 | 6 | 10926 |
| IC9 | — | — | — | — | — | 10926 | *over the 300 s budget* |
| IC10 | 10 | 6329.0 | 13 | 8,082,140 | 14 | 10926 |
| IC11 | 10 | 202.4 | 7 | 113,867 | 18 | 10926 |
| IC12 | 20 | 9562.2 | 89 | 1,230,661 | 112 | 10926 |
| IC13 | 1 | 32.4 | 7 | 56,648 | 8 | 10926 |
| IC14 | — | — | — | — | — | 14266 | *refused, 4.2 GB* |

Every query that completed returned rows, so the `VACUOUS` gate passed on all 11.

**IC5 answers.** 20 rows in 117.3 s, inside the budget. This is the query that
could not complete at all while #204 and #219 were open, and it is the clearest
single improvement visible in this run.

**IC14 reproduces a known figure exactly.** `7a5869d23`'s commit message records
IC14 refusing at "GraphTraverseExec at 4.2 GB"; this run reports 4.2 GB. The
pool refuses it upstream of the path enumeration, which is why it now fails in
seconds rather than being killed by hand after 111 minutes at 19.2 GB as the
2026-08-27 document records.

---

## Tier 0 — the blocker

### 0.1 An anonymous unlabelled source does not drive from the edge-type adjacency — **FIXED, uncommitted**

`MATCH ()-[r:TYPE]->()` scans every vertex in the graph and expands, instead of
driving from the typed edge's adjacency. Labelling the source produces an
identical answer for two orders of magnitude less cost.

Measured on `ldbcdb9`, same binary, same process, answer identical at 180,623:

| query | peak RSS | wall |
|---|---:|---:|
| baseline (`MATCH (p:Person) RETURN count(p)`) | 894 MB | 16.4 ms |
| `MATCH (:Person)-[w:KNOWS]->() RETURN count(w)` | 925 MB (+30 MB) | 104.5 ms |
| `MATCH ()-[w:KNOWS]->() RETURN count(w)` | **3,173 MB (+2,278 MB)** | **5,228.7 ms** |

**76x the memory, 50x the time, same answer.**

Standalone repro, no bench needed:

```bash
LDBC_DB=<sf1-store> PROBE_MAX_MB=8192 \
  ./target/release/examples/ldbc_probe <<< 'MATCH ()-[w:KNOWS]->() RETURN count(w) AS c'
```

**Why it took the benchmark down.** `params.rs` derived `workFromYear` with
`MATCH ()-[w:WORK_AT]->() RETURN max(w.workFrom)`. WORK_AT has 21,654 edges;
the traverse asked the memory pool for 662.7 MB and was refused against the
1 GiB default (`max_query_memory`, `crates/uni-common/src/config.rs:499`,
default at `:742`). Derivation runs before any query and every supervisor child
re-derives, so all 14 children died at the same line and the supervisor
recorded 14 aborts.

**This is not a regression from the accounting work.** Three separate checks:

- The derivation query shape dates to `4cc430faa`, the commit that introduced
  derived parameters.
- The same binary fails identically against `ldbcdb8`, so the fresh load is not
  implicated.
- `7a5869d23` does not touch `traverse.rs` at all, and `b2bfcbd49` only
  *reduced* over-counting there (`GraphTraverseMainExec`, not
  `GraphTraverseExec`).

The reservation that now refuses it was added by `718e033d9` (#242) at
`crates/uni-query/src/query/df_graph/traverse.rs:1687-1689`. That commit did not
break the benchmark — it made a long-standing unbilled 2.3 GB query visible.
The accounting is, if anything, **conservative**: it charges 699.5 MB for
something that actually holds 2,278 MB.

**Fixed by narrowing the scan, not by a new operator.** The first instinct was
an edge-driven scan, and the measurement killed it: `Scan(Person)` reads 9892
vertices where an edge scan would read 21654 edges, so the cheaper plan was
already expressible and the planner simply was not choosing it.

`plan_traverse_with_source` now narrows an unlabelled source to the edge type's
declared endpoint labels, reusing `replace_scan_all_with_label_union`. Direction
mirrors `AdjacencyManager`'s `labels_to_load` exactly — outgoing takes
`src_labels`, incoming `dst_labels`, undirected the union — which is also the
soundness argument: the CSR is loaded only for those labels and a typed traverse
reads only through the CSR, so an edge whose source lies outside the declared
set is *already* invisible. The narrowing cannot drop a row the traverse would
have produced.

Measured at SF1, both arms in one process against `ldbcdb9`:

| query | not narrowed | narrowed |
|---|---:|---:|
| `()-[:KNOWS]->()` | 5014.1 ms | **89.8 ms** |
| `()-[:HAS_MEMBER]->()` | 5520.1 ms | **780.6 ms** |
| `()-[:LIKES]->()` | **refused, 7.4 GB** | **1071.9 ms** |
| `()-[:WORK_AT]->()` max | 5201 ms | **150 ms** |
| `()-[:REPLY_OF]->()` | 5536 / 5503 ms | 5564 / 5479 ms |

LIKES could not be answered at all before. REPLY_OF is the neutral case and is
the point of the single-label rule: scan cost is linear at ~530 ms/Mrow with
the per-row cost alike on both tables, so narrowing to one label is at worst
neutral even when that label is 64.5% of the graph.

**Restricted to a single label, measured.** Two or more replaces one `ScanAll`
over the shared `vertices` table with a union of per-label scans whose rows
together can exceed it — 2.2x slower on IS_LOCATED_IN, 1.5x on HAS_TAG, 1.4x on
HAS_CREATOR. Those are every multi-source type at SF1 with enough rows to time
and each covers ~96% of the graph. A multi-label type with collectively small
labels could still win; none exists in this corpus, so it is unmeasured and not
enabled on a guess. See 0.3.

**Verified end to end, 2026-09-14.** `cargo nextest run -p uni-db --release
--tests`: **2972 passed, 0 failed**, 95 skipped. SF1 re-run against `ldbcdb9`
with the fix in: **11 of 14**, the same three failures (IC3, IC9, IC14) and
**every answering query returning an identical row count** to the run without
it.

Read the comparison counter, not the clock. Four queries — IC1, IC11, IC12,
IC13 — planned byte-identically across the two runs and still moved -2.5%,
+29.1%, +16.4% and -1.9%, so single-shot latency on this harness carries at
least ±29% run-to-run variance and no per-query timing comparison between the
runs means anything. `index_comparisons` is deterministic and does:

| query | comparisons, without -> with |
|---|---|
| IC2 | 122814874 -> 122302874 (-0.4%) |
| IC4 | 12116194 -> 12050050 (-0.5%) |
| IC5 | 21959580 -> 21941180 (-0.1%) |
| IC6 | 2316040 -> **2412520 (+4.2%)** |
| IC7 | 5832008 -> 5796844 (-0.6%) |
| IC8 | 2745324 -> 2724844 (-0.7%) |
| IC10 | 8082140 -> 7934684 (-1.8%) |

The rewrite therefore reaches half the IC set, which a scan of the query text
for a literal `()` does not predict — it also fires on a *named but unlabelled*
variable that plans as `ScanAll`. Six of the seven do less work. **IC6 does
4.2% more and that is unexplained**; its `index_scans` also rose 153 -> 159.
Small, but it is a cost the rule imposes somewhere and nobody has looked at
where. First step is `EXPLAIN` on IC6 either side of the rewrite.

Guarded by seven plan-shape tests in
`crates/uni-query/tests/common/planner/pattern_anchor_test.rs` covering
direction, undirected, the multi-label decline, untyped relationships and an
already-labelled source. Plan shape is the right assertion because both
spellings return identical answers — only the plan tells them apart.

**Note for whoever takes it:** the trigger for the expensive path is *reading an
edge property*. `MATCH ()-[w:WORK_AT]->() RETURN count(w)` succeeds;
`... RETURN max(w.workFrom)` does not. Materialising edge or target properties
routes off the sync fast path (`traverse.rs:1646`) onto the chunking path that
carries the #242 reservation. Any fix must be tested on both arms.

### 0.2 Harness: label the derivation source — **done, uncommitted**

`crates/uni/benches/ldbc/params.rs` now reads
`MATCH (:Person)-[w:WORK_AT]->() RETURN max(w.workFrom) AS y`. WORK_AT declares
exactly one source label, so the forms are equivalent — verified on all three
aggregates (count 21,654, max 2014, min 1998), 131.3 ms against 5,213.6 ms.

This is **not** a workaround dressed as a fix: an unlabelled source for an edge
type with one declared source label is a defect in the query as written. The
comment at the site records the engine gap and the standalone repro so 0.1 is
not masked by it. 0.1 stays open on its own merits.

---

### 0.3 Multi-label source narrowing is unmeasured, not rejected — **open, S to settle**

0.1 declines two-or-more labels because all three multi-source types at SF1
cover ~96% of the graph and measured 1.4x to 2.2x slower. That is a fact about
this corpus, not about the rule. A type whose source labels are collectively
small — say `[Person, Forum]`, 100k of 3.18M — would plausibly win, and nothing
in LDBC exercises it.

Settling it needs either a synthetic fixture with that shape, or a cardinality
gate. The gate is the larger job than it looks: `CardinalityCache` keys
`Vertex(label)` to `vertices_{label}`, a *different table* from the `vertices`
that `ScanAll` reads, so there is no total-vertex denominator today; adding
`AllVertices` means a new variant, a mapping in `cached_row_count` and
`refresh_row_count`, a new L0 arm (`l0.vertex_labels.len()` — `label_to_vids`
double-counts multi-labelled vertices), and a seeding call on the async query
entry path, because the cache is lazily populated and the planner can only
`get`.

Do the fixture first. It answers whether the gate is worth building.

---

## Tier 1 — reproducible, unexplained

Neither of these has a mechanism. Both are listed with the cheapest test that
could kill the first hypothesis, per the ordering principle's rule 4.

### 1.1 ~712 MB is held before the traverse asks — **RESOLVED 2026-09-14**

It is `GraphScanExec`'s reservation for the `ScanAll`, and it is an honest
charge for a batch that genuinely exists.

`ScanAll` lowers to `GraphScanExec::new_schemaless_all_scan`, which sets
`is_schemaless = true`. The chunking gate at
`crates/uni-query/src/query/df_graph/scan.rs:2063` reads
`whole.is_none() && self.filter.is_none() && !self.is_schemaless`, so a
schemaless scan never reaches `RangeChunking` — the #214 path that bounds
construction for a full-label scan. **The entire vertex set is therefore built
as one batch**, and `scan.rs:2195` reserves its true size. The rustdoc at
`scan.rs:828-834` states the consequence outright: for an unchunked scan "the
batch is already built by this point, so the reservation bounds how long an
over-budget result survives rather than preventing its construction."

The exclusion is not arbitrary. The sizing step it guards calls
`storage.vertex_row_count(&label)` (`scan.rs:2066`) — a *per-label* count — and
a label-less scan has no label to count. Same missing primitive as 0.3.

Measured, same process, RSS above a 9892-row baseline:

| scan | rows | RSS | delta | bytes/row |
|---|---:|---:|---:|---:|
| `(n:Person)` | 9 892 | 845 MB | — | — |
| `(n:Comment)`, chunked | 2 052 169 | 1230 MB | +384 MB | 187 |
| `(n)`, unchunked | 3 181 724 | 3091 MB | **+2245 MB** | **706** |

Comment carries 64.5% of ScanAll's rows for 17% of its memory growth: 5.8x the
memory for 1.55x the rows. That gap is the chunking, not the row count.

**Two consequences.**

First, it closes the question as predicted: the 711.8 MB does fold into 0.1,
because narrowing an unlabelled source does not merely read fewer rows — it
moves the scan off the schemaless path onto the chunked one. That is the larger
half of 0.1's 76x memory reduction and it was not in the original diagnosis.

Second, it promotes a defect in its own right — 1.4.

### 1.4 `ScanAll` is never chunked — **P1, new, opened by 1.1**

Every full-graph scan materialises every vertex in a single `RecordBatch`. At
SF1 that is 711.8 MB reserved and ~2245 MB resident for `MATCH (n)`, a query
with no traversal in it at all. `RangeChunking` already solves exactly this for
a labelled scan (#214); the schemaless arm is excluded only because it cannot
name a label to size.

This is not confined to the benchmark: it is every unlabelled `MATCH (n)` on any
graph, and it scales with the graph rather than with the result. The pool cannot
prevent it either — by the time the reservation is taken the batch exists, so
the refusal bounds residency, not construction.

**Fix shape, and it is shared with 0.3.** Give `CardinalityCache` an
`AllVertices` key over the main `vertices` table, then let the schemaless arm
size itself from it and take `RangeChunking`. The same addition supplies the
denominator 0.3 needs to widen the source narrowing to multiple labels. One
primitive unblocks both — which is the argument for doing it once, properly,
rather than either in isolation.

Sizing: **M**. Per the earlier survey the cache addition is four small edits
(variant, two table mappings, an L0 arm using `l0.vertex_labels.len()` since
`label_to_vids` double-counts multi-labelled vertices) plus a seeding call on
the async query entry path, because the cache is lazily populated and the
planner can only `get`. Wiring the scan arm is separate and needs care: the
existing `Sizing` state is label-keyed throughout.

### 1.2 CONTAINER_OF ingests at 8.5k rows/s against HAS_MEMBER's 133k — **open question**

From the load above:

| edge file | rows | seconds | rows/s |
|---|---:|---:|---:|
| CONTAINER_OF (Forum→Post) | 1,003,605 | **117.5** | **8,541** |
| REPLY_OF (Comment→Post) | 1,011,420 | 16.9 | 59,847 |
| HAS_MEMBER (Forum→Person) | 1,611,869 | 12.1 | 133,212 |
| HAS_CREATOR (Post→Person) | 1,003,605 | 7.2 | 139,389 |
| HAS_CREATOR (Comment→Person) | 2,052,169 | 60.5 | 33,920 |

CONTAINER_OF is **34% of the entire edge phase for 6% of the edge rows**, and
16x slower per row than REPLY_OF at a nearly identical row count.

**Fan-out is falsified.** HAS_MEMBER is also Forum-sourced with *higher* fan-out
(~17.8 per forum vs ~11) and is the fastest file in the set. Whatever
distinguishes CONTAINER_OF, it is not source fan-out.

**Surviving candidate, unverified:** CONTAINER_OF is the only Forum→**Post**
edge, so its destination is the 1M-row Post table where HAS_MEMBER's is the
9,892-row Person table; and it lands after ~14M edges are already resident.
Destination-table size and arrival order are confounded in this run.

**First test:** they separate cheaply — load CONTAINER_OF *first* into an empty
store and re-time it. If it stays slow, arrival order is out and the
destination table is the live variable.

**Caveat on all load timings above:** a concurrent CUDA build was running on the
same machine for part of the load. It cannot explain a 16x gap between two files
in the same phase, but the absolute rows/s figures are not clean and should be
re-measured on a quiet machine before being quoted anywhere.

---

### 1.3 The pool bounds a fraction of real memory — **P1, new**

IC14's refusal is for 4.2 GB against a 1 GiB pool. Meanwhile the process peak
RSS across the run reached **14,266 MiB**, and IC5 completed at 10,662 MiB —
**ten times the pool's ceiling**, without the pool objecting.

So `max_query_memory` is not a bound on what a query costs the machine. It
bounds the subset of allocations routed through operator reservations, and the
majority of real memory is outside it. This is the honest limit of the #242 and
#261 accounting work, and it is not a criticism of that work — those commits
bounded what they claimed to bound. It does mean **a 1 GiB `max_query_memory`
must not be described, in docs or release notes, as a 1 GiB cap on query
memory.**

Two consequences worth separating:

- Operationally, an SF1 Interactive workload needs ~11-14 GB of headroom per
  process today regardless of the pool setting.
- For the accounting track, the open question is which allocations should join
  the pool next. Answering it needs the same instrument as 1.1 — a per-consumer
  dump — plus an RSS delta per operator. Size: **M** to investigate, unknown to
  fix.

---

## Tier 2 — benchmark honesty

### 2.0 Three queries do not answer — **P1, sizing blocked on 2.1**

| query | outcome | note |
|---|---|---|
| IC3 | over 300 s | consumes the whole-span date window (`startDate`/`endDate`) |
| IC9 | over 300 s | the query the sync fast path's own comment cites as handing the sort a 2.3 GB input (`traverse.rs:1640`) |
| IC14 | refused, 4.2 GB | `allShortestPaths`; pool refuses upstream of enumeration |

**Do not root-cause IC3 before 2.1.** Its parameters are derived to admit
essentially the whole corpus, so its 300 s timeout is partly a statement about
the parameters and not only about the engine. Running it against curated
parameters first is cheaper than profiling it and may dissolve the finding
entirely. IC4 shares the same window and completes in 8.1 s, so the window is
not automatically fatal — which is exactly why IC3 needs the controlled
comparison rather than a guess.

IC9 and IC14 are not parameter-sensitive in the same way and can be taken on
their own. IC14's ceiling is a live question for 1.3: whether 4.2 GB is the
honest cost of SF1 `allShortestPaths` or an over-estimate is unmeasured, and the
2026-08-27 record of 19.2 GB and climbing suggests the former.

### 2.1 Parameters admit most of the corpus — [#227], **investigated 2026-09-14**

Confirmed still accurate. The date window collapses to the whole corpus:
`startDate = minDate = min(Post.creationDate)`, `endDate = minDate + span =
maxDate`, so IC3's and IC4's `[startDate, endDate)` filter is a **no-op** and
IC5's `membership.joinDate > $minDate` admits essentially all 1.6M HAS_MEMBER
edges. For scale, LDBC's own example header on ic3.cypher uses a **28-day**
window; ours is the full corpus span.

`durationDays` is confirmed dead — `grep -rn durationDays` hits only
`params.rs:114`, no `.cypher` file references it. The issue's own correction was
right.

**The curated parameters cannot be sourced. This is the finding.** The issue is
written as though a curated set exists to be selected alongside the derivation.
It does not, anywhere this repo can reach:

- `scripts/fixtures/fixtures.toml` has 31 LDBC entries, every one a
  `dynamic/*.csv` or `static/*.csv` dataset file. No substitution-parameter,
  `*_param.txt` or factor-table entry.
- The upstream mirror the fixtures point at holds 33 files total: the 31 CSVs,
  `.gitattributes`, and a rename script. No `substitution_parameters/`.
- Repo-wide, `substitution_param|paramgen|_param.txt|factor_table` matches only
  doc comments in `ldbc_snb.rs` and `params.rs`.

LDBC's `paramgen` consumes datagen's factor tables, which a pre-baked CSV mirror
does not ship. So there are exactly two paths, and the issue's **M** sizing
covers neither honestly:

1. **Regenerate** — run LDBC datagen + paramgen at SF1, mirror
   `substitution_parameters/*_param.txt` as new fixtures. Authentic and
   reusable; cost is a datagen run and a new fixture set, not a code change.
2. **Hand-author** — pick a narrow window and ids against the loaded graph and
   justify each. Cheap to write, but it is *our* parameter set wearing LDBC's
   name, and it cannot be defended as comparable to anyone else's numbers. If
   this path is taken the document must say so wherever the numbers appear.

Recommend (1). The whole point of #227 is comparability, and (2) does not
deliver it — it would replace "not comparable, and we say so" with "not
comparable, and we imply otherwise", which is worse.

**The wiring is genuinely small either way.** `params::derive` returns
`HashMap<String, uni_db::Value>` (`params.rs:46`) and is called at one place,
`ldbc_snb.rs:592-595`. An alternative producer of the same type behind an env
switch is a few lines, and `run_query` binds only keys the query text mentions
(`ldbc_snb.rs:349-353`), so a curated map may be partial and merged over the
derived one. The 13 live keys are listed in the survey above.

**One blocker that must land with it:** the `VACUOUS` gate
(`ldbc_snb.rs:795-812`) exits 1 on any zero-row query, and its error message
tells the reader to *widen* `params.rs`. A curated window will legitimately make
some queries return nothing at SF1. The gate has to become lane-aware — assert
non-emptiness for the derived/oracle lane, report-but-do-not-fail for the
curated lane — or the honest parameters turn the harness red and the next person
widens them again. That is the same pressure that produced today's parameters,
so leaving the gate alone would re-create the problem.

### 2.2 The load docstring understates the load by ~80% — **S**

`ldbc_snb.rs:409` says a full SF1 load is "~4 minutes". Measured: **442.2 s
(7m22s)** on 22 cores. Anyone budgeting an iteration loop off that comment is
planning against a figure that is nearly half the real one. Correct the comment
when 1.2 is understood, since the number will move.

---

## Tier 3 — harness ergonomics and hygiene

### 3.1 No way to set the memory pool from the bench — **S**

`ldbc_snb.rs:329-339` builds `UniConfig` overriding only the two timeouts, so
the pool is always the 1 GiB default. There is no `LDBC_*` knob for it, while
`ldbc_probe` has `PROBE_MAX_MB` and `operator_census` has `CENSUS_POOL_MB`.

Add `LDBC_MAX_MEMORY_MB`, **default unchanged at 1 GiB**. The point is not to
make failures go away — it is to be able to ask "does this query need more than
the default, and how much?" without editing and rebuilding the bench. Keeping
the default means a run that needs the knob still fails loudly by default.

### 3.2 Derivation is re-run by every supervisor child — **S**

Each child re-derives the full parameter set before running its slice. When
derivation is what fails, the supervisor pays it 14 times to learn one fact. A
cached parameter set on disk (written once, read by children, keyed by store
path) would make the restart loop cheap and would also guarantee every child
runs the *same* parameters — which #208's determinism note wants anyway.

### 3.3 A teardown panic obscures the real error — **S**

When the main thread panics, a tokio worker follows it with
`called 'Result::unwrap()' on an 'Err' value: JoinError::Cancelled` at
`lance-datafusion-7.0.0/src/utils.rs:59`. It is downstream noise — the runtime
being torn down mid-flight — but it is the *last* panic printed, so it is the one
a reader sees first. Worth a note in the runbook at minimum; upstream if the
unwrap is ours to influence.

### 3.4 The store fleet is 101 GB of mostly-unusable debris — **S**

Eight SF1 stores under `~/uni-bench-tmp`. Five are unusable (partial loads,
unreplayed WAL). `ldbcdb` is the active trap: complete adjacency, no WAL, but
missing `vertices_Message`, so IC2/7/8/9 return **0 rows silently** — only the
bench's own `VACUOUS` gate catches it, and an ad-hoc probe has none.

All eight predate the #247 index fix, so their per-label `vertices_<Label>`
tables carry no indexes and any lookup-vs-scan measurement on them compares two
scans.

Delete the five broken ones; keep `ldbcdb8` and the new `ldbcdb9`. Record the
signature check (15 fwd / 14 vertex / 0 WAL) in the runbook so it is done before
a store is trusted, not after a measurement is published.

---

## Sequencing at a glance

| # | item | tier | size | blocks |
|---|---|---|---|---|
| 0.2 | label the derivation source | 0 | **done** | the whole run |
| 0.1 | narrow an unlabelled source to its edge type's labels | 0 | **done** | — |
| 1.1 | dump the per-consumer reservation table | 1 | S | 1.3 |
| 0.3 | multi-label fixture, then decide on a cardinality gate | 0 | S | widening 0.1 |
| 2.1 | curated parameter source (#227) | 2 | M code, L to source values | 2.0's IC3, comparable latencies |
| 1.4 | chunk `ScanAll` (shares 0.3's primitive) | 1 | M | full-graph scan memory |
| 2.0 | IC3 / IC9 / IC14 | 2 | IC3 gated on 2.1 | — |
| 1.3 | what the pool does not bound | 1 | M to investigate | honest memory claims |
| 1.2 | CONTAINER_OF load-order test | 1 | S | — |
| 3.1 | `LDBC_MAX_MEMORY_MB` | 3 | S | easier work on 0.1, 2.0 |
| 3.2 | cache derived parameters | 3 | S | — |
| 3.4 | prune the store fleet | 3 | S | — |
| 2.2 | correct the load-time comment | 2 | S | after 1.2 |
| 3.3 | teardown-panic note | 3 | S | — |

**1.1 is still one debug build and still worth it.** It names the 711.8 MB
holder and is the starting point for 1.3. With 0.1 fixed, the first thing it
should answer is whether that 711.8 MB is gone from the narrowed plans — 1.1
predicted it would fold into 0.1, and that prediction is now testable.

**2.1 before IC3.** Profiling a query whose parameters admit the whole corpus
risks optimising for a workload nobody runs.

---

## Out of scope

- The differential oracle (Track 0 of the 2026-08-27 plan). Still unstarted,
  still the highest-leverage item for silent wrong answers, and not made more or
  less urgent by this run.
- Percentiles. The harness runs each query once; the table above is single-shot
  wall time, not a distribution.

---

## What this run does not establish

Stated plainly so it is not read into the tables above:

- **The 11/14 latencies are not LDBC-comparable.** #227 is open and the
  parameters admit most of the corpus. They are upper bounds and a baseline for
  this harness against itself — nothing more.
- **Three queries do not answer** (2.0), and IC3's share of that is
  parameter-contaminated rather than cleanly an engine result.
- **`max_query_memory` does not bound query memory** (1.3). Peak RSS reached
  14,266 MiB under a 1 GiB pool.
- **The 711.8 MB holder is still unidentified** (1.1), though now narrowed to
  the unlabelled-source shape rather than a global baseline.
- **CONTAINER_OF has no mechanism** (1.2); one hypothesis was falsified and the
  replacement is untested.
- **Load timings are contaminated** by a concurrent CUDA build on the same
  machine. The 16x gap between two files in the same phase survives that, the
  absolute rows/s figures do not.
- **Single run, one machine.** No repetition, no second host.

Four hypotheses were formed and killed during this run, recorded because the
correction is the part that does not survive summarising:

1. That the two newest commits caused the failure. Falsified — neither touches
   `GraphTraverseExec`, and the query shape dates to `4cc430faa`.
2. That the `[:KNOWS]` type filter was not reaching the expansion. Falsified —
   the traverse returns exactly 2 x 180,623 in 155 ms.
3. That `batch.get_array_memory_size()` was inflating the charge. Falsified by
   RSS: the reservation is a *conservative under-estimate* of a genuinely
   2,278 MB query.
4. That source fan-out explained CONTAINER_OF's ingest cost. Falsified by
   HAS_MEMBER, which has higher fan-out and is the fastest file in the set.

Hypothesis 3 is the one worth remembering: the accounting was suspected, and the
accounting was right. The defect was in the plan it was billing for.

---

## Evidence

Commands that produced the figures, for re-running:

```bash
# fresh SF1 load + all 14 (the failing run)
TMPDIR=$HOME/uni-bench-tmp LDBC_DB=$HOME/uni-bench-tmp/ldbcdb9 \
  LDBC_OUT=$HOME/uni-bench-tmp/ldbc-results-9 \
  cargo bench -p uni-db --bench ldbc_snb

# the blocker, standalone
cargo build -p uni-db --example ldbc_probe --release
LDBC_DB=$HOME/uni-bench-tmp/ldbcdb9 PROBE_MAX_MB=8192 \
  /usr/bin/time -f "RSS=%M KB" ./target/release/examples/ldbc_probe <<'EOF'
MATCH ()-[w:KNOWS]->() RETURN count(w) AS c
MATCH (:Person)-[w:KNOWS]->() RETURN count(w) AS c
EOF

# reservation is invariant to morsel size (rules out a per-batch cost)
for bs in 128 1024 8192; do
  PROBE_BATCH_SIZE=$bs LDBC_DB=$HOME/uni-bench-tmp/ldbcdb9 \
    ./target/release/examples/ldbc_probe <<< 'MATCH ()-[w:KNOWS]->() RETURN count(w) AS c'
done   # 699.5 MB, identical at every size

# property-read is the trigger, not edge count
LDBC_DB=$HOME/uni-bench-tmp/ldbcdb9 ./target/release/examples/ldbc_probe <<'EOF'
MATCH ()-[w:WORK_AT]->() RETURN count(w) AS c
MATCH ()-[w:WORK_AT]->() RETURN max(w.workFrom) AS y
EOF
# first succeeds, second refused at 662.7 MB

# store signature, before trusting any store
find <store> -name 'adjacency_*_fwd.lance' -maxdepth 3 | wc -l   # must be 15
find <store> -name 'vertices_*.lance'      -maxdepth 3 | wc -l   # must be 14
find <store> -path '*wal*' -type f                     | wc -l   # must be 0
```

Key source locations:

- pool default 1 GiB — `crates/uni-common/src/config.rs:499` (field), `:742` (default)
- pool construction — `crates/uni-query/src/query/executor/read.rs:723`
- the reservation — `crates/uni-query/src/query/df_graph/traverse.rs:1687-1689`
- `Expansion` tuple (40 B padded) — `traverse.rs:184`
- consumer registration (`GraphTraverseExec[{partition}]`) — `traverse.rs:643`
- sync fast path, taken when no properties materialise — `traverse.rs:1646`
- anchor ranking to extend for 0.1 — `crates/uni-query/src/query/planner.rs:4953`
- bench `UniConfig` (no memory override) — `crates/uni/benches/ldbc_snb.rs:329-339`
- `VACUOUS` gate — `ldbc_snb.rs:798-812`
