# uni-db 4.1.0

**Release focus: limits that bind, and paths that stop when you stop asking.** Every declared
limit in 4.0 could be set; several of them did nothing. A `LIMIT` on a variable-length pattern
truncated a result that had already been built; `.max_memory()` was missing from one of the four
builder combinations and silently ignored by `profile()`; `config({...})` accepted a key it could
not honour and dropped it. The theme is the same in each: a request that produced no effect and no
error.

27 commits since 4.0.0.

---

## ⚠️ Behaviour changes

Three, none of which change a correct program's results.

**`config({...})` rejects a key it cannot honour.** `Uni.builder().config({"ssi_enabled": False})`
used to be accepted and ignored — the caller asked for a setting, got no error, and got the
default. An unrecognised key is now a `ValueError` naming the key and listing what is accepted.
Twelve of `UniConfig`'s forty fields are reachable from Python; the rest were never settable this
way, so code that passed one was already getting the default. Code that *worked* is unaffected;
code that was quietly broken now says so.

**`config({...})` merges instead of replacing.** It used to assign a value built from
`UniConfig::default()`, so `.strict_schema(True).config({"query_timeout": 11})` silently put
`strict_schema` back to `False`. Only the keys present in the dict are touched now, so builder
methods and `config()` compose in either order and the last write to a setting wins.

**An unbounded `[*]` pattern that truncates now says so.** A pattern with no written upper bound is
planned with a ceiling of 100 hops. Reaching that ceiling with more graph left used to produce a
short answer in silence; it now attaches a `QueryWarning` saying the results are incomplete, on the
same terms the two interior safety caps already used. It fires only when truncation actually
happened — a pattern that finishes inside 100 hops, or one with an explicit bound, stays quiet.

---

## `LIMIT` stops the work instead of trimming it

`MATCH p = (a)-[:R*]->(b) RETURN p LIMIT 5` cost exactly what no limit cost. The variable-length
operator enumerated every path a source vertex owns inside a single `poll_next`, so the consumer
could stop asking but the operator could not stop producing — and on a cyclic graph that set is
combinatorial.

Fetch pushdown is not the mechanism and cannot reach this operator: the plan always has a
`FilterExec` for the target label between the limit and the traversal, and pushing a fetch past a
row-dropping filter would under-deliver. Instead the enumeration became resumable, so a limit binds
as ordinary back-pressure. Measured on an 852-vertex, 1013-edge cyclic graph, where the same query
without a limit does not complete inside 30 s:

| query | 4.0 | 4.1 |
| --- | --- | --- |
| `... RETURN p LIMIT 5` | 30 s timeout | 5 rows, 3.97 s |
| `... RETURN p LIMIT 8192` | 30 s timeout | 8192 rows, 3.97 s |
| `... RETURN p LIMIT 20000` | 30 s timeout | 20000 rows, 16.3 s |

The limit applies one batch (8192 rows) at a time, so any limit up to 8192 costs the same as
`LIMIT 1`. The remaining ~4 s is the breadth-first search, which has to run before any accepting
endpoint is known and which no limit can avoid.

## A pinned endpoint prunes instead of costing more

`MATCH p = (a)-[:R*]->(b {uid: 'x'}) ... LIMIT 3` was *slower* than the same query with an
unpinned endpoint, which is backwards — a pin can only reduce the answer. The predicate reached
execution only as a filter above the traversal, so every reachable vertex was an accepting
endpoint and the enumeration built paths to all of them. The planner now also hands the traversal
the target's property conditions, which narrow the accepting set during the search itself.

The same holds when the endpoint comes from an earlier clause
(`MATCH (b {..}) WITH b MATCH p = (a)-[:R*]->(b)`), which took a separate fix: a target already in
scope is traversed into a temporary `__rebound_b`, and the bound-column lookup searched under that
name rather than the original.

| spelling | 4.0 | 4.1 |
| --- | --- | --- |
| endpoint unpinned, `LIMIT 3` | 0.55 s | 0.55 s |
| endpoint pinned inline | 30 s timeout | 0.73 s |
| endpoint pinned via `WITH` | 23–46 s | 0.72 s |

The narrowing only prunes. The filter above the traversal still applies the predicate and remains
what makes the answer correct, which is what lets the narrowing be conservative: anything it cannot
establish is admitted, because a vertex wrongly excluded would be a missing row that no later stage
can restore.

---

## New API

**`max_memory()` and `profile()` on the transaction Cypher builder.** These were the only empty
cells in the {session, tx} × {Cypher, Locy} matrix. A long-running read inside a write transaction
is the shape that most needs a ceiling, and was the one that could not be given one. Both land on
the Rust builder and on the Python `TxQueryBuilder` / `AsyncTxQueryBuilder`.

**`profile()` honours the builder's limits.** `session.query_with(q).max_memory(n).profile()`
accepted the bound and discarded it, on every path. It now binds.

**`LocyResult.metrics`.** The Rust wrapper always carried `QueryMetrics`; the Python converter
discarded them, so an identical workload yielded timing and scan counters through Cypher and
nothing through Locy. Note that Locy does not go through the Cypher parse/plan/cache path, so
`parse_time_ms` and `plan_time_ms` are always `0.0` there and `plan_cache_hit` always `False`.

**`max_memory()` on the Locy builders** (`locy_with(...)`), session and transaction, sync and async.

---

## Correctness fixes

**A second handle on the same store read committed vertices but no committed edges**, and the same
shape appeared after a crash before the first flush. One root cause: WAL replay repopulated L0 but
not the adjacency overlay, and the read path consults only the overlay. Write-time timestamps now
travel through the WAL too, so a replayed vertex keeps its `created_at`.

**Re-applying an unchanged schema to a populated label was rejected**, which made a persistent
store unopenable. Three members of that class are fixed: the `NOT NULL` relaxation is scoped to
newly declared properties, index declarations compare by configuration rather than by lifecycle
metadata, and an index build records the row count it actually indexed.

**`OPTIONAL MATCH` dropped rows from an unwound source**, and grouped on a per-row key rather than
the encoded entity. **`UNWIND` lost its source pruning across a plan rebuild**, which turned
`collect(DISTINCT n)` into 2 where the answer was 1.

**Six openCypher expressions failed on an entity that arrived via `collect()` + `UNWIND`** while
working on the same entity bound natively — `coalesce`, `toUpper`, `trim`, `replace`, `left` and a
`CASE` branch. The value was always decodable; it was simply opaque to DataFusion built-ins.

**`timeout()` did not interrupt execution beneath a pipeline-breaking operator**, on the Cypher and
the Locy path. **Variable-length enumeration is now charged against the query's memory budget as it
runs** rather than after the set is already resident — a limit checked after the fact is a report,
not a limit.

---

## Internal

A new `scripts/ci/check_rust_python_parity.py` gate compares the Rust surface to the Python
bindings across three axes — `UniConfig` fields, `UniError` variants, and a curated set of builder
and result types. The four guards that existed all compared Python to Python, so a Rust field or
error variant that was never bound was invisible to them; that is how the `config()` defects above
survived. The check does not demand that every gap be closed — most are deliberate — only that each
be declared with a reason, so a new one fails a build rather than going unnoticed.
