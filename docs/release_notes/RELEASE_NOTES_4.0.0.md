# uni-db 4.0.0

**Release focus: answers you can trust, and the instruments to see them.** The bulk of this
release is a sweep through places where a failed read, a missing value or an unsupported shape
produced something that looked like an answer and was not one. Nine changes are breaking because
the correct result differs from the old one. Alongside that, query counters, index-usage stats and
compaction reports stopped reporting invented numbers and started reporting measured ones.

410 commits since 3.4.0.

---

## ⚠️ Breaking changes

Nine in total. Two groups change the result of queries that look correct today; the rest tighten
types, rename a Python class, or reshape a Rust API. Read the two "returns an error now" sections
even if you use none of the Rust APIs.

### APOC procedures match Neo4j

All five land under one `feat!`. Two of them change the result of queries that look correct today
(`text.indexOf`, and the four-function string→number family), so re-check any code that pinned the
old values.

**`apoc.convert.toString` no longer returns NULL for lists, maps and other non-primitives.** The
argument is declared `ArgType::CypherValue`, so anything that is not a `Bool`/`Int`/`Float`/`String`
arrives as an opaque `LargeBinary` envelope; it used to fall through a catch-all and yield NULL.
It now decodes the envelope and renders the value the way Neo4j does — `apoc.convert.toString([1,2,3])`
is the string `"[1, 2, 3]"`, a map is `{k: v}`, and NULL stays NULL. Both live envelope encoders are
accepted (the procedure dispatcher's `serde_json` bytes and the scalar-function adapter's tagged
`cypher_value_codec`); they are unambiguous at the first byte.

**`apoc.create.uuids(n)` and `apoc.text.repeat(s, n)` error instead of silently truncating.** Both
bound their output at 1,000,000 (`MAX_SYNTHESIZED_LEN`) and both used to clamp with `min()`, so
`apoc.create.uuids(2_000_000)` quietly returned exactly half the rows asked for and an over-long
`text.repeat` returned a clipped string — neither distinguishable from a complete answer. The cap is
unchanged; exceeding it is now a `CODE_RESOURCE_LIMIT` error that names the cap. A single
`support::reject_over_cap` helper serves both, so the two siblings cannot drift apart again.

**`apoc.text.indexOf` returns a character index, not a UTF-8 byte offset.**
`apoc.text.indexOf('cafés','s')` was 5 and is now 4, matching Neo4j. It also matches this module's
own `text.length`, which always counted characters — the two units disagreed, and three doc sites
described the byte offset as intended.

**The string→number family parses like Java `DecimalFormat`.** `apoc.number.parseInt`,
`apoc.number.parseFloat`, `apoc.convert.toInteger` and `apoc.convert.toFloat` all used
`str::parse`, which demands the entire string be numeric. They now take the leading numeric prefix,
accept `,` grouping separators between digits, and truncate toward zero for the integer variants:
`"3.7"` → 3, `"-3.7"` → -3, `"1,234"` → 1234, `"1,234.5"` → 1234.5, `"12abc"` → 12. A string with no
digits at all is still NULL — NULL on genuine garbage is correct APOC behavior and is preserved.
One narrowing comes with this: `parseFloat("inf")`/`"NaN"` used to reach `f64::from_str` and now
yield NULL, matching `DecimalFormat`.

### `nodes()`, `relationships()`, `length()` and `size()` reject the wrong type

`nodes(x)` and `relationships(x)` returned `NULL` for anything that was not a path — indistinguishable
from an empty path, and the symptom of a missing `Value::Path` arm rather than a deliberate rule.
They now raise a type error naming the type actually received. `length()`/`size()` did the same for
a tagged (encoded) number or boolean, most often a value sourced from a JSON column; that case now
errors too. A native `Int64` or bool already errored before this change, so this closes the gap for
the encoded form specifically.

`NULL` in still gives `NULL` out in every case — Cypher's null propagation is unchanged. Audit
anything that relied on a `NULL` result to mean "not a path" or "no length".

### A property's type reads back as a spec, not a `Debug` string

`PropertyInfo.data_type`, on both the Rust and Python surfaces, rendered Rust's internal `Debug`
form — so what you read off a schema could not be fed back into `parse_data_type` or into
`property(name, data_type)`. It now emits the same lowercase spec string that those accept:

| property | was | now |
|---|---|---|
| `id: String` | `"String"` | `"string"` |
| `n: Int64` | `"Int64"` | `"int64"` |
| `emb: Vector{dimensions: 8}` | Debug-shaped | `"vector:8"` |
| `sig: BinaryVector{dimensions: 64}` | `"BinaryVector { dimensions: 64 }"` | `"binary_vector:64"` |

One asymmetry is deliberate: `Timestamp` still renders as `"timestamp"`, which parses back as
`DateTime`. `"timestamp"` has always been an accepted spelling for `DateTime`, and changing it
would silently change the column type for existing callers.

The same commit stops a `WriteLease::Custom` database reporting itself to Python as
`WriteLease.LOCAL` — which implied no external coordination when coordination was in fact external.
It now reports `WriteLease.CUSTOM`, and passing `CUSTOM` back into a builder raises `ValueError`
rather than silently downgrading to `Local`, because Python cannot supply the underlying
`Box<dyn WriteLeaseProvider>`.

### Locy values have one canonical rendering

Two internal sites built identifying strings with `format!("{v:?}")` — a Locy join key, and
`DERIVE` Skolem-id generation. `Debug` over a `HashMap` follows iteration order, and `RandomState`
is seeded per map instance, so **the same logical value could render differently on successive calls
within a single process**. The documented symptom: a `VALIDATE` with a path-valued `KEY` over a
20-row fixture returned anywhere from 1 to 5 scored rows out of 20, with accuracy between 0.0 and
1.0 depending on the run — a plausible-looking metric built on 15 to 19 silently dropped rows.

Everything now goes through `Value::canonical_string`: maps render with sorted keys at every depth,
entities compare by identity alone, every branch is prefixed by its kind so `Int(1)` and
`String("1")` cannot collide, and strings carry their length so an embedded comma cannot imitate a
structural one. `toString()` over a map, node, edge or path changes shape accordingly.

Nothing could have correctly depended on the old rendering, since it was never stable. But code
that pattern-matched the old `Debug`-shaped output, or persisted it into a property via `CREATE`,
will see different strings now.

### Compaction reports what it actually did

Every field of the compaction result except `duration` was a constant or a mislabelled count —
`compact_label` hardcoded `files_compacted: 1` even outside the table-existence guard, so a caller
could not tell a real compaction from a no-op. `StorageBackend::optimize_table` returned
`Result<()>`, discarding the real metrics entirely.

Removed, because the storage layer cannot provide them faithfully without opening a footer per
fragment on both sides (and returns zeros on legacy storage anyway):

- `CompactionStats::bytes_before`, `bytes_after`, `files_compacted`
- `CompactionStatus::l1_size_bytes`

Added or renamed:

- `tables_optimized` — "was anything touched", without conflating tables with files
- `semantic_passes` — covers `crdt_merges`, which `tables_optimized` alone cannot
- `total_bytes_compacted` → `total_bytes_reclaimed`, and it is now actually computed
- `l1_size_bytes` → `l1_estimated_bytes`, named honestly: it is `rows * 145`
- `StorageBackend::optimize_table` now returns `OptimizeReport`
- new `CompactionConfig::version_retention` (default 7 days)

`crdt_merges` previously died at the merge site and always read 0; it is now plumbed through,
including on the Cypher `VACUUM` path. `StorageManager::compact()` walked only vertex tables, so
`uni.admin.compact()` could report success having never touched an edge table; one shared walk now
covers both. The `YIELD` columns of `uni.admin.compact` and `uni.admin.compactionStatus` change to
match — docs that said `YIELD bytes_before, bytes_after` no longer work.

One finding this honesty surfaced but does **not** fix: compaction runs one flush behind. The pass
immediately after a flush reports no work and a second pass does the merge
(`compact#1: removed=0 added=0`, then `compact#2: removed=2 added=1`). Reproducible and known.

### Python: `PyPreparedQuery` is now `PreparedQuery`

`from uni_db import PreparedQuery` raised `ImportError`. The exported class was
`uni_db.PyPreparedQuery` — the only `Py`-prefixed name among 194 exports — while a stub-only alias
`PreparedQuery = PyPreparedQuery` satisfied type checkers but was never executed, because a `.pyi`
is not imported at runtime.

`uni_db.PreparedQuery` is now the real class. **`uni_db.PyPreparedQuery` no longer exists and there
is no compatibility alias** — rename any reference to it.

The same commit fixes `UniSession.explain()` / `.profile()` (and the async pair), which raised
`AttributeError` because they called a method that does not exist on `Session`; they route through
`query_with()` now. Five stub signatures were corrected to match runtime: `Session.add_hook`,
`AsyncSession.add_hook` and `SessionTemplateBuilder.hook` take `(name, hook)` rather than `(hook)`,
`LocyConfig.register_classifier`'s second parameter is `callable`, and `Calibrator.__call__` was
missing entirely.

### Rust API: sparse IDF field, and a join's child arity

`SparseVectorIndexConfig` gains `idf_modifier: bool`. The struct is not `#[non_exhaustive]` and
derives no `Default`, so **every direct struct literal must now name the field** — `..Default::default()`
is not available. Cypher and Python are unaffected: the option is additive, defaults off, and is
spelled `idf` in DDL (`OPTIONS{type:'sparse', idf:true}`) and in the Python config dict.

`VidLookupJoinExec`'s index probe is now a real `ExecutionPlan` child rather than a bespoke
execution path, so `with_new_children` takes two children instead of one. This only affects code
that manipulates execution-plan trees directly. For everyone else it is a fix: `PROFILE` output for
queries using this join was skipping the probe subtree entirely and now reports it.

---

## Silent-wrong-answer fixes

The largest theme of the release, tracked as **issue #233**: a systematic audit of places where a
failed or missing read became a plausible default instead of an error. Roughly 40 commits. If you
have acted on results from an earlier version, this is the section to read.

**"Absent" and "could not read" were the same answer.** Decoders across `uni-store`, `uni-common`
and `uni-query` returned `Value::Null` or `None` both for a genuine null and for a payload that
failed to decode. A corrupt property blob read as NULL. A truncated payload failed a `WHERE` clause
and the row simply vanished. A uniqueness probe read its own failure as "no duplicate" and admitted
a constraint-violating row. A deleted vertex's tombstone could be dropped when its labels failed to
read, leaving the vertex visible after flush. All of these now propagate the error.

**Identity fabricated from missing data.** A map without `_src`/`_dst` produced an edge with
`Vid::INVALID` endpoints instead of null, so `startNode(r)` returned a fake vertex id where it had
correctly returned null. Negative integers coerced to vids wrapped onto the INVALID sentinel and
were accepted as valid.

**Locy aggregates counted rows they never read.** SUM, AVG, MNOR and MPROD folded cells that failed
to decode as the identity element rather than skipping them — SUM reported 0.0, and AVG still
incremented its divisor, dragging the mean toward zero for rows that were never actually read.

**CRDT merges resolved by a policy nobody chose.** A failed merge was resolved three different
undocumented ways across `property_manager.rs` and `l0.rs` — drop the newer value, drop the older,
or last-writer-wins — instead of surfacing the failure. A batched pre-merge path used `or_insert`,
letting the overlay win outright instead of merging, which could walk a GCounter backwards.

**Plugins loaded that should not have.** An Extism plugin declaring an ABI range it did not support
loaded anyway, and a garbage range string (`abi = "not-a-range"`) matched every host version instead
of failing closed. Graph-algorithm inputs like `edgeTypes: "KNOWS"` — a scalar where an array was
expected — silently widened to "no restriction", so PageRank and WCC scored a different graph than
the one requested. `NeighborhoodOverlap` with empty seeds defaulted to vertex 0.

**Indexes reported a status they had not earned.** Indexes marked `Stale` were never picked up for
rebuild unless an unrelated trigger fired, so a query could plan against a stale index indefinitely.
Failed default-index builds and failed physical builds still reported `Online`.

**Storage.** Semantic compaction could destroy indexes and columns it could not see, and left
`overflow_json` residue behind; the declared column now wins over stale residue. MVCC tie-breaking
was non-deterministic, and a tombstone losing a version tie resurrected a deleted row. The
main-edges fallback could resurrect a deleted edge. Two `IN` lists on the same column could lose an
"unknown" match in the Lance SQL translation.

---

## Crashes, hangs and durability

- **`query_timeout` did not stop anything** (#207). It was checked after the fact, not enforced: a
  250 ms budget could run 7 s, and one LDBC query ran 111 minutes against a 300 s budget. Nine
  execution sites now preempt.
- **A hung query with no bound.** The Python bindings swallowed a failure to spawn the deadline
  watchdog thread, so a pure-Python `while True: pass` guest callback ran unbounded.
- **The flush barrier lied.** `flush_to_l1`, documented as a durability barrier, returned success
  when the async drain had not finished (seen above ~600k rows) or had actually failed. Downstream,
  index builds declined and queries served exact scans from L0 while believing the data was durable.
- **CLI writes were lost** (#251). Every subcommand dropped `Uni` without `shutdown`, so
  `uni query "CREATE ..."` printed success and exited 0 while the write never reached the snapshot
  manifest. The next open failed with an unrecoverable "no snapshot manifest".
- **A fork crash window orphaned data.** `drop_fork` deleted the recovery tombstone before the fork
  artifacts, so a crash in between stranded the fork's WAL directory and id allocator with nothing
  left to drive recovery.
- **CDC could checkpoint past a gap.** An `.ok()` collapsed "batch build failed" into "no rows", so
  the runtime delivered an empty batch and advanced the checkpoint instead of halting the stream.
  Separately, `periodic_cancel` bypassed the durable path, so a cancelled job could resurrect on
  restart.

---

## Query correctness

A long series unified how graph entities are encoded across the query layer, fixing a cluster of
silent-wrong-result bugs: `REMOVE n.prop`, `SET n:Label` and `REMOVE n:Label` were no-ops against
native entities; `SET n.p = null` inserted a literal null instead of removing the property;
`labels(n)` errored on a native node value; `id()` and `elementId()` returned NULL for a native
`Value::Node` from a pattern comprehension.

Also fixed: `DISTINCT` could count the same vertex twice depending on its encoding (#235); `IN` and
comparison now compare entities by identity; undirected relationships lost their stored direction on
several paths (#193, #188); `UNION` over entities failed across differing labels and mis-named its
result columns (#191, #190); pattern comprehensions had several scoping bugs (#189) including
unanchored comprehensions failing to correlate through outer values; `startNode`/`endNode` resolution
(#187); `COUNT{}`/`COLLECT{}` treated as aggregates rather than scalar subqueries, and outer
non-entity variables unreadable inside their bodies (#199).

Type and parsing edge cases: a string merely starting with "P" was accepted as a duration (#186);
`CREATE LABEL INT/FLOAT` were not 64-bit per spec; pattern predicates were rejected where a boolean
is valid; mixed-type list literals were mis-routed; `CASE` could not unify two entity types.

Locy: query parameters were not resolved in post-`FOLD` expressions (#273); IS-ref TO-target binding
by name (#272); `IN` given proper three-valued logic (#216).

---

## New

**Cypher.** `FOREACH` is implemented end to end — it existed in the executor but was unreachable
from the language. Pattern comprehensions can project a whole entity, not just a property, so
`[(n)-[:R]->(x) | x]` works. `datetime()` and `localdatetime()` accept `epochMillis` / `epochSeconds`
map fields, which previously required a `year`. Pattern anchoring now matches from whatever variable
is already bound regardless of where it sits in the pattern — middle of a path, comma-separated
paths, quantified paths — instead of falling into a full scan or cross-join.

**Locy.** A new `REQUIRE` keyword expresses a threshold that constrains the recursion itself,
evaluated every iteration, as distinct from a post-`FOLD WHERE` that only filters the converged
answer. This is the difference needed for rules like the OFAC 50% Rule, where the threshold is part
of the definition rather than a presentation filter. A new `HavingInRecursivePath` compiler warning
fires on the ambiguous combination that previously produced silently wrong answers, and the docs no
longer recommend the broken idiom.

**Schema.** Adding a property to a label that already holds flushed data is supported — it
previously wedged the label permanently, growing L0 without bound on every subsequent write while
returning `Ok`. Widening only: nullable columns and relaxing `NOT NULL`, applied as a Lance
metadata-only `add_columns`.

**Bulk.** `bulk_insert_vertices_labeled` creates multi-label vertices. The only bulk path hardcoded
one label per vertex, so genuinely multi-labelled data — an LDBC `Place` that is also a `City` —
silently lost a label at load time.

**Sparse search.** An opt-in IDF query-weight modifier boosts rare terms and discounts ubiquitous
ones, for BM25-like and BGE-M3 sparse heads. Off by default; SPLADE-style learned weights already
encode importance.

**Vector search.** IVF_PQ picks `sub_vectors` from dimensionality instead of a fixed 16, so
compression — and recall loss — no longer scales silently with embedding width, and ships a measured
default `refine_factor` clamped to [12, 32].

**Providers.** uni-xervo's `RemoteLlamaCppProvider` (`remote/llamacpp`) is registered and enabled by
default, so catalogs naming it build a runtime through `Uni::open(...).xervo_catalog(...)`.

---

## Observability

Counters that shipped in `PROFILE` output while being hardcoded now carry real values:
`rows_scanned`, `bytes_read`, `l0_reads`, `storage_reads`, `branch_scans` and `snapshot_reads` were
all zero. `index_scans`, `index_comparisons` and `scans_reported` come from Lance's execution stats
rather than a planner prediction, so profiling can tell whether a scalar index was actually
consulted. Vector and full-text search report `vector_index_scans` and `fts_index_scans` separately —
both previously reported `index_scans = 0` whether a real ANN search or a brute-force scan ran.
`OperatorStats::index_hits` was hardcoded `None` everywhere and now distinguishes "no opinion" from
"consulted zero" from "consulted N times".

---

## Performance

**MERGE.** A named relationship (`MERGE (a)-[e:R]->(b)`) keeps the fast path: **0.792 s → 0.105 s
(7.5x)**, against a MATCH+CREATE floor of 0.113 s. A found-or-created far endpoint gets a fast path:
**0.814 s → 0.065 s** and **0.876 s → 0.058 s (~12.5x)**. MERGE's pattern walk anchors on what the
row binds instead of a fixed left-to-right order: **5.38 s → 0.75 s at 5k, 16.15 s → 0.75 s at 20k
(21.5x)**, flat across the sweep afterward.

**Locy.** Rule-body `WHERE` reaches the scan and its index instead of sitting as a filter above a
fully materialized scan: **0.889 s → 0.160 s (5.5x)**, closing Locy's overhead against plain Cypher
from 5.8x to 1.1x.

**Scan and traversal.** A reverse-direction query now uses the index in both spellings:
**521.6 ms → 11.3 ms**. Batched edge-property resolution over 4,809 edges: **7,486 ms → 143 ms**.
Deduplicated traversal-target fetches, 2,052,169 requests over 9,343 distinct targets:
**4,247 ms → 1,937 ms**. Unlabelled traversal through a typed columnar path cut SF1 index
comparisons from **246 million to 3.2 million**. A scan-range walk retuned on bytes rather than rows
restored `RETURN count(n)` over 3,055,774 rows from a regressed **8.20 s back to 1.18 s**. IC5's UDF
argument decoding, memoized per run of equal rows, removed **218.2 s of that function's 220.5 s**.

**Memory.** Several LDBC queries aborted the process outright and now complete. Emitting scan and
traversal output in `batch_size` slices took IC2 from failing at 1332 MB to succeeding, and combined
peak RSS from **6,776 MB → 4,883 MB**; materializing traversal output a chunk at a time took IC9
from failing at 2.3 GB to succeeding, and the combined figure to **2,926 MB**. Columnar target
hydration replaced `HashMap<Vid, HashMap<String, Value>>`: at a 300k target table
**1620.9 MiB → 814.1 MiB**, and chunking the vid list on top brought 60k/300k to
**191.4 / 226.5 MiB — a 1.18x ratio where the tables differ by 10.6x**, so memory stopped scaling
with target-table size. Large collected lists are interned, costing **9 bytes per row** instead of
`rows x list_size`. Variable-length path expansion was entirely unaccounted — **106 MB of expansions
passed an 8 MB ceiling silently** — and after row-chunking sits at 26 MB.

**Query limits.** `max_query_memory` now bounds DataFusion's pool during execution rather than
checking the finished result, counts real heap-inclusive bytes instead of a flat per-row constant,
and wall-clock deadlines are enforced incrementally — including inside transactions, which had no
timeout at all.

**Vector recall.** Dimension-aware `sub_vectors` on SIFT-1M at K=10: nprobes=16 **0.5360 → 0.9080**,
nprobes=64 **0.5620 → 0.9780**, nprobes=128 **0.5620 → 0.9820**. A refine-factor sweep closed the
IVF_PQ recall ceiling: at nprobes=64, **no refine 0.5620 @ 50.8 QPS → refine=20 0.9920 @ 49.1 QPS**
— 43 points of recall for a 3% throughput cost, beating the best HNSW cell (0.980 @ 32.6 QPS) at
8x the exact scan's throughput.

**Build.** Link-time identical-code folding (`--icf=safe --gc-sections`) on Linux:
**225.8 MiB → 222.4 MiB**, verified neutral on `hot_paths_iai` (max 0.36% movement against a ±1.6%
noise floor).

---

## Upgrading

`cargo update -p uni-db` / `pip install -U uni-db`. Nothing fails at compile time — these are
result changes. The two to audit are `text.indexOf` (any caller slicing with the returned index, or
comparing it against a byte length) and the number-parsing family (any caller relying on `"3.7"` or
`"1,234"` coming back NULL). Callers that were silently receiving truncated `create.uuids` /
`text.repeat` output now see an error naming the cap.

Beyond the APOC changes, four things need attention:

1. **Python**: rename `uni_db.PyPreparedQuery` to `uni_db.PreparedQuery`. There is no alias.
2. **Anything reading `data_type`** off a schema now sees `"int64"` rather than `"Int64"`, and
   `"binary_vector:64"` rather than `"BinaryVector { dimensions: 64 }"`.
3. **Compaction callers**: `bytes_before`, `bytes_after`, `files_compacted` and `l1_size_bytes` are
   gone; `total_bytes_compacted` is `total_bytes_reclaimed`. This includes `YIELD` lists on
   `uni.admin.compact` and `uni.admin.compactionStatus`.
4. **Rust `SparseVectorIndexConfig` literals** must name `idf_modifier`; the struct derives no
   `Default`.

`nodes()`, `relationships()`, `length()` and `size()` now raise a type error where they returned
`NULL` — audit any query that relied on the null.
