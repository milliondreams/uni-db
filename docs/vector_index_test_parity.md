# Vector Index Test Parity

The three vector-search modalities — **dense** (single-vector KNN), **multi-vector**
(ColBERT / MaxSim, incl. MUVERA), and **sparse** (SPLADE / learned-sparse) — are held
to the same correctness bar. This note records that bar as a **coverage contract**:
every modality must exercise each lifecycle and auto-embed scenario below, validated
against an independent brute-force oracle where a score is involved.

The parity audit that produced this contract uncovered three real bugs (two silent
data-correctness issues); see [Bugs found](#bugs-found-by-the-parity-audit).

## Lifecycle parity

| Capability | sparse | dense | multi |
|---|:--:|:--:|:--:|
| Brute-force oracle equivalence (exact score) | `sparse_index.rs` | `dense_index.rs` | `multivector_muvera.rs` |
| L0 union / flush equivalence | `sparse_index.rs` | `dense_index.rs` | `multivector_l0.rs` |
| L0 update (last-writer-wins) + tombstone | `sparse_index.rs` | `dense_index.rs` | `multivector_l0.rs` |
| MVCC snapshot isolation | `sparse_index.rs` | `dense_index.rs` | `multivector_snapshot.rs` |
| Reopen durability | `sparse_index.rs` | `dense_index.rs` | `multivector_muvera.rs` |
| WAL-replay of unflushed delta (no rebuild) | `sparse_index.rs` | `dense_index.rs` | (resilience) |
| Backfill (create-index-after-flush) | `sparse_index.rs` | `dense_index.rs` | `multivector_muvera.rs` |
| Cypher DDL (`CREATE VECTOR INDEX`) | `sparse_index.rs` | `dense_index.rs` | `multivector_muvera.rs` |
| Crash / WAL **failpoints** | `common/sparse_resilience.rs` | `common/dense_resilience.rs` | `common/multivector_resilience.rs` |
| **Metamorphic** fuzz oracle (smoke + soak) | `common/metamorphic/sparse.rs` | `common/metamorphic/dense.rs` | `common/metamorphic/multi.rs` |

Failpoint lanes run under `--features failpoints`; metamorphic soak tiers are
`#[ignore]` and sized by `METAMORPHIC_CASES` in the nightly job.

### Oracles

Each modality validates the engine's reported `score` against a disjoint brute-force
computation in f64 (EPS absorbs f32 rounding):

- **sparse** — `sparse_dot` (HashMap inner-join on term ids).
- **dense** — cosine-derived `(1 + cos) / 2` (what `calculate_score` produces for the
  `Cosine` metric; an exact `Flat` index makes it oracle-exact on every path).
- **multi** — cosine-MaxSim `Σ_q max_d cos(q, d)`, each query token's best (possibly
  negative) cosine, matching the engine's unclamped `maxsim`.

The multi metamorphic oracle forces `over_fetch` high so the first-stage candidate
generator returns the whole corpus: it pins the exact MaxSim **re-rank scoring**, not
the approximate first-stage **recall** (a separate property unsuited to exact-equality
checking).

## Auto-embed parity

Text → vector at write time and query time. Cross-modality cells live in
`autoembed_parity.rs`; per-modality happy-path basics in
`{multivec,hybrid,sparse}_autoembed.rs`.

| Scenario | sparse | dense | multi |
|---|:--:|:--:|:--:|
| Deferred write | ✅ | ✅ | ✅ |
| Explicit value not overwritten | ✅ | ✅ | ✅ |
| **SET source column re-embeds** | ✅ | ✅ | ✅ |
| Persistence across reopen | ✅ | ✅ | ✅ |
| `document_prefix` applied | — | ✅ | — |
| Batched into one inference | — | ✅ | — |
| Multi-source (≥2 columns) | ✅ | — | — |
| Error: text query, no runtime | ✅ | ✅ | ✅ |
| Error: text query, no `embedding_config` | ✅ | ✅ | — |

Residual (lower-value) cells are tracked, not yet filled: multi/hybrid query-time text
auto-embed, and `document_prefix` for sparse/multi.

## Bugs found by the parity audit

1. **Auto-embed went stale on `SET`** (all modalities) — `SET d.content = …` left the
   embedding pointing at the old text. The full-row write path's "what changed" signal
   (`touched_keys`) was only populated under the partial-write flag. *Fixed: re-embed
   any target whose source column was touched.*
2. **Dense search ignored committed-unflushed L0** — `uni.vector.query` returned stale
   results until a flush (L0 inserts missing, L0 updates not re-scored), because the L0
   merge extracted embeddings via `Value::as_array()`, which dropped the typed
   `Value::Vector` real writes use. *Fixed: extract via the canonical `TryFrom<&Value>`.*
3. **Reopen rejected a prebuilt runtime** for embedding-configured schemas (required a
   `xervo_catalog`, ignoring `.xervo_runtime(...)`). *Fixed.*

## The contract

When adding a new vector modality or capability, fill the corresponding row/column with
an oracle-backed test before shipping. A green result-based test is not sufficient: the
two silent-correctness bugs above were invisible to every passing test until a
mechanism-level oracle compared the engine to an independent ground truth.
