# Partial-Column Writes via Lance MergeInsert

**Status:** Design — soundness probe passed
([`lance_merge_insert_probe.rs`](../../crates/uni-store/tests/common/storage/lance_merge_insert_probe.rs)).
**Origin:** Round 3 of
[`plan-and-implement-a-valiant-flame.md`](../../../.claude/plans/plan-and-implement-a-valiant-flame.md).

## Problem

The production hotpath for the issue #72 ingest workload (SET frequency/
last_seen/confidence on a vertex carrying a 768-dim embedding with an
HnswSq index) runs at **17.7 ms/row** with batch size 3. 99.8% of that
wall is inside `MutationSetExec`. Two structural costs dominate:

1. `execute_set_items_locked` (`crates/uni-query/src/query/executor/write.rs:1746`)
   does a full **read** of the vertex (`prop_manager.get_all_vertex_props_with_ctx(vid)`),
   merges SET items into the resulting `Properties` map, then writes the
   **full** map back via `writer.insert_vertex_with_labels`.
2. The L0→Lance flush emits the full row (including the 768-dim embedding),
   triggering full-row encoding, fragment write, and downstream HNSW
   maintenance — even though no embedding value changed.

For a 3-property SET on a wide-row schema, the per-row cost grows with the
size of the largest unchanged column, not the size of the change.

## What we already know

- **Soundness verified end-to-end** by
  [`lance_merge_insert_partial_columns_preserves_hnsw_sq`](../../crates/uni-store/tests/common/storage/lance_merge_insert_probe.rs).
  `MergeInsertBuilder::try_new(ds, vec!["_vid"]).when_matched(UpdateAll)` with
  a partial-column source (no `embedding`) leaves the embedding byte-equal
  and the HnswSq index returns the correct neighbor after
  `optimize_indices(OptimizeOptions::append())`.
- **L0 already tracks partial deltas.**
  `L0Buffer.vertex_properties: HashMap<Vid, Properties>` accumulates changed
  keys through `merge_crdt_properties`. The full-row read in
  `execute_set_items_locked` is a CRDT-safety convenience, not a
  correctness requirement.
- **MVCC reads are agnostic.** `record_batches_to_rows`-style readers go
  through the property manager (`fetch_all_props_from_storage` + L0 overlay);
  MergeInsert with `WhenMatched::UpdateAll` leaves untouched columns at
  their pre-merge values inside Lance, so post-merge scans see the
  complete row without any read-side change.

## Goals

1. UPDATE paths that touch only scalar columns flush through MergeInsert
   with a partial-column source, **skipping the read** of the wide
   (embedding-carrying) columns and skipping their write.
2. **No regressions** on:
   - CREATE / INSERT paths (still full-row Append).
   - DELETE paths (still bump `_version`, set `_deleted=true`).
   - CRDT merge semantics (unchanged: L0 still drives the merge).
   - HnswSq / FullText / Scalar index correctness.
   - Reads — both within-tx and post-flush.
3. **Opt-in** for one release cycle behind a config knob so it can be
   compared against the current Append-only path under load.

## Non-goals

- Replacing the L0 layer or its CRDT semantics.
- Changing how DELETE / CREATE flush.
- Touching FullText / Scalar (BTREE/HASH) index maintenance flows.
- Any LanceDB upgrade.

## Design

### Threading a "dirty keys" set through the write path

Today the write path is "give me a `Properties` map and I'll insert it."
Add a parallel *partial* path that carries only the keys the caller
explicitly touched. The L0 layer already coalesces these into a per-VID
map; the new bit is **at flush time** we know which keys to put in the
MergeInsert source batch.

#### 1. `execute_set_items_locked` (uni-query)

Path: `crates/uni-query/src/query/executor/write.rs:1746-2058`.

Today the per-target accumulator is:

```rust
struct PendingVertexSet {
    vid: Vid,
    labels: Vec<String>,
    props: Properties,   // FULL: read from L0/storage, then merged
}
```

After the change:

```rust
struct PendingVertexSet {
    vid: Vid,
    labels: Vec<String>,
    /// Only the keys this statement touched (the SET targets).
    touched: Properties,
}
```

The lazy read at lines 1792–1804 is dropped. The Round-3 coalescing logic
already collects all SetItems targeting the same VID into one pending
entry — only `touched` is mutated.

Validation still happens per-key (`validate_property_value`), and the row
binding is still updated in place so subsequent RHS expressions see the
new value (`row.get_mut(var_name)` at line 1817).

At flush time the writer is given the **partial** map and a flag:

```rust
writer
    .insert_vertex_partial(pv.vid, pv.touched, &pv.labels, tx_l0)
    .await?;
```

`insert_vertex_partial` is the new entry point — see (2).

**Edge cases that still need a full read** (fall back to today's path):

- `SetItem::Variable` / `VariablePlus` (`SET n = {map}` / `n += {map}`).
  These replace or merge the whole property map and require the existing
  row.
- `SetItem::Labels`. Label SETs need to walk current labels first.
- `enrich_properties_with_generated_columns` requires the full map to
  compute generated values that depend on other properties.

For these, keep the existing read-modify-write path; the optimization
applies only to coalesced `SetItem::Property` runs.

#### 2. uni-store writer: split entry points

Path: `crates/uni-store/src/runtime/writer.rs`.

```rust
impl Writer {
    /// Today's full-row upsert. Used by CREATE, MERGE-ON-CREATE,
    /// Variable / VariablePlus SET, Labels SET.
    pub async fn insert_vertex_with_labels(...);

    /// New. Used by SetItem::Property when only scalar columns change.
    /// `touched` must contain ONLY the property keys that were assigned;
    /// `_vid`, `_deleted=false`, `_version=<next>` are filled in here.
    pub async fn insert_vertex_partial(
        &self,
        vid: Vid,
        touched: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<...>;
}
```

`insert_vertex_partial` stages the write into L0 the same way as today
(into `L0Buffer.vertex_properties[vid]` via `merge_crdt_properties`), but
also records the touched key-set in a parallel per-VID structure so the
flush knows which columns to send in the MergeInsert source.

#### 3. L0 buffer: per-VID dirty-key tracking

Path: `crates/uni-store/src/runtime/l0.rs:87-138, 357-401`.

Add a sibling field:

```rust
pub struct L0Buffer {
    pub vertex_properties: HashMap<Vid, Properties>, // unchanged
    pub vertex_versions: HashMap<Vid, u64>,          // unchanged

    /// Per-VID set of property keys that should land via MergeInsert
    /// at flush time. A VID present in this set is a "partial" pending
    /// update: only the listed keys plus system columns go into the
    /// MergeInsert source. Absence (or `vertex_label_inserts` / tombstone
    /// presence) → full-row Append on flush.
    pub vertex_partial_keys: HashMap<Vid, HashSet<String>>,
}
```

- `insert_vertex_with_labels` (full-row path) **clears** the VID from
  `vertex_partial_keys` (a full write supersedes any partial state).
- `insert_vertex_partial` accumulates touched keys into the entry. If
  the VID already has a full-row pending insert (e.g., from an earlier
  CREATE in the same tx), the partial keys are folded in but the
  full-row Append path is kept.
- `delete_vertex` / tombstone insertion clears `vertex_partial_keys` for
  that VID — the flush emits a deletion row, not a partial update.

The CRDT merge already running inside `merge_crdt_properties` keeps
working unchanged; this is purely orthogonal flush-mode metadata.

#### 4. Flush path: emit two RecordBatches per label

Path: `crates/uni-store/src/runtime/writer.rs:2398-2685`,
`crates/uni-store/src/storage/main_vertex.rs:129-201`,
`crates/uni-store/src/storage/vertex.rs:144-212`.

Today `flush_stream_l1` collects `vertices_by_label` and emits **one**
Append batch per label. Change to emit up to **two** batches per label:

- `vertices_by_label_full: Vec<(Vid, labels, full_props, version)>`
  → existing Append path (CREATE, DELETE-via-tombstone, Variable/Labels
  SET, anything in `vertex_partial_keys` absent).
- `vertices_by_label_partial: Vec<(Vid, touched_keys, partial_props, version)>`
  → new MergeInsert path.

`MainVertexDataset::build_record_batch` already builds the full schema;
add a sibling `build_partial_record_batch(rows, schema, touched_columns)`
that emits only:

- `_vid` (join key)
- `_version` (always present so MVCC ordering still holds)
- `_deleted` (always `false` here; deletion uses the full path)
- For each label-specific schema field whose name appears in any row's
  `touched_keys`: the column with values from `partial_props` where
  present, else null.

The partial batch is fed to:

```rust
let mut builder = MergeInsertBuilder::try_new(
    dataset.clone(),
    vec!["_vid".to_string()],
)?;
builder
    .when_matched(WhenMatched::UpdateAll)
    .when_not_matched(WhenNotMatched::DoNothing); // safety: never insert via MergeInsert
let (updated_ds, stats) = builder.try_build()?
    .execute_reader(reader)
    .await?;
metrics::record_merge_stats(&stats);
```

`WhenNotMatched::DoNothing` is load-bearing: a partial write to a VID
that doesn't yet exist is a logic bug, not an INSERT. The Append batch
handles the INSERT.

#### 5. Vector / FullText / Scalar index maintenance

Append already creates new unindexed fragments and relies on Round-3
era `optimize_indices` to fold them in. MergeInsert behaves the same —
the probe proved that an `OptimizeOptions::append()` call brings the
HnswSq index back to full coverage. Plumb the existing post-flush
optimize call (or its equivalent in `index_manager`) to fire after
both batches land, not just after the Append.

For scalar (BTREE / HASH) and FullText indexes: the same logic that
runs after Append today runs after MergeInsert. **No new index code
needed** — but the test suite must cover MergeInsert paths to confirm
this empirically (see Test plan).

#### 6. Compaction and read-side dedup

Path: `crates/uni-store/src/storage/compaction.rs:150-230`,
`crates/uni-store/src/runtime/property_manager.rs:911-952`.

Compaction reads each row's columns and applies LWW-by-version /
CRDT-merge. After MergeInsert, the **same row at the same row-address**
holds the updated values (Lance handles the column overlay internally),
so compaction sees a single up-to-date row per VID per fragment.

Read-side property manager: no change. `fetch_all_props_from_storage`
scans the label table; rows after MergeInsert have the new values
on the merged columns and the original values on the rest. L0 overlay
still wins for in-flight changes.

### Sequencing / version safety

`_version` is bumped by `L0Buffer.insert_vertex_with_labels_impl` (or
its new partial sibling) on every insert. Two scenarios to verify:

- **Within a single L0 flush, two updates to the same VID.** Today the
  CRDT merge handles this in L0; the flush emits one row with the
  latest version. Under the new scheme, the same applies — only the
  union of touched keys lands in the partial source, and the version
  is the final one.
- **A CREATE then SET on the same VID in the same tx.** L0 records
  the CREATE as full-row Append; the subsequent SET adds to
  `vertex_partial_keys` but also writes through to the in-L0 full row.
  At flush time, prefer the full-row path (drop the partial-keys entry
  on conflict). This is safe because the full row already contains the
  SET values via CRDT merge.

## Configuration

Behind one flag on `WriterConfig`:

```rust
pub struct WriterConfig {
    ...
    /// Enable MergeInsert path for UPDATE-only SET flushes. Behind a
    /// flag for the first release so we can A/B against Append-only.
    pub partial_lance_writes: bool,  // default: false
}
```

When `false`, `insert_vertex_partial` is implemented as
`insert_vertex_with_labels(vid, touched_props_merged_with_existing,
labels, tx_l0)` — i.e., the current read-modify-write code path, so the
caller can use the new API unconditionally.

When `true`, `flush_stream_l1` emits the partial batches as designed.

## Test plan

In addition to the existing probe
(`crates/uni-store/tests/common/storage/lance_merge_insert_probe.rs`):

1. **`writer_partial_set_preserves_other_columns`** — write a vertex
   with 5 scalar columns + an embedding column + an index. Apply a
   `SetItem::Property` touching 2 columns. Flush. Re-read: touched
   columns updated, other columns + embedding byte-equal, KNN still
   returns the row.
2. **`partial_then_full_in_same_l0_uses_full_path`** — in one tx, SET
   one property (partial-track), then `SET n = {a:1, b:2}` (Variable
   replace, full-row). Verify exactly one full-row Append in the flush
   (no partial batch), and that the replace semantics hold.
3. **`partial_set_with_crdt_property`** — vertex has a CRDT counter
   property and a scalar property. Touch only the scalar via partial
   write; touch only the CRDT counter from a second L0 layer.
   Post-compaction the counter merges correctly and the scalar reflects
   the latest version.
4. **`partial_set_with_scalar_index`** — create a HASH index on the
   property being SET, run the partial flush, query via the index.
5. **`partial_set_with_fulltext_index`** — same shape, with a FullText
   index on a STRING column.
6. **`partial_then_delete`** — partial SET landed, then DELETE in a
   later tx. Verify `_deleted=true` lands and the partial row is
   shadowed.
7. **`diag_72_set_data_scale_with_hnsw` re-run** — record the
   median MutationSetExec ms/row pre- and post-flag. Expect a
   significant drop (target: ≤4 ms/row from 16.1 ms/row at
   N=2000 + HnswSq + 500 warmup, per Round 3 numbers).
8. **Flag-off equivalence** — run a fixed test suite with
   `partial_lance_writes=false` and confirm bit-for-bit identical
   results to today.

## Risks & mitigations

- **Compaction NULL fill.** If MergeInsert ever writes a NULL into a
  previously-non-null column on the same fragment, compaction's
  union-with-prior logic must keep the non-null value. **Mitigation:**
  partial source omits columns entirely rather than sending NULL; Lance
  treats the unsent columns as untouched. Verified by the probe.
- **Index lag.** New fragments after a MergeInsert are un-indexed
  until `optimize_indices(append())` runs. Vector queries fall back to
  flat search in the interim — same as today's Append flow. **Mitigation:**
  the existing index-maintenance scheduler covers both paths.
- **Version skew.** If two partial writes race in different L0 layers
  with the same VID, the CRDT merge inside L0 already serializes them
  before flush, so the MergeInsert source has one canonical
  `(vid, version)` pair per row. **Mitigation:** preserve the existing
  `vertex_versions` invariant — bumped on every L0 insert, regardless
  of full vs partial.
- **Flag-on regressions on edge cases not covered by the test plan.**
  Ship behind `partial_lance_writes=false` for one release. Roll out
  on the issue #72 ingest workload first; flip default after a few
  weeks of cluster runtime.

## Rollout

1. **Stage 1 — landing the plumbing** (this design). Ship behind the
   flag, default off. Tests #1–#8 above must all pass with the flag
   either on or off.
2. **Stage 2 — measurement**. Run the issue #72 diag with the flag
   flipped, capture per-row median + p99. If the win is < 3× at the
   target shape, stop and investigate before changing the default.
3. **Stage 3 — default flip**. After a release cycle of clean
   telemetry, flip the default to `true`. Keep the flag as a kill
   switch for one more release.

## Out of scope (deferred follow-ups)

- **DELETE via MergeInsert.** Today DELETE flushes a tombstoned full
  row. A MergeInsert with `_deleted=true` + bumped `_version` would
  work the same way and skip the property payload. Worth doing once
  Stage 3 ships, but no production hotpath motivates it today.
- **Generated columns on partial writes.** If a generated column
  depends on a touched property, the partial path needs to compute and
  include it. For now, generated-column properties force the full-row
  read path (see `execute_set_items_locked` §1 fallback list).
- **Per-edge partial writes.** Same shape applies to edges; defer
  until the vertex path is stable.
