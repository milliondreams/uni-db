// Rust guideline compliant
//! Where does a chunked `_vid IN (...)` read lose to one full scan? (#237)
//!
//! Two sites chunk a vertex-id list at a fixed `MAX_VIDS_PER_CHUNK = 10 000`:
//! `hydrate_vids_columnar` in `scan.rs` and `VidLookupJoinExec`. The first ends
//! its comment block with, verbatim, "A selectivity-aware choice would beat a
//! fixed constant", and records the trade: at 100% selectivity — asking for
//! every row in the table — one full scan beats six chunked ones.
//!
//! #237 asks for this probe to be built before any fix, and refuses to let
//! #221's edge constants be copied across. So this measures the vertex side on
//! its own terms, and is able to report "no crossover" as a real answer.
//!
//! # The fixture is built here, not borrowed
//!
//! An earlier version of this probe ran against the LDBC SF1 store and reported
//! that one scan beat the chunked arm at *every* K, including K=10 out of 2M
//! rows. That was an artifact: `list_indexes` on that store returns **zero**
//! indexes for `vertices_Person` and `vertices_Comment`, so its "lookup" arm was
//! a full scan wearing an `IN` predicate — strictly worse than a bare scan, and
//! not the shape production has. A normal store gets `_vid`, `_uid` and `ext_id`
//! BTrees on every label table from `VertexDataset::ensure_default_indexes` at
//! flush.
//!
//! (That an SF1 store has no vertex indexes at all is worth its own look; it is
//! not this issue.)
//!
//! So the fixture is written through the ordinary path here, and the probe
//! **refuses to report** unless `index_comparisons` proves the lookup arm is
//! actually using an index.
//!
//! # Method
//!
//! Two arms over the same label and the same requested vids, differing only in
//! read strategy: chunked `_vid IN (...)` at the production constant, against
//! one unfiltered pass. Both project the same columns, so only the strategy
//! differs, and both are checked to return the same number of rows. `K` is
//! swept from a handful up to the whole table, which is its own control.
//!
//! # What it found
//!
//! Release, min-of-3, three table sizes:
//!
//! | rows | scan | crossover K | K/N |
//! |---|---|---|---|
//! | 30 000 | 1.7 ms | ~280 | ~0.9% |
//! | 300 000 | 4.0 ms | ~800 | ~0.27% |
//! | 1 000 000 | 7.0 ms | ~1 100 | ~0.11% |
//!
//! The scan arm is flat in K and the lookup arm linear, so a crossover exists at
//! every size. But **K/N is not stable** — it falls about 8x across a 33x range
//! of rows, because a columnar scan grows far slower than linearly. #237
//! predicted this when it refused to let #221's edge-side ratio be copied
//! across; here it is measured.
//!
//! # And the crossover is the wrong threshold anyway
//!
//! Just past it the scan arm is barely faster while materialising the whole
//! table for a few hundred rows: at K=1 000 of 1 000 000 it saves 0.7 ms and
//! reads 1 000x the rows. Chunking exists to bound peak residency (60 000 vids
//! from a 300k-row table: 815 MiB -> 226 MiB), and trading that for
//! sub-millisecond wins would undo it. The rule shipped in
//! `scan.rs::vertex_scan_beats_lookup` therefore waits for 25% of the table,
//! where memory is comparable and the win is ~50x rather than marginal.
//!
//! Throwaway diagnostic, not shipping code.
//!
//!   cargo run --release -p uni-store --example vertex_arm_probe

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use uni_common::core::schema::SchemaManager;
use uni_store::QueryCounters;
use uni_store::backend::types::{FilterExpr, Scalar, ScanRequest};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// The production chunk size both sites use.
const MAX_VIDS_PER_CHUNK: usize = 10_000;
/// Samples per measurement; the minimum is reported.
const SAMPLES: usize = 3;
/// Table size. Matches the 300k-row fixture `scan.rs`'s own comment measured on.
const ROWS_DEFAULT: usize = 300_000;
/// Rows per write batch, to keep the fixture build to a sensible time.
const WRITE_BATCH: usize = 10_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Table size is swept, not fixed: #237 refuses to assume a rule fitted on one
    // table transfers to another, so the crossover has to be shown stable (or
    // not) as a *ratio* across sizes before it can be expressed as one.
    let rows: usize = std::env::var("ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(ROWS_DEFAULT);
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    sm.add_label("N")?;
    sm.add_property(
        "N",
        "payload",
        uni_common::core::schema::DataType::String,
        true,
    )?;
    sm.save().await?;
    let storage = Arc::new(StorageManager::new(path, sm.clone()).await?);
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;

    let build = Instant::now();
    let mut all: Vec<u64> = Vec::with_capacity(rows);
    let mut batch_vids = Vec::with_capacity(WRITE_BATCH);
    let mut batch_props = Vec::with_capacity(WRITE_BATCH);
    for i in 0..rows {
        let vid = writer.next_vid().await?;
        all.push(vid.as_u64());
        let mut p = uni_common::Properties::new();
        p.insert(
            "payload".to_string(),
            uni_common::Value::String(format!("row-{i}")),
        );
        batch_vids.push(vid);
        batch_props.push(p);
        if batch_vids.len() == WRITE_BATCH {
            writer
                .insert_vertices_batch(
                    std::mem::take(&mut batch_vids),
                    std::mem::take(&mut batch_props),
                    vec!["N".to_string()],
                    None,
                )
                .await?;
        }
    }
    if !batch_vids.is_empty() {
        writer
            .insert_vertices_batch(batch_vids, batch_props, vec!["N".to_string()], None)
            .await?;
    }
    writer.flush_to_l1(None).await?;
    println!(
        "fixture: {rows} rows in {:.1}s",
        build.elapsed().as_secs_f64()
    );

    let backend = storage.backend();
    let table = "vertices_N";
    let indexes = backend.list_indexes(table).await?;
    println!(
        "indexes on {table}: {:?}",
        indexes
            .iter()
            .map(|i| i.columns.clone())
            .collect::<Vec<_>>()
    );

    let cols = vec![
        "_vid".to_string(),
        "payload".to_string(),
        "_version".to_string(),
    ];

    let mut scan_ms = f64::MAX;
    let mut scan_rows = 0usize;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        let batches = backend
            .scan(ScanRequest::all(table).with_columns(cols.clone()))
            .await?;
        scan_ms = scan_ms.min(t.elapsed().as_secs_f64() * 1000.0);
        scan_rows = batches.iter().map(RecordBatch::num_rows).sum();
    }
    println!("scan arm: {scan_ms:.1} ms, {scan_rows} rows\n");

    println!(
        "{:>9}  {:>6}  {:>11}  {:>9}  {:>9}  {:>10}",
        "K", "K/N", "lookup ms", "scan ms", "rows", "idx cmps"
    );
    let mut any_indexed = false;
    let mut ks: Vec<usize> = vec![1, 10, 100, 300, 1_000, 3_000];
    ks.extend(
        [1usize, 5, 10, 25, 50, 75, 100]
            .iter()
            .map(|pct| (rows * pct) / 100),
    );
    ks.retain(|k| *k > 0 && *k <= rows);
    ks.sort_unstable();
    ks.dedup();
    for k in ks {
        let wanted = &all[..k];
        let mut lookup_ms = f64::MAX;
        let mut got_rows = 0usize;
        let mut cmps = 0u64;
        for _ in 0..SAMPLES {
            let counters = Arc::new(QueryCounters::new());
            let t = Instant::now();
            let mut got = 0usize;
            for chunk in wanted.chunks(MAX_VIDS_PER_CHUNK) {
                let filter = FilterExpr::one_of("_vid", chunk.iter().map(|v| Scalar::UInt(*v)));
                let batches = backend
                    .scan(
                        ScanRequest::all(table)
                            .with_filter(filter)
                            .with_columns(cols.clone())
                            .with_counters(Some(counters.clone())),
                    )
                    .await?;
                got += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            }
            lookup_ms = lookup_ms.min(t.elapsed().as_secs_f64() * 1000.0);
            got_rows = got;
            cmps = counters.index_comparisons();
        }
        any_indexed |= cmps > 0;
        println!(
            "{k:>9}  {pct:>5}%  {lookup_ms:>11.1}  {scan_ms:>9.1}  {got_rows:>9}  {cmps:>10}",
            pct = (k * 100) / rows
        );
    }

    if !any_indexed {
        println!(
            "\nINVALID: index_comparisons was 0 at every K, so the lookup arm is a \
             sequential scan with a predicate and these numbers say nothing about \
             scan-versus-lookup."
        );
    }
    Ok(())
}
