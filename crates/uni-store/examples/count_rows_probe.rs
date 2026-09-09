// Rust guideline compliant
//! Is `StorageBackend::count_rows(table, None)` metadata-only, or does it scan?
//!
//! The claim is asserted in-repo -- `main_edge.rs:597` and
//! `uni-query/src/types.rs:83` both say the unfiltered form reads fragment
//! metadata rather than rows -- and it is load-bearing: `prefers_full_scan`
//! calls it on the edge-property read path, and a cached-cardinality statistic
//! would rest on it entirely. It had not been measured.
//!
//! # Method
//!
//! Absolute timings prove nothing here; the question is a *shape*. A metadata
//! read is O(fragments) and indifferent to row count, a scan is O(rows). So the
//! probe builds the same table at three sizes with the fragment count held
//! fixed and asks whether the time tracks rows.
//!
//! Two controls, because a probe that cannot see a scan would report "flat" for
//! a scan too:
//!
//!   * **filtered count** -- `count_rows(table, Some(id >= 0))` must read every
//!     row to answer, so it has to grow with N. If this comes back flat, the
//!     instrument is broken and the unfiltered result means nothing.
//!   * **full scan** -- collecting every batch, as the reference for what
//!     linear actually costs at these sizes.
//!
//! A third axis holds rows fixed and varies fragments, which separates
//! "metadata-only" (O(fragments)) from "O(1)".
//!
//! Throwaway diagnostic, not shipping code.
//!
//!   cargo run --release -p uni-store --example count_rows_probe

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use uni_store::LanceDbBackend;
use uni_store::backend::StorageBackend;
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar, ScanRequest};

/// Row counts to compare. The top end is large enough that a scan is
/// unmistakable and small enough to keep the probe under a minute.
const SIZES: &[usize] = &[10_000, 100_000, 1_000_000];

/// Rows per written batch. Fixed across sizes so fragment count scales with N
/// in the first table and can be varied independently in the second.
const CHUNK: usize = 50_000;

/// Samples per measurement; the minimum is reported, as the least noisy
/// estimate of the underlying cost.
const SAMPLES: usize = 5;

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn batch(lo: usize, hi: usize) -> RecordBatch {
    let ids: Int64Array = (lo..hi).map(|i| Some(i as i64)).collect();
    let payload: StringArray = (lo..hi).map(|i| Some(format!("row-{i:08}"))).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload)]).unwrap()
}

/// Build `table` with `rows` rows in batches of `chunk`.
async fn build(backend: &LanceDbBackend, table: &str, rows: usize, chunk: usize) -> usize {
    let mut written = 0usize;
    let mut fragments = 0usize;
    while written < rows {
        let hi = (written + chunk).min(rows);
        let b = vec![batch(written, hi)];
        if written == 0 {
            backend.create_table(table, b).await.unwrap();
        } else {
            backend
                .write(table, b, uni_store::backend::types::WriteMode::Append)
                .await
                .unwrap();
        }
        fragments += 1;
        written = hi;
    }
    fragments
}

async fn min_of<F, Fut>(mut f: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut best = Duration::MAX;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        f().await;
        best = best.min(t.elapsed());
    }
    best
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let backend = LanceDbBackend::connect(dir.path().to_str().unwrap(), None).await?;

    println!("axis 1: rows vary, batch size fixed at {CHUNK}\n");
    println!(
        "{:>10} {:>6} {:>14} {:>16} {:>14}",
        "rows", "frags", "count(None) ms", "count(filter) ms", "full scan ms"
    );
    println!("{}", "-".repeat(66));

    let mut first_unfiltered: Option<f64> = None;
    let mut last_unfiltered = 0.0;
    let mut first_filtered: Option<f64> = None;
    let mut last_filtered = 0.0;

    for &n in SIZES {
        let table = format!("t_{n}");
        let frags = build(&backend, &table, n, CHUNK).await;

        // Warm the dataset open so the first sample does not pay for it alone.
        let _ = backend.count_rows(&table, None).await?;

        let unfiltered = min_of(|| async {
            let c = backend.count_rows(&table, None).await.unwrap();
            assert_eq!(c, n, "unfiltered count must be exact");
        })
        .await;

        // Selects every row, so the answer matches the unfiltered count and only
        // the *work* differs -- which is the whole point of the control.
        let filter = FilterExpr::compare("id", CmpOp::GtEq, Scalar::Int(0));
        let filtered = min_of(|| async {
            let c = backend.count_rows(&table, Some(&filter)).await.unwrap();
            assert_eq!(c, n, "filtered count must match: predicate selects all");
        })
        .await;

        let scan = min_of(|| async {
            let b = backend
                .scan(ScanRequest::all(table.as_str()))
                .await
                .unwrap();
            let got: usize = b.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(got, n, "scan must return every row");
        })
        .await;

        let (u, f, s) = (
            unfiltered.as_secs_f64() * 1e3,
            filtered.as_secs_f64() * 1e3,
            scan.as_secs_f64() * 1e3,
        );
        first_unfiltered.get_or_insert(u);
        first_filtered.get_or_insert(f);
        last_unfiltered = u;
        last_filtered = f;

        println!("{n:>10} {frags:>6} {u:>14.3} {f:>16.3} {s:>14.3}");
    }

    let span = *SIZES.last().unwrap() as f64 / SIZES[0] as f64;
    let u_growth = last_unfiltered / first_unfiltered.unwrap();
    let f_growth = last_filtered / first_filtered.unwrap();

    println!("\nover a {span:.0}x increase in rows:");
    println!("  count(None)   grew {u_growth:>7.2}x");
    println!(
        "  count(filter) grew {f_growth:>7.2}x   <- control: must grow, else the probe is blind"
    );

    println!("\naxis 2: rows fixed at {}, fragments vary", SIZES[1]);
    println!("\n{:>6} {:>14}", "frags", "count(None) ms");
    println!("{}", "-".repeat(22));
    let n = SIZES[1];
    for &chunk in &[n, n / 10, n / 100] {
        let table = format!("f_{chunk}");
        let frags = build(&backend, &table, n, chunk).await;
        let _ = backend.count_rows(&table, None).await?;
        let d = min_of(|| async {
            backend.count_rows(&table, None).await.unwrap();
        })
        .await;
        println!("{:>6} {:>14.3}", frags, d.as_secs_f64() * 1e3);
    }

    Ok(())
}
