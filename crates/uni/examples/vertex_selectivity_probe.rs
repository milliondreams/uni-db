//! Does the vertex-side chunked lookup ever lose to a full scan? (#237)
//!
//! #221 gave the *edge* path a measured scan-vs-lookup choice
//! (`MainEdgeDataset::prefers_full_scan`), fitted to two ratios:
//! `EID_SCAN_CROSSOVER_RATIO = 4096` and `EID_SPAN_RATIO = 32`. Three sibling
//! sites still choose with a fixed constant, and the vertex ones say so —
//! `scan.rs` ends its comment block with, verbatim, "A selectivity-aware choice
//! would beat a fixed constant."
//!
//! #237 is explicit that the edge constants must not simply be copied across:
//! vertex tables are per-label and much narrower than the unified 17.3M-row
//! edge table, and #221's own rustdoc concedes its numbers are "fitted to one
//! dataset on one machine". So this measures the vertex side on its own terms,
//! and is written **before** any fix so it can fail first.
//!
//! # STATUS: this probe does not yet reach the site #237 is about
//!
//! Read this before trusting a number out of it.
//!
//! It drives `MATCH (n:L) WHERE id(n) IN $vids RETURN count(n)` on the
//! assumption that an id-list predicate reaches `hydrate_vids_columnar`'s
//! chunked `_vid IN (...)` scans. **It does not.** Measured on LDBC SF1
//! `Person` (9 892 rows, vids 0…9 891):
//!
//! ```text
//! full scan                    15.4 ms   scanned=9892
//! K=1     spread               10.3 ms   scanned=9892
//! K=10    spread               10.7 ms   scanned=9892
//! K=100   spread               11.9 ms   scanned=9892
//! K=1000  spread               34.7 ms   scanned=9892
//! K=1000  dense                33.4 ms   scanned=9892
//! ```
//!
//! `rows_scanned` is the whole table at every `K`, so both arms are full scans
//! and the wall-time spread is post-filter cost, not read strategy. Comparing
//! them says nothing about scan-versus-lookup. Wall time alone would have
//! looked like a clean crossover story and been entirely wrong — which is why
//! the `scanned` column is printed.
//!
//! Two separate things this did establish, neither of them #237:
//!
//! 1. `hydrate_vids_columnar` is reached from the **traversal target**
//!    hydration path (`traverse.rs::build_target_property_columns`), not from a
//!    `WHERE id(n) IN …` predicate. A probe for #237 has to drive a traversal
//!    whose target-vid count and spread it can control — that is the work
//!    remaining here.
//! 2. A *literal* list does engage the `_vid` pushdown where a *parameter* does
//!    not: `WHERE id(n) IN [0,1,2]` took 8.0 ms against a 21.4 ms full scan,
//!    while `IN $vids` reported the full table scanned. That is a candidate
//!    parameter-folding gap, **unverified** — the literal arm's `rows_scanned`
//!    was not captured, so it is a lead, not a finding.
//!
//! # The two axes, once it points at the right path
//!
//! #221 found that size alone picks the wrong arm on the edge side: a large
//! *dense* request was a measured regression when pushed onto a scan. So this
//! varies both `K` (how many vids) and their spread (packed versus scattered at
//! fixed `K`). If the two agree on the vertex side, the vertex rule is simpler
//! than the edge one and should say so rather than inherit a span term it does
//! not need.
//!
//! A run reporting the same time for every `K` is measuring nothing; the
//! `K`-sweep is its own control.
//!
//! ```text
//! LDBC_DB=$HOME/uni-bench-tmp/ldbc-work-216 \
//!   VS_LABEL=Comment cargo run --release -p uni-db --example vertex_selectivity_probe
//! ```

use std::time::Instant;

use uni_db::{Uni, Value};

/// Min-of-N, so a single scheduling hiccup does not become the reported cost.
const SAMPLES: usize = 3;

async fn timed(session: &uni_db::Session, q: &str, params: Option<&[i64]>) -> (f64, i64, usize) {
    let mut best = f64::MAX;
    let mut rows = 0i64;
    let mut scanned = 0usize;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        let r = match params {
            Some(vids) => {
                session
                    .query_with(q)
                    .param(
                        "vids",
                        Value::List(vids.iter().map(|v| Value::Int(*v)).collect()),
                    )
                    .fetch_all()
                    .await
            }
            None => session.query(q).await,
        }
        .expect("probe query failed");
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        best = best.min(ms);
        // The load-bearing observable. Wall time cannot tell an indexed lookup
        // from a full scan plus a post-filter — both return the same rows — but
        // `rows_scanned` can. If the lookup arm reports the whole table, the
        // `_vid` pushdown did not engage and this probe is comparing a scan
        // with a scan.
        scanned = r.metrics().rows_scanned;
        rows = match r.rows().first().map(|x| x.values()[0].clone()) {
            Some(Value::Int(i)) => i,
            _ => -1,
        };
    }
    (best, rows, scanned)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let label = std::env::var("VS_LABEL").unwrap_or_else(|_| "Comment".into());
    let db = Uni::open(&root).build().await?;
    let session = db.session();

    // The label's extent, which every ratio below is denominated in.
    let bounds = session
        .query(&format!(
            "MATCH (n:{label}) RETURN min(id(n)) AS lo, max(id(n)) AS hi, count(n) AS c"
        ))
        .await?;
    let vals = bounds.rows()[0].values().to_vec();
    let (lo, hi, total) = match (&vals[0], &vals[1], &vals[2]) {
        (Value::Int(a), Value::Int(b), Value::Int(c)) => (*a, *b, *c),
        other => return Err(format!("unexpected bounds row: {other:?}").into()),
    };
    println!("label={label} rows={total} vids=[{lo}, {hi}]");

    let (scan_ms, scan_rows, scan_scanned) = timed(
        &session,
        &format!("MATCH (n:{label}) RETURN count(n) AS c"),
        None,
    )
    .await;
    println!(
        "full scan (floor for a scan-and-filter arm): {scan_ms:.1} ms, {scan_rows} rows, \
         scanned={scan_scanned}\n"
    );

    // Axis 1: how many, spread over the whole extent.
    println!(
        "{:>10}  {:>12}  {:>10}  {:>12}  {:>8}",
        "K", "spread ms", "rows", "scanned", "vs scan"
    );
    for k in [1i64, 10, 100, 1_000, 10_000, 100_000] {
        if k > total {
            continue;
        }
        let stride = ((hi - lo) / k).max(1);
        let vids: Vec<i64> = (0..k).map(|i| lo + i * stride).collect();
        let (ms, rows, scanned) = timed(
            &session,
            &format!("MATCH (n:{label}) WHERE id(n) IN $vids RETURN count(n) AS c"),
            Some(&vids),
        )
        .await;
        println!(
            "{k:>10}  {ms:>12.1}  {rows:>10}  {scanned:>12}  {:>8}",
            format!("{:.2}x", ms / scan_ms)
        );
    }

    // Axis 2: the same K, packed contiguously. #221 found this flipped the
    // right answer on the edge side; if it does not here, the vertex rule is
    // simpler than the edge one and should say so rather than inherit a span
    // term it does not need.
    println!(
        "\n{:>10}  {:>12}  {:>10}  {:>12}  {:>8}",
        "K (dense)", "dense ms", "rows", "scanned", "vs scan"
    );
    for k in [1_000i64, 10_000, 100_000] {
        if k > total {
            continue;
        }
        let vids: Vec<i64> = (0..k).map(|i| lo + i).collect();
        let (ms, rows, scanned) = timed(
            &session,
            &format!("MATCH (n:{label}) WHERE id(n) IN $vids RETURN count(n) AS c"),
            Some(&vids),
        )
        .await;
        println!(
            "{k:>10}  {ms:>12.1}  {rows:>10}  {scanned:>12}  {:>8}",
            format!("{:.2}x", ms / scan_ms)
        );
    }

    db.shutdown().await?;
    Ok(())
}
