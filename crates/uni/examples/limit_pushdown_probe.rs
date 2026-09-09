//! Does a Cypher `LIMIT` reach storage? (#239)
//!
//! `ScanRequest::limit` exists and is read by the Lance backend, but
//! `with_limit` has no callers, so the field is always `None`. #239 concludes
//! that `MATCH (n:L) RETURN n LIMIT 1` therefore reads the whole table — and
//! records that this was "derived from the code path, not from a benchmark".
//!
//! This measures it against LDBC SF1, where `vertices_Message` is 1.3 GB.
//!
//! # The instrument
//!
//! Wall time is secondary; the direct observable is `rows_scanned` /
//! `storage_rows`, which count what the scan actually pulled. If `LIMIT 1`
//! reports the whole table, the limit did not reach storage. If it reports ~1,
//! it did, and #239 is already fixed by some other path.
//!
//!   LDBC_DB=$HOME/uni-bench-tmp/sf1 cargo run --release -p uni-db \
//!       --example limit_pushdown_probe

use uni_db::Uni;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let label = std::env::var("LIMIT_LABEL").unwrap_or_else(|_| "Message".into());
    let db = Uni::open(&root).build().await?;
    let session = db.session();

    // Baseline: how big is the table, by the engine's own count?
    let t = std::time::Instant::now();
    let total = session
        .query(&format!("MATCH (n:{label}) RETURN count(n) AS c"))
        .await?;
    println!(
        "count(n)          -> {:?}  rows_scanned={}  scans={}  iops={}  {:?}",
        total.rows()[0].values()[0],
        total.metrics().rows_scanned,
        total.metrics().scans_reported,
        total.metrics().lance_iops,
        t.elapsed()
    );

    for limit in [1usize, 10, 1000] {
        let q = format!("MATCH (n:{label}) RETURN id(n) AS v LIMIT {limit}");
        let t = std::time::Instant::now();
        let r = session.query(&q).await?;
        let m = r.metrics().clone();
        println!(
            "LIMIT {limit:<5}       -> rows={}  rows_scanned={}  scans={}  iops={}  {:?}",
            r.rows().len(),
            m.rows_scanned,
            m.scans_reported,
            m.lance_iops,
            t.elapsed()
        );
    }

    db.shutdown().await?;
    Ok(())
}
