//! Does `GraphTraverseMainExec`'s reservation cover its peak? (#242)
//!
//! The operator reserves `buffered_bytes + estimate_adjacency_bytes(adjacency)`
//! on entering `Processing`, then returns `expand_batch(...)` directly — the
//! expanded fan-out output is neither reserved nor sliced. If that reading is
//! right, the pool sees the *input* while the peak is the *output*.
//!
//! # The falsifiable claim
//!
//! Set a ceiling **above** input+adjacency and **below** the expanded output.
//!
//!   * query succeeds  -> the output is unaccounted; the reservation
//!     under-covers, and `grep MemoryConsumer` reports this operator as fine.
//!   * query is refused -> the output *is* covered and the reading is wrong.
//!
//! A second, tiny ceiling checks the operator reserves anything at all, so a
//! pass at the middle ceiling cannot be explained by the pool being inert.
//!
//! The edge type is deliberately left undeclared: that is what routes a
//! single-hop traversal through `GraphTraverseMainExec` rather than its
//! schema'd twin.
//!
//!   cargo run --release -p uni-db --example traverse_main_peak_probe
//!   TM_MAX_MB=8 cargo run --release -p uni-db --example traverse_main_peak_probe

use uni_db::{DataType, Uni, Value};

/// Source rows. The input side, and small.
const SOURCES: i64 = 500;
/// Targets per source. The multiplier that makes output >> input.
const TARGETS: i64 = 400;

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Src")
        .property("k", DataType::Int)
        .label("Dst")
        .property("k", DataType::Int)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Src {k: i})")
        .param("n", Value::Int(SOURCES))
        .fetch_all()
        .await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Dst {k: i})")
        .param("n", Value::Int(TARGETS))
        .fetch_all()
        .await?;
    // `E` is never declared, so this is the schemaless traversal path.
    tx.query("MATCH (a:Src), (b:Dst) CREATE (a)-[:E]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let baseline = peak_rss_kb();
    let session = db.session();
    let q = "MATCH (a:Src)-[:E]->(b:Dst) RETURN count(*) AS c";
    let mut builder = session.query_with(q);
    let ceiling = std::env::var("TM_MAX_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    if let Some(mb) = ceiling {
        builder = builder.max_memory(mb * 1024 * 1024);
    }

    let t0 = std::time::Instant::now();
    match builder.fetch_all().await {
        Ok(r) => {
            let peak = peak_rss_kb();
            println!(
                "ceiling={:?} MB  OK  rows={:?}  peak_rss={} MB (delta {} MB)  took={:?}",
                ceiling,
                r.rows()[0].values()[0],
                peak / 1024,
                peak.saturating_sub(baseline) / 1024,
                t0.elapsed()
            );
        }
        Err(e) => println!("ceiling={ceiling:?} MB  REFUSED  {e}"),
    }
    db.shutdown().await?;
    Ok(())
}
